// Copyright 2026 The Steward Authors
// SPDX-License-Identifier: Apache-2.0

// Command server runs the workflow service: approval stages, quorum, approve
// as a group, reassignment, bulk decisions and due dates, over gRPC, with the
// saga engine, its SLA timer and the identity outage reconciler in-process.
package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Bugs5382/go-buildinfo"
	"github.com/Bugs5382/go-buildinfo/health"
	grpcactor "github.com/Bugs5382/go-grpc-actor"
	log "github.com/Bugs5382/go-log"
	gootel "github.com/Bugs5382/go-otel"
	postgres "github.com/Bugs5382/go-postgres"
	pgotel "github.com/Bugs5382/go-postgres/otel"
	"github.com/Bugs5382/go-rabbitmq"
	rmqotel "github.com/Bugs5382/go-rabbitmq/otel"
	sagapg "github.com/Bugs5382/go-saga-orchestration/store/postgres"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	workflowv1 "github.com/Steward-GRC/steward-workflow/gen/go/steward/workflow/v1"
	corev1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/core/v1"
	identityv1 "github.com/Steward-GRC/steward-workflow/gen/go/thirdparty/identity/v1"
	"github.com/Steward-GRC/steward-workflow/internal/access"
	"github.com/Steward-GRC/steward-workflow/internal/assignment"
	"github.com/Steward-GRC/steward-workflow/internal/audit"
	"github.com/Steward-GRC/steward-workflow/internal/builder"
	"github.com/Steward-GRC/steward-workflow/internal/config"
	"github.com/Steward-GRC/steward-workflow/internal/grpcsvc"
	"github.com/Steward-GRC/steward-workflow/internal/identity"
	"github.com/Steward-GRC/steward-workflow/internal/outage"
	"github.com/Steward-GRC/steward-workflow/internal/readiness"
	"github.com/Steward-GRC/steward-workflow/internal/saga"
	"github.com/Steward-GRC/steward-workflow/internal/server"
	"github.com/Steward-GRC/steward-workflow/internal/store"
	"github.com/Steward-GRC/steward-workflow/internal/worker"
)

const serviceName = "workflow"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger := log.NewLogger(serviceName)
	if err := run(ctx, logger); err != nil {
		logger.Fatal(err, "workflow service stopped")
	}
}

func run(ctx context.Context, logger log.Logger) error {
	bi := buildinfo.Get()
	logger.Info("starting", log.F("version", bi.Version), log.F("commit", bi.Commit))
	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	otelShutdown, err := gootel.Init(ctx, serviceName, cfg.OTLPEndpoint)
	if err != nil {
		return fmt.Errorf("otel: %w", err)
	}
	defer func() {
		if err := otelShutdown(context.Background()); err != nil {
			logger.Warn("otel shutdown", log.F("error", err.Error()))
		}
	}()

	if err := pgotel.InstrumentMigrate(ctx, serviceName, func() error {
		return postgres.MigrateWithTable(cfg.MigrateDSN, cfg.MigrationsDir, store.MigrationsTable)
	}); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	db, err := postgres.New(ctx, cfg.DatabaseDSN, pgotel.WithTracing())
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer db.Close()
	if err := sagapg.Migrate(cfg.SagaDatabaseDSN); err != nil {
		return fmt.Errorf("saga store migrate: %w", err)
	}
	sagaStore, err := sagapg.Open(ctx, cfg.SagaDatabaseDSN)
	if err != nil {
		return fmt.Errorf("saga store: %w", err)
	}
	defer sagaStore.Close()

	conn, err := rabbitmq.Connect(ctx, cfg.RabbitURL, append(rmqotel.Instrument(), rabbitmq.WithLogger(rabbitLogger{logger}))...)
	if err != nil {
		return fmt.Errorf("rabbitmq: %w", err)
	}
	defer func() { _ = conn.Close() }()
	auditPub := conn.NewPublisher(audit.Exchange,
		rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: audit.Exchange, Kind: "topic", Durable: true}),
		rabbitmq.WithDefaultContentType(audit.ContentType))
	auditAdapter := grpcsvc.NewAuditAdapter(audit.New(publisher{auditPub}), logger)
	// The approval notices go to obligations on "jobs" as JSON.
	jobsPub := publisher{conn.NewPublisher("jobs", rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "jobs", Kind: "topic", Durable: true}))}

	dialOpts, err := server.DialOptions(cfg.TLS.CertFile, cfg.TLS.KeyFile, cfg.TLS.ClientCAFile)
	if err != nil {
		return err
	}
	identityConn, err := grpc.NewClient(cfg.IdentityAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("identity dial: %w", err)
	}
	defer func() { _ = identityConn.Close() }()
	coreConn, err := grpc.NewClient(cfg.CoreAddr, dialOpts...)
	if err != nil {
		return fmt.Errorf("core dial: %w", err)
	}
	defer func() { _ = coreConn.Close() }()
	directory := identity.New(identityv1.NewIdentityReadServiceClient(identityConn))
	corePolicies := corev1.NewPolicyServiceClient(coreConn)
	coreCategories := corev1.NewCategoryServiceClient(coreConn)

	compileOpts := builder.CompileOptions{StageSLAHours: cfg.StageSLAHours, StageReminderHours: cfg.StageReminderHours}
	defStore := store.NewWorkflowDefStore(db)
	runStore := store.NewAssignmentStore(db)
	assignments := store.NewAssignments(db)

	decisions := saga.NewDecisionResolver(runStore, defStore, assignments)
	assigner := saga.NewStageAssigner(defStore, assignments, compileOpts).
		WithAccessFilter(access.NewResolver(directory, access.NewCoreAdapter(corePolicies, coreCategories))).
		WithApprovalNotifier(saga.NewApprovalEmitter(jobsPub)).
		WithLogger(logger)
	policyClient := worker.NewCorePolicyClient(worker.CorePolicyClientOpts{
		Core:     corePolicies,
		Runs:     runStore,
		RunRefs:  runRefs{runStore},
		Resolver: decisions,
		Audit:    auditAdapter,
	})
	engine, err := saga.Build(sagaStore, policyClient, decisions, assigner, logger)
	if err != nil {
		return fmt.Errorf("saga engine: %w", err)
	}

	wfSrv := grpcsvc.NewWorkflowServer(grpcsvc.WorkflowServerOpts{
		Resolver:   assignment.NewResolver(assignment.NewCoreCategoryFetcher(coreCategories), runStore),
		DefStore:   defStore,
		RunStore:   runStore,
		SagaClient: saga.NewEmbedded(engine),
		// Only the request path names the act-as admin; the reconciler's
		// system events below go to the plain adapter.
		AuditEmit:      grpcsvc.NewImpersonationAuditEmitter(auditAdapter),
		Assignments:    assignments,
		ActorExtractor: actorFromContext,
		Admins:         directory,
		CoreStatus:     coreStatus{corePolicies},
		RunNotifier:    grpcsvc.NewRunEventEmitter(jobsPub),
		CompileOpts:    compileOpts,
		Logger:         logger,
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		if err := saga.RunTimer(ctx, engine, sagaStore.Pool()); err != nil && ctx.Err() == nil {
			logger.Error(err, "saga timer stopped")
		}
	}()
	identityHealth := healthpb.NewHealthClient(identityConn)
	go outage.NewReconciler(outage.Config{
		Interval: 30 * time.Second,
		HealthCheck: outage.IdentityHealthCheck(func(c context.Context, in *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
			return identityHealth.Check(c, in)
		}),
		Assignments:         assignments,
		Logger:              outage.LoggerFunc(func(msg string) { logger.Info("outage: " + msg) }),
		ConsecutiveFailures: 3,
		Audit:               outage.AuditFunc(func(action, subject string) { auditAdapter.Emit(ctx, action, subject) }),
	}).Run(ctx)

	if len(cfg.TrustedCallers) == 0 {
		logger.Warn("WORKFLOW_TRUSTED_CALLERS is not set: forwarded actors are ignored, so act-as can't name the admin")
	}
	checker, err := readiness.New(readiness.Deps{
		Postgres: readiness.PostgresDB(db), Broker: conn,
		Identity: readiness.GRPCPeer(identityConn), Core: readiness.GRPCPeer(coreConn),
	}, health.WithTTL(5*time.Second), health.WithTimeout(2*time.Second), health.WithLogger(logger))
	if err != nil {
		return fmt.Errorf("readiness: %w", err)
	}

	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", ":"+cfg.GRPCPort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	probeLis, err := lc.Listen(ctx, "tcp", ":"+cfg.ProbePort)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	logger.Info("serving", log.F("port", cfg.GRPCPort), log.F("probe_port", cfg.ProbePort))
	probesDone := make(chan error, 1)
	go func() {
		probesDone <- server.ServeProbes(ctx, probeLis, checker)
		cancel()
	}()
	err = server.Serve(ctx, lis, logger, server.Options{
		CertFile: cfg.TLS.CertFile, KeyFile: cfg.TLS.KeyFile, ClientCAFile: cfg.TLS.ClientCAFile,
		TrustedCallers: cfg.TrustedCallers, Checker: checker,
	}, func(s *grpc.Server) { workflowv1.RegisterWorkflowServiceServer(s, wfSrv) })
	cancel()
	if perr := <-probesDone; err == nil {
		err = perr
	}
	return err
}

// actorFromContext is the effective caller go-grpc-actor's server
// interceptor admitted: during act-as, the target.
func actorFromContext(ctx context.Context) (grpcsvc.ActorClaims, bool) {
	a, ok := grpcactor.FromContext(ctx)
	if !ok {
		return grpcsvc.ActorClaims{}, false
	}
	return grpcsvc.ActorClaims{UserID: a.Subject}, true
}

// publisher narrows a go-rabbitmq publisher to the Publish the emitters use.
type publisher struct{ p *rabbitmq.Publisher }

func (p publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	return p.p.Publish(ctx, routingKey, body)
}

type rabbitLogger struct{ l log.Logger }

func (r rabbitLogger) Debugf(f string, a ...any) { r.l.Debug(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Infof(f string, a ...any)  { r.l.Info(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Warnf(f string, a ...any)  { r.l.Warn(fmt.Sprintf(f, a...)) }
func (r rabbitLogger) Errorf(f string, a ...any) { r.l.Error(nil, fmt.Sprintf(f, a...)) }

// runRefs maps a saga run to its version for the escalation audit subject.
type runRefs struct{ s *store.AssignmentStore }

func (r runRefs) PolicyVersionIDForRun(ctx context.Context, runID string) (string, error) {
	run, err := r.s.GetRunByRunID(ctx, runID)
	if err != nil {
		return "", err
	}
	return run.PolicyVersionID, nil
}

// coreStatus writes a withdrawn version back to draft in core.
type coreStatus struct{ c corev1.PolicyServiceClient }

func (a coreStatus) SetVersionStatus(ctx context.Context, policyVersionID, status, actorUserID string) error {
	_, err := a.c.SetVersionStatus(ctx, &corev1.SetVersionStatusRequest{
		PolicyVersionId: policyVersionID, Status: status, ActorUserId: actorUserID,
	})
	return err
}
