# steward-workflow 🐹

> 🧭 Approval stages, quorum, approve as a group, reassignment, bulk decisions and due dates for Steward

## 🎯 What it is

The workflow service runs a policy version through its approval workflow: ordered stages, each with
individual approvers and groups that approve as a whole, a quorum, an SLA and escalation. It serves
`steward.workflow.v1.WorkflowService` over gRPC and runs the go-saga engine in-process.

- 🗳️ **Separate seats**: a group decides by its own quorum of yes votes; an individual approval
  never counts toward it.
- 🔁 **Reassignment and bulk decisions**, each recorded in an append-only history.
- 🎭 **Act-as**: decisions are made as the target and recorded with the real admin.

## 🚀 Run

```bash
cp .env.example .env   # Postgres, RabbitMQ, core and identity addresses
task run
```

The image: `docker build --build-arg VERSION=<tag> --build-arg COMMIT=<sha> .`

## 📚 More

- [API, seats and events](docs/api.md)
- [Configuration](docs/configuration.md)
- [Data and migrations](docs/data.md)
- [Error codes](docs/error-codes.md)
- [Runbook](docs/runbook.md)

## 🛠 Develop

```bash
task build    # go build ./...
task test     # go test ./... (Postgres and RabbitMQ tests need Docker)
task lint     # gofmt check + golangci-lint + yamllint
task proto    # fetch the pinned callee protos and regenerate gen/
task license  # check Apache-2.0 headers (golic)
```

## 🙏 Acknowledgements

Steward was originally written by [@Bugs5382](https://github.com/Bugs5382).

## ⚖️ License

Apache-2.0 (c) 2026 The Steward Authors
