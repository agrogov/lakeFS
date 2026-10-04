# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is lakeFS

lakeFS is an open-source data version control system (Git for data) that transforms object storage (S3, Azure Blob, GCS) into a Git-like repository with branches, commits, merges, and diffs. It is S3 API-compatible and integrates with Spark, Hive, Athena, DuckDB, etc.

The Go module is `github.com/treeverse/lakefs`.

## Build Commands

```bash
# Build both binaries (lakefs + lakectl); also runs gen-ui, gen-api, gen-code, clients
make build

# Build binaries only (skip codegen)
make build-binaries

# Build UI only
make gen-ui                    # runs npm install + vite build in webui/

# Generate all code (protos, mocks, API, UI, SDK clients)
make gen
make gen-api                   # regenerates pkg/api/apigen/lakefs.gen.go etc.
make gen-code                  # regenerates mocks, permission enums, wrappers
make gen-proto                 # regenerates .pb.go files via buf

# Build Docker image
make build-docker
```

## Running Tests

```bash
# Full test suite (Go + Hadoop FS)
make test

# Go tests only (with race detector, coverage)
make test-go

# Go tests without codegen (faster if already generated)
make run-test

# Go tests without race detector (fastest)
make fast-test

# Single Go test or package
go test -count=1 -race ./pkg/graveler/...
go test -count=1 -race -run TestMySpecificTest ./pkg/catalog/...

# Frontend tests
cd webui && npm test            # vitest run (one-shot)
cd webui && npm run test-watch  # vitest watch mode

# System/integration tests (requires running lakeFS instance)
make esti
make system-tests
```

## Lint

```bash
# Lint everything (Go + UI)
make lint

# Go only
go tool golangci-lint run ./...

# UI only
cd webui && npm run lint

# Check UI formatting
cd webui && npm run format:check

# Auto-format Go
make gofmt

# Auto-format UI
cd webui && npm run format
```

## Validation (CI checks)

```bash
# Run all CI validation steps
make checks-validator

# Individual validators (check that generated files are up-to-date)
make validate-api
make validate-proto
make validate-mockgen
make validate-permissions-gen
```

## Architecture

### Two Binaries

- **`cmd/lakefs`** — the server. Handles S3-compatible gateway, REST API, UI serving, background jobs (GC, etc.)
- **`cmd/lakectl`** — the CLI client for interacting with lakeFS

### Core Layers (bottom-up)

```
Block Store (pkg/block)
    ↓
KV Store (pkg/kv)
    ↓
Graveler (pkg/graveler)
    ↓
Catalog (pkg/catalog)
    ↓
API / Gateway (pkg/api, pkg/gateway)
```

**`pkg/block`** — Storage adapters for S3 (`block/s3`), Azure (`block/azure`), GCS (`block/gs`), local filesystem (`block/local`), and in-memory (`block/mem`). All implement the `adapter.go` interface. The `factory/` sub-package selects the adapter from config.

**`pkg/kv`** — Key-value store abstraction used for metadata (not object data). Backends: PostgreSQL (`kv/postgres`), DynamoDB (`kv/dynamodb`), CosmosDB (`kv/cosmosdb`), local (`kv/local`), in-memory (`kv/mem`). The `store.go` interface is the central abstraction.

**`pkg/graveler`** — The versioning engine. Implements Git-like semantics (branches, commits, diffs, merges) on top of KV. Key sub-packages:
- `graveler/committed/` — immutable SSTable-like committed data ranges stored in object storage
- `graveler/branch/` — branch locking and protection logic
- `graveler/settings/` — per-repository settings stored in KV

**`pkg/catalog`** — Higher-level data catalog built on Graveler. The `Store` interface in `catalog.go` is the main entry point used by the API layer. Manages entries (files), listings, imports, GC.

**`pkg/api`** — REST API server generated from `api/swagger.yml` (OpenAPI). The generated server stub is in `apigen/lakefs.gen.go`. `controller.go` implements the handlers. Served via `serve.go`.

**`pkg/gateway`** — S3-compatible HTTP gateway. Translates S3 API calls into lakeFS catalog operations. `handler.go` is the entry point; `operations/` contains per-operation handlers.

**`pkg/auth`** — Authentication and authorization. Supports built-in auth (stored in KV) and external auth services. `basic_service.go` is the primary implementation.

**`pkg/actions`** — Webhook/hook system. Runs user-defined actions on lakeFS events (pre-commit, post-commit, etc.).

**`pkg/pyramid`** — Tiered local cache for object storage blocks (hot cache on local disk).

### REST API

The single source of truth for the REST API is **`api/swagger.yml`**. Running `make gen-api` regenerates `pkg/api/apigen/lakefs.gen.go` using `oapi-codegen`. Do not hand-edit the generated file.

Similarly, `api/authentication.yml` and `api/authorization.yml` drive auth service code generation.

**Authentication** — four schemes are supported (declared in the swagger):
- `basic_auth` — HTTP Basic (access key ID + secret)
- `jwt_token` — Bearer JWT (obtained via `POST /auth/login`)
- `cookie_auth` — session cookie (`internal_auth_session`)
- `oidc_auth` / `saml_auth` — SSO cookies

**API endpoint groups** (all under `/api/v1`):

| Group | Prefix | Purpose |
|---|---|---|
| Health / Config | `/healthcheck`, `/config` | Server health, storage config, version |
| Setup | `/setup_lakefs`, `/setup_comm_prefs` | First-run setup |
| Auth | `/auth/...` | Users, groups, policies, credentials, external principals |
| Repositories | `/repositories`, `/repositories/{repository}` | CRUD for repos, dump/restore |
| Branches | `.../branches/{branch}` | Create/delete branches, commits, diff, revert, cherry-pick, hard reset, import |
| Objects | `.../objects` | Upload/download/delete/stat/copy objects; multipart (`pmpu`) |
| Refs | `.../refs/{ref}` | List objects, diff between refs, commits on ref, symlinks |
| Commits | `.../commits/{commitId}` | Read commit metadata |
| Tags | `.../tags/{tag}` | CRUD tags |
| Merges | `.../refs/{sourceRef}/merge/{destinationBranch}` | Merge branches |
| Pulls | `.../pulls/{pull_request}` | Pull requests and merge |
| Actions | `.../actions/runs` | List/get action runs and hook outputs |
| GC | `.../gc/...` | Garbage collection rules, prepare-commits |
| Metadata | `.../metadata/...` | Low-level SSTable range/meta-range access |
| Branch protection | `.../branch_protection`, `.../settings/branch_protection` | Protection rules |
| Statistics | `/statistics`, `/usage-report/summary` | Usage stats |

The full interactive API reference is at https://docs.lakefs.io/reference/api/oss/.

### Frontend

The web UI lives in `webui/` and is a React + TypeScript + Vite application. It talks to the lakeFS REST API. Built assets land in `webui/dist/` and are embedded into the `lakefs` binary via Go's embed.

### System Tests (Esti)

`esti/` contains end-to-end system tests (named "Esti"). They run against a live lakeFS instance. Not meant to be run as part of the regular `make test` unit test flow — use `make esti` or `make system-tests` for these.

### Generated Code

Many files are auto-generated and should not be edited by hand:
- `pkg/api/apigen/lakefs.gen.go` — from `api/swagger.yml`
- `pkg/*/mock/*.go` — from `go:generate` mockgen directives
- `pkg/permissions/actions.gen.go` — from `go:generate`
- `*.pb.go` files — from protobuf definitions via buf
- `clients/python/`, `clients/java/`, `clients/rust/` — from OpenAPI generator

### Contrib

`contrib/auth/` contains optional auth backends (ACL-based auth) that are not compiled into the default binary.
