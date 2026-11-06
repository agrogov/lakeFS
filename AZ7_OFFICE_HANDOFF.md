# AZ7 office handoff — real Azure verification of Metadata Search

Written 2026-11-07 on the home Mac, which has no access to the hr2 cluster. Everything needed to
finish AZ7 from the office Mac is in this file and in the pushed branch; nothing on the home Mac
(its Kanban board, agent sessions, worktrees, scratch files) is needed.

Design and plan: [`METADATA_SEARCH_AZURE.md`](METADATA_SEARCH_AZURE.md) (§5 AZ7, and the
"Verification after AZ1–AZ6" section). Engine recipe to mirror:
[`esti/ops/mds-engines/README.md`](esti/ops/mds-engines/README.md).

## 1. Where things stand

| Task | State | Commit |
|---|---|---|
| AZ1 location translation in the catalog | done | `b79a41e` |
| AZ2 namespace root, companion namespace, deny ARNs | done | `fdedd3e` |
| AZ3 indexer Azure IO and `metadata_search.azure` | done | `ed7c0d3` |
| AZ4 Go integration test on Azurite | done | `21fbf95` |
| AZ5 esti end to end on Azurite | done | `cff0360` |
| AZ6 docs and design records | done | `8a28450` |
| **AZ7 real Azure verification on hr2** | **not run** — needs the office network | — |

AZ7 is the gate before the Azure support is called done: Azurite has no `dfs` endpoint and no HNS,
and Trino/Spark cannot run against it.

## 2. Get the code on the office Mac

```bash
git clone https://github.com/agrogov/lakeFS.git   # or: git fetch origin in an existing clone
cd lakeFS
git checkout feature/metadata-search
git pull --ff-only
git log --oneline -1   # must include this handoff commit or later
```

Build the server/indexer binary (one binary serves both roles):

```bash
go build -o lakefs-sso -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=az7-$(git rev-parse --short HEAD)" ./cmd/lakefs-sso
```

Only `cmd/lakefs-sso` wires the Iceberg catalog and Metadata Search. The repository `Dockerfile`
builds `cmd/lakefs` and has **no `lakefs-sso` target**, so the image for hr2 has to come from
however hr2's `lakefs-sso` image is built today (registry/pipeline), at this branch's head.
Record the commit SHA you deploy.

## 3. Inputs to collect in the office (fill in)

| Input | Value |
|---|---|
| kubectl context for hr2 | |
| namespace lakeFS runs in | |
| `lakefs-sso` image (registry/repo:tag or digest) built at this branch | |
| storage account | |
| container | |
| prefix for the test repository (path form, **not** the container root) | |
| account is FNS or HNS (`az storage account show --query isHnsEnabled`) | |
| lakeFS ingress URL that reaches `/mds/iceberg/api` | |
| admin access key pair | |

## 4. Steps

1. **Deploy the server** with the image above and:
   - `blockstore.type: azure`, `blockstore.azure.storage_account` / `storage_access_key` (shared key);
   - `iceberg.enabled: true`, `iceberg.metadata_search.enabled: true`;
   - leave `iceberg.azure.location_host` at its default `dfs` for the first run.
   On start, bootstrap creates user `mds-indexer` (group `MetadataSearchIndexers`). As admin, create
   an access key for `mds-indexer`.
2. **Run the indexer** from the Kubernetes example in `docs/metadata-search.md`
   (section "Azure Blob Storage", the Secret + Deployment around line 365): same image, args
   `mds run --config /etc/lakefs/config.yaml`, with the `mds-indexer` credentials. Its Azure IO
   credentials come **only** from the env vars
   `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCOUNT` and `LAKEFS_METADATA_SEARCH_AZURE_STORAGE_ACCESS_KEY`
   (the indexer does not read `blockstore.azure.*`; that is by design, D3).
3. **Create one repository** on `https://<account>.blob.core.windows.net/<container>/<prefix>` and
   upload three objects under `orders/`, with user metadata, the same shape as the SeaweedFS record
   in `esti/ops/mds-engines/README.md` ("Observed result"). Commit. Port-forward the indexer's
   `listen_address` and wait until `/_health` shows `last_indexed` equal to the branch head.
4. **Spark**: the "Spark" section of `docs/metadata-search.md` (Azure part around line 751):
   `ADLSFileIO`, `--packages org.apache.iceberg:iceberg-azure-bundle:1.8.1`, the
   `adls.auth.shared-key.*` keys. Use the same image as the SeaweedFS run for comparability:
   `tabulario/spark-iceberg@sha256:790a72e744e0d09f161e1b4c7c2f79625cc598cf0476da4703e62f1f4e023460`.
   Run on both endpoints (`/mds/iceberg/api` and `/iceberg/relative_to/<repo>.main/api`).
5. **Trino**: the "Trino" section (Azure part around line 847). Pin the image digest; the SeaweedFS
   run used `trinodb/trino@sha256:2704eb53935af5e582d63d6033e9bf99d20c325e9b0e30b1016ef59a4773979f`.
   Check which switch that version accepts: `fs.azure.enabled` or `fs.native-azure.enabled`, and
   record it. `azure.auth-type=ACCESS_KEY` + `azure.access-key`.
6. **FNS gate**: if the account is FNS and Spark or Trino fails on the `dfs` host, set
   `iceberg.azure.location_host: blob`, restart the server, rerun, and record which engine needed it.
   HNS accounts should work on `dfs` as is.
7. **Record** in `esti/ops/mds-engines/README.md`, a new "Azure (hr2)" section shaped like the
   SeaweedFS one: "Versions used" table (lakefs-sso SHA, Spark/Trino images, Iceberg runtime,
   account kind FNS/HNS, `location_host` used), "Observed result" (the three rows: path, size,
   commit_id, user_metadata, per engine and endpoint), and "Snippet changes this run required".
   Remove secrets and account keys from anything you paste.
8. **Close out**: mark AZ7 done in `METADATA_SEARCH_AZURE.md` §5 and the status line at the top,
   and the Azure row of `METADATA_SEARCH_PLAN.md`; delete this handoff file in the same commit.
   One docs-only commit, no AI attribution lines.

## 5. Rules

- Don't change anything on hr2 without the owner's say-so for each step (deploy, restart,
  indexer start, repository creation).
- If something fails on the real account, record it; don't change the design (D1–D4) to make the
  run pass. Code fixes go in their own commit with a test.

## 6. Open items not caused by the Azure work (can be done anywhere)

- `pkg/mds/table` `TestVendedCredentials` fails with an S3 403 (`InvalidAccessKeyId` on PutObject);
  it fails the same way on `8047321`, the commit before AZ1.
- `golangci-lint` never ran on AZ1–AZ6: with Go 1.27.2 it stops at type-checking ("export data
  version 5 is greater than maximum supported version 4"). Run it with a linter built for Go 1.27,
  or with the module's Go 1.26 toolchain:
  `go tool golangci-lint run ./pkg/icebergcatalog/... ./pkg/mds/... ./pkg/block/... ./esti/...`
- The Azurite tests need Docker. On a colima machine, `pkg/block/azure` panics instead of skipping
  unless `DOCKER_HOST=unix://$HOME/.colima/default/docker.sock` is set.

## 7. Optional: rerun AZ7 as a Kanban task in the office

The home Kanban board is not available in the office. To hand AZ7 to an agent there, create a task
on the office board (it must be able to run shell commands, unlike the home `pi-chat` session):

```bash
kanban task create --project-path "$PWD" --base-ref feature/metadata-search \
  --auto-review-enabled true --auto-review-mode commit --start-in-plan-mode true \
  --title "AZ7 — Real Azure verification on hr2 (operator gate)" \
  --prompt "Follow AZ7_OFFICE_HANDOFF.md exactly. Section 3 values: <paste>. Work in plan mode; ask for approval before every step that changes the cluster. One docs-only commit, no AI attribution lines."
```
