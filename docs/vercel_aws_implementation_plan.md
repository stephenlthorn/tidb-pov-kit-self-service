# TiDB PoV: Vercel + AWS Implementation Plan

## 1) Goal
Build a production-ready SaaS-style PoV control plane where:
- Frontend is hosted on Vercel.
- Users authenticate with role-based access (`admin`, `user`).
- Users can configure and launch PoV runs.
- Workload generation runs inside AWS (preferably the customer's account/VPC) via a prebuilt AMI.
- Metrics + artifacts feed back into the app and PDF report reliably.

## 2) Architecture (target)

### Control Plane
- `apps/web` (Next.js on Vercel): UI, auth, RBAC-protected pages.
- `apps/api` (Next.js route handlers OR small Python service):
  - Stores configs/runs.
  - Issues signed run payloads.
  - Triggers orchestration jobs.
  - Ingests artifacts/metrics.
- `db` (Postgres): users, orgs, projects, run configs, run state, audit logs.
- `queue` (SQS preferred for GA reliability): run requests + status events.

### Execution Plane
- `workload-agent` (AMI image): runs PoV modules/workload generator on EC2.
- Launch model:
  - Option A (recommended): cross-account `AssumeRole` + `RunInstances` in customer account.
  - Option B: customer self-runs AMI and pastes one-time run token from UI.
- Agent flow:
  - boot with user-data
  - pull signed config
  - run selected modules/tests
  - push logs + results + metrics to signed upload endpoints

### Reporting
- Normalized run artifacts stored in object storage.
- Report service generates:
  - JSON summary
  - charts payload
  - PDF
- Dashboard reads run status + metrics + report readiness.

## 3) Scope split: what I do vs what you do

### I will implement
- UI + API code changes.
- RBAC model and protected routes.
- Job orchestration model + run state machine.
- AMI bootstrap scripts + agent packaging.
- Artifact ingestion + report pipeline wiring.
- Docs, templates, and validation tooling.

### You will provide / execute
- Cloud accounts and credentials (Vercel + AWS).
- Auth provider app credentials.
- DNS/domain and production environment variables.
- AWS IAM trust setup in customer account(s).
- Final legal/compliance decisions (public AMI exposure, data retention, OSS license/policy).

## 4) Phased implementation plan

## Phase 0 - Foundation (1-2 days)
Deliverables
- New `docs/architecture.md` with sequence diagrams.
- `.env.example` split by environment (`local`, `preview`, `prod`).
- Config schema (`zod` or `pydantic`) for wizard/manual paths.
- Run state model: `draft -> queued -> launching -> running -> collecting -> report_building -> completed|failed|canceled`.

Acceptance criteria
- All config paths produce one canonical run payload.
- Existing local app behavior remains usable during transition.

User steps
1. Confirm preferred stack for auth: `Auth.js + Google/Microsoft OAuth` (recommended) or another IdP.
2. Confirm whether we implement both orchestration options (A and B) or just A first.

## Phase 1 - Auth + RBAC on Vercel (2-3 days)
Deliverables
- Next.js auth integration.
- `users.role` and policy checks on server routes.
- Admin pages:
  - invite user
  - assign role
  - disable user
- Audit log for auth/admin actions.

Acceptance criteria
- `user` cannot access admin routes.
- `admin` can manage users and run visibility.

User steps
1. Create OAuth apps (Google/Microsoft/Okta) and share client IDs/secrets via env vars.
2. Create Vercel project and set env vars for preview/prod.

## Phase 2 - Simplified app flow (quick start + manual) (2-4 days)
Deliverables
- Keep two entry paths only:
  - Quick Start
  - Manual Configuration
- Manual configuration contains:
  - TiDB connection
  - comparison target
  - tests/modules
  - workload generator controls (nested)
- Run Review page becomes final preflight summary.

Acceptance criteria
- No duplicated settings across pages.
- Save/continue and save/run cannot break navigation.

User steps
1. Validate UX copy and defaults.
2. Approve required fields per tier (Starter/Essential/Premium/Dedicated).

## Phase 3 - Run orchestration API (3-5 days)
Deliverables
- API endpoints:
  - `POST /runs` create run
  - `POST /runs/{id}/start`
  - `GET /runs/{id}`
  - `POST /runs/{id}/cancel`
- Queue worker for run launch transitions.
- CLI stream endpoint for live logs in dashboard.

Acceptance criteria
- Runs are durable and resumable after process restart.
- Status and logs always reflect current run state.

User steps
1. Provide AWS account/region for control-plane services.
2. Approve queue/storage service choices.

## Phase 4 - Customer-account workload execution (AMI) (5-8 days)
Deliverables
- `workload-agent` bootstrap package.
- EC2 Image Builder recipe/pipeline definition.
- User-data templating and signed run manifest retrieval.
- Health + heartbeat reporting.
- Retry-safe upload of logs/artifacts.

Acceptance criteria
- Agent can execute end-to-end run from signed config.
- Works in same VPC/private connectivity path to TiDB.

User steps
1. Create customer-account IAM role with trust policy for control plane (external ID).
2. Provide VPC/subnet/security-group parameters for test account.
3. Validate network path to TiDB endpoint from EC2.

## Phase 5 - Metrics and report fidelity (3-5 days)
Deliverables
- Report data contract with required non-null fields.
- Warm workload phase integrated as first-class phase in run model.
- Collect:
  - module metrics
  - workload throughput/latency percentiles
  - comparison deltas
- Grafana/Prometheus ingestion option:
  - pull PromQL snapshots for report charts.

Acceptance criteria
- PDF never shows empty critical metrics when run completed.
- Warm-phase latency shown separately from cold/ramp phases.

User steps
1. If using Grafana, provide datasource access/token details.
2. Approve final report KPIs and chart list.

## Phase 6 - Hardening + OSS readiness (2-4 days)
Deliverables
- Secret scanning + config scrubber.
- Remove non-open-source-safe copy/content.
- License + NOTICE + contribution guide.
- Threat model + minimum security controls checklist.

Acceptance criteria
- No hardcoded credentials or internal-only language.
- Public repo passes security and compliance checks.

User steps
1. Confirm desired license (MIT/Apache-2.0).
2. Review and approve public-facing wording.

## Phase 7 - Deployment and operations (2-3 days)
Deliverables
- Vercel deployment config.
- Production runbook.
- Incident runbook (failed launch, stuck run, metrics missing).
- Cost controls + quotas.

Acceptance criteria
- New user can onboard and execute first PoV in under 30 minutes.

User steps
1. Configure production domain and DNS.
2. Configure alerting channels.
3. Run UAT checklist and sign-off.

## 5) Data model (minimum)
- `users(id, email, role, status, created_at)`
- `organizations(id, name)`
- `projects(id, org_id, name)`
- `run_configs(id, project_id, created_by, payload_json, version)`
- `runs(id, project_id, config_id, status, started_at, ended_at, error)`
- `run_events(id, run_id, ts, level, message, meta_json)`
- `run_artifacts(id, run_id, type, uri, checksum, size_bytes)`
- `audit_logs(id, actor_user_id, action, target_type, target_id, meta_json, ts)`

## 6) Security baseline (implementation requirements)
- Server-side authz checks on every mutating endpoint.
- Signed, short-lived URLs/tokens for agent config + artifact upload.
- Least-privilege IAM for launch roles.
- No inbound SSH required (prefer SSM).
- Immutable run config snapshot stored at run creation.
- Full audit trail for admin and run actions.

## 7) First execution sprint (recommended)
Sprint target (this week)
1. Implement Phase 0 + Phase 1.
2. Stabilize current manual/wizard config into one canonical payload.
3. Add run state machine tables + APIs (`create/start/status`).

Definition of done
- Deployed on Vercel preview with login and role protection.
- Admin can create users.
- Authenticated user can create a run draft and start it (mock runner allowed in sprint 1).

## 8) Risks / decisions to resolve early
- Public AMI vs private/share-by-account only.
- Cross-account orchestration vs customer self-execution mode.
- Queue/orchestration platform choice (`SQS worker` vs `Vercel Workflow`).
- Report metric source of truth (`local artifacts` vs `Prometheus/Grafana pull`).

## 9) Immediate next actions
1. I start Phase 0 implementation in code (schema, run state model, API skeleton, docs).
2. You complete account prerequisites (Vercel project + OAuth app + AWS target account for launch tests).
3. We run a joint checkpoint and then implement Phase 1 fully.
