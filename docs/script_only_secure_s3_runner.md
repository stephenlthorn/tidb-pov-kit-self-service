# Script-Only Deployment (Any VM) with Secure S3 Archival

This path removes Vercel/UI entirely. Runs happen from scripts on any VM and are **blocked** unless S3 preflight + upload succeed.

## 1) Security model (recommended)

Use a PingCAP-owned results bucket and isolate each customer by prefix:

- Bucket: `pingcap-tidb-pov-results`
- Prefix: `tidb-pov/<customer-slug>/...`
- Encryption: SSE-KMS required
- Access: least privilege IAM role, prefix-scoped
- Credentials: short-lived STS (`assume-role`) whenever possible

## 2) AWS setup

### 2.1 Create/prepare bucket

- Enable **Block Public Access** (all 4 settings)
- Enable **Bucket Versioning**
- Enable **Default Encryption** with your KMS key (SSE-KMS)
- Optionally add lifecycle:
  - `healthchecks/*` expire in 7 days
  - run artifacts transition/archive by policy

### 2.2 Apply bucket policy template

Use:
- `docs/aws/policies/pov_results_bucket_policy_template.json`

Replace:
- `${BUCKET_NAME}`
- `${PREFIX}` (for example `tidb-pov`)
- `${PROJECT}` (for example `customer-acme`)
- `${KMS_KEY_ARN}`
- `${UPLOADER_ROLE_ARN}`

### 2.3 Create uploader IAM policy and role

Use:
- `docs/aws/policies/pov_uploader_role_policy_template.json`

Attach it to a role (for EC2 instance profile or cross-account assume-role).

## 3) VM runtime credentials

### Option A (best on AWS): EC2 instance profile
Attach the uploader role to the VM instance profile.

### Option B (any VM): STS assume-role
Use base AWS creds that can assume the uploader role, then run:

```bash
eval "$(scripts/aws_assume_role_env.sh arn:aws:iam::<acct-id>:role/<uploader-role> <external-id> us-east-1)"
```

This exports short-lived AWS credentials for the current shell.

## 4) Run the PoV script workflow

1. Copy `scripts/pov_vm.env.example` to your own env file.
2. Fill required values (bucket, project, config path, owner account, kms key).
   - Keep multi-word args quoted, for example:
     - `POV_RUN_ARGS="--no-menu --no-wizard"`
3. Run:

```bash
export POV_ENV_FILE=/absolute/path/to/pov_vm.env
curl -fsSL https://raw.githubusercontent.com/stephenlthorn/tidb-pov-kit-self-service/main/scripts/pov_pull_run_upload.sh | bash
```

## 5) Fail-closed behavior

`run_all.sh` enforces archival by default (`POV_ENFORCE_S3_UPLOAD=true`):

- S3 write/read preflight before tests
- hard fail if final upload fails
- run is not treated as successful unless archive completes

## 6) Required environment variables

- `POV_S3_BUCKET`
- `POV_S3_PREFIX`
- `POV_S3_PROJECT`
- `POV_S3_REGION`
- `POV_ENFORCE_S3_UPLOAD=true`
- `POV_S3_EXPECTED_BUCKET_OWNER` (recommended)
- `POV_S3_KMS_KEY_ID` (recommended / required if bucket policy enforces KMS)

Optional:

- `POV_S3_ROLE_ARN`
- `POV_S3_EXTERNAL_ID`
- `POV_S3_SESSION_NAME`

## 7) Verify result intake

Each run uploads a manifest:

`tidb-pov/<project>/runs/<run-tag>/manifest.json`

Use this manifest as the source of truth for audit and reporting.
