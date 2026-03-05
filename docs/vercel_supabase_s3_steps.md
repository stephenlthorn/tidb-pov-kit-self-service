# Vercel + Supabase + S3 Setup Steps

## 1) Supabase persistence for users/invites/config

1. In Supabase, create a project in your preferred region.
2. Copy the Postgres connection string (Settings -> Database).
3. In Vercel project env vars, set one:
   - `DATABASE_URL=<supabase postgres url>` (recommended)
   - or `POSTGRES_URL_NON_POOLING=<url>`
4. Make sure URL includes SSL, for example `?sslmode=require`.
5. Redeploy Vercel.

Expected behavior after redeploy:
- Admin setup is shown once only.
- Admin/user accounts and invites persist across redeploys.

## 2) S3 for PDF/metrics/log artifact storage

### Create bucket
1. Create S3 bucket in `us-east-1` (or your chosen region).
2. Keep bucket private.

### Create IAM user/credentials for Vercel
1. Create IAM user for Vercel artifact uploads.
2. Generate Access Key + Secret Access Key.
3. Attach this least-privilege policy (replace bucket/prefix):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListPrefix",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::<BUCKET_NAME>"],
      "Condition": {
        "StringLike": {
          "s3:prefix": ["tidb-pov/*"]
        }
      }
    },
    {
      "Sid": "ObjectRW",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": ["arn:aws:s3:::<BUCKET_NAME>/tidb-pov/*"]
    }
  ]
}
```

### Vercel env vars for S3
Set in Vercel project env:
- `S3_ARTIFACTS_ENABLED=true`
- `S3_BUCKET=<BUCKET_NAME>`
- `S3_REGION=us-east-1`
- `S3_PREFIX=tidb-pov`
- `S3_ARTIFACTS_PROJECT=default`
- `AWS_ACCESS_KEY_ID=<IAM_ACCESS_KEY>`
- `AWS_SECRET_ACCESS_KEY=<IAM_SECRET_KEY>`
- `AWS_SESSION_TOKEN=<optional>`

Then redeploy.

## 3) App usage

1. Build report as normal.
2. In Dashboards, click `Sync Artifacts to S3`.
3. The app uploads:
   - `reports/tidb_pov_report.pdf`
   - `metrics/metrics_summary.json`
   - `logs/web_ui_run.log`
4. If local files are missing later (for example on Vercel cold start), report/metrics are pulled from S3 automatically.

## 4) Recommended production tweaks

- Use a dedicated S3 prefix per customer/project (`S3_ARTIFACTS_PROJECT`).
- Rotate IAM access keys periodically.
- Add S3 lifecycle policy for old artifacts (e.g., 90-180 days).
