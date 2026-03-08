# TiDB Cloud Cluster Provisioning Guide

Follow these steps before running `./run_all.sh`. Expect ~10 minutes for a first setup.

---

## 1. Create a TiDB Cloud account

1. Go to [https://tidbcloud.com](https://tidbcloud.com) and sign up.
2. Verify email and log in.

---

## 2. Pick a tier (Serverless first)

Default starting point: **Serverless (Starter)** for fast PoC validation.

Use `./run_all.sh` intake wizard to choose tier automatically from requirements:

- `serverless` for pilot/dev where PITR/regional HA/CDC enterprise controls are not required
- `essential` for production-style baseline with PITR and regional options
- `premium` for stronger enterprise controls and CDC without whitelist assumptions
- `dedicated` for VPC peering or long retention needs
- `byoc` for own-account deployment, sovereignty, customer IAM/KMS/spend control

The wizard writes:
- `results/pre_poc_checklist.md`
- `results/pre_poc_intake.json`
- `results/config.resolved.yaml`

---

## 3. Create your cluster

### Option A: Serverless (Starter) — fastest path

1. Click **Create Cluster**.
2. Select **TiDB Cloud Serverless**.
3. Choose region closest to app clients.
4. Create and wait for **Available**.

### Option B: Dedicated / BYOC

1. Click **Create Cluster**.
2. Select **TiDB Cloud Dedicated** or **BYOC** per intake decision.
3. Choose cloud provider and region.
4. For Dedicated/BYOC HTAP testing, provision TiFlash nodes.
5. Wait for cluster status **Available**.

---

## 4. Configure network access

1. In cluster detail page, open **Security -> Network Access**.
2. Add your runner IP (or approved CIDR).
3. Save.

---

## 5. Set database password

1. In cluster page, click **Connect**.
2. Generate or set password.
3. Copy it securely.

---

## 6. Fill `config.yaml`

```yaml
tidb:
  host:     "gateway01.us-west-2.prod.aws.tidbcloud.com"
  port:     4000
  user:     "<prefix>.root"
  password: "YOUR_PASSWORD_HERE"
  database: "test"
  ssl:      true
```

Optional: fill `comparison_db` for side-by-side benchmarking against MySQL-family targets (Aurora MySQL, MySQL, RDS MySQL, SingleStore) or to capture PostgreSQL/SQL Server comparison plans.

---

## 7. Run the kit (with checklist + decision flow)

```bash
chmod +x run_all.sh
./run_all.sh config.yaml
```

The intake will ask tier/security questions, then run the full PoV automatically.

Useful flags:

```bash
./run_all.sh config.yaml --wizard
./run_all.sh config.yaml --no-wizard
./run_all.sh config.yaml --tier serverless
./run_all.sh config.yaml --allow-blocked
```

---

## 8. Optional: add your own queries (Module 0)

```yaml
customer_queries:
  - name: "get_user_by_id"
    sql:  "SELECT * FROM users WHERE id = ?"
    params: [1]
  - name: "balance_check"
    sql:  "SELECT balance FROM accounts WHERE user_id = ? AND currency = ?"
    params: [1, "USD"]
```

These are validated in M0 and included in M1 workload mix.
