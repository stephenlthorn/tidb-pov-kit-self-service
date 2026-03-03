# Pre-PoC Checklist: Security Architecture and Shared Responsibility

Use this checklist before committing PoC effort. The same checks are executed by `setup/pre_poc_intake.py` and saved per-run under `results/pre_poc_checklist.md`.

## 1) Tier gate (fast decision tree)

1. Need deployment inside customer cloud account/VPC for sovereignty/compliance/IAM/KMS/spend?
- Yes -> **BYOC**
- No -> continue

2. Need VPC peering (not only private endpoint/private link)?
- Yes -> **Dedicated**
- No -> continue

3. Need PITR?
- Yes -> **Essential+**
- Also need backup retention up to 90 days? -> **Dedicated**

4. Production cross-AZ/regional failover required?
- Yes -> **Essential+**
- No -> **Serverless** candidate

5. Need CDC/Changefeed?
- Yes -> **Premium/BYOC/Dedicated** preferred (Essential may require whitelist)

6. Need enterprise controls (maintenance window/CMEK/audit governance)?
- Yes -> **Premium+**
- No -> keep previous candidate

## 2) Shared responsibility readiness (go/no-go)

Mark each item `PASS`, `FAIL`, or `N/A`.

- Shared responsibility model reviewed and accepted.
- Data ownership/residency boundary is acceptable for selected tier.
- Network boundaries are defined (PrivateLink/private endpoint/peering as required).
- Customer can manage VPC/SG/NACL controls where applicable.
- CMK/key-lifecycle ownership model is acceptable where applicable.
- Elevated bootstrap IAM roles can be removed after provisioning.
- JIT support access model is acceptable (customer bastion + customer-controlled enablement).
- CloudTrail + TiDB audit logs retention and SIEM ingestion path is defined.
- Supply-chain scanning process exists for deployable artifacts where applicable.
- Control-plane severance behavior is acceptable (data plane continues; automation paused).

## 3) Continue / hold decision

- **Proceed**: no blocking `FAIL` items.
- **Proceed with risks**: only non-blocking `FAIL` items.
- **Hold**: any blocking `FAIL` item remains unresolved.

## 4) Common issues to avoid

- IP/network allowlist missing before run.
- Tier selected does not support modules enabled in config.
- TiFlash not provisioned/replicated before HTAP or vector tests.
- Import path mismatch for `IMPORT INTO` in cloud environment.
- Data scale too large for first validation run.
