# AWS Customer-Account Runner Setup (Vercel Control Plane)

This setup keeps nearly all run infrastructure cost in the customer account.

## Target account values (provided)
- Account ID: `219248915861`
- Region: `us-east-1`
- VPC: `vpc-0d2122788fc944c5c`
- Subnet: `subnet-08a374994d1a8c902`
- Runner SG: `sg-01014111d2efb1ab2`

## Cost model
- Customer pays: EC2, EBS, data transfer, NAT/PrivateLink/network, CloudWatch logs in customer account.
- You pay: Vercel + tiny control metadata. If AMI snapshots are owned by your AWS account, snapshot storage remains billed to your account.

## 1) Control-plane identity (minimal AWS footprint)
Even with Vercel-only control plane, you still need one AWS IAM principal that can call `sts:AssumeRole`.

Create in your management AWS account:
- Role name: `TidbPovVercelAssumerRole`
- Permission policy (minimum):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::219248915861:role/TidbPovCustomerRunnerLaunchRole"
    }
  ]
}
```

You will use this role ARN in customer trust policy:
- `arn:aws:iam::<CONTROL_PLANE_ACCOUNT_ID>:role/TidbPovVercelAssumerRole`

## 2) External ID
Use a high-entropy external ID (generated):
- `tidbpov-56b91434026898ead6ec93666188eae8`

Store in Vercel secret/env:
- `AWS_EXTERNAL_ID=tidbpov-56b91434026898ead6ec93666188eae8`

## 3) Customer launch role (role ARN you asked about)
In account `219248915861`, create role:
- Role name: `TidbPovCustomerRunnerLaunchRole`

### 3a) Trust policy (replace control-plane account ID)
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::<CONTROL_PLANE_ACCOUNT_ID>:role/TidbPovVercelAssumerRole"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "tidbpov-56b91434026898ead6ec93666188eae8"
        }
      }
    }
  ]
}
```

### 3b) Permission policy (attach to role)
Also create a runner instance profile role (next section) and replace `<RUNNER_ROLE_ARN>`.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DescribeOnly",
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeInstances",
        "ec2:DescribeInstanceStatus",
        "ec2:DescribeImages",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeVpcs",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeVolumes"
      ],
      "Resource": "*"
    },
    {
      "Sid": "RunInstancesConstrained",
      "Effect": "Allow",
      "Action": "ec2:RunInstances",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "aws:RequestedRegion": "us-east-1"
        },
        "ForAllValues:StringEquals": {
          "ec2:InstanceType": [
            "c7i.2xlarge",
            "c7i.4xlarge",
            "c7i.8xlarge"
          ]
        }
      }
    },
    {
      "Sid": "TagOnCreate",
      "Effect": "Allow",
      "Action": "ec2:CreateTags",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "ec2:CreateAction": "RunInstances"
        }
      }
    },
    {
      "Sid": "TerminateOnlyManaged",
      "Effect": "Allow",
      "Action": "ec2:TerminateInstances",
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "ec2:ResourceTag/tidb-pov-managed": "true"
        }
      }
    },
    {
      "Sid": "PassRunnerRoleOnly",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "<RUNNER_ROLE_ARN>"
    }
  ]
}
```

## 4) Runner instance profile role (in customer account)
Create IAM role: `TidbPovRunnerInstanceRole`
- Trusted entity: EC2
- Attach managed policy: `AmazonSSMManagedInstanceCore`

Create instance profile and attach role:
- Instance profile name: `TidbPovRunnerInstanceRole` (same-name auto profile is fine)

## 5) VPC/subnet/SG restrictions
At launch time, always set:
- subnet: `subnet-08a374994d1a8c902`
- SG: `sg-01014111d2efb1ab2`

SG guidance:
- Inbound: none
- Outbound:
  - 4000/tcp to TiDB endpoint path
  - 443/tcp for control APIs/SSM

## 6) Role ARN and External ID retrieval
After creating `TidbPovCustomerRunnerLaunchRole`:
- Open IAM role details
- Copy **Role ARN** from summary (this is the value the UI stores)
- Trust relationship shows `sts:ExternalId` (must match Vercel env secret)

## 7) Instance-size presets + max instances
Preset sizing (starting point; tune by observed CPU/network):
- Small (`<= 50k QPS` target): `c7i.2xlarge`, max `2` instances/run
- Medium (`<= 200k QPS` target): `c7i.4xlarge`, max `8` instances/run
- Large (`500k-1M QPS` target): `c7i.8xlarge`, max `24` instances/run

Note: 1M QPS generally requires multiple load generators, warm cache, low-latency network path, and point-get style SQL.

## 8) Private AMI sharing
Private AMI behavior:
- Not publicly searchable.
- Share explicitly to specific AWS account IDs.
- Shared accounts can use it, others cannot.
- If encrypted with CMK, KMS key access must also be shared.

## 9) Current environment values (ready to use)
- Launch role ARN: `arn:aws:iam::219248915861:role/TidbPovCustomerRunnerLaunchRole`
- Assumer role ARN: `arn:aws:iam::219248915861:role/TidbPovVercelAssumerRole`
- Runner role ARN: `arn:aws:iam::219248915861:role/TidbPovRunnerInstanceRole`
- Runner instance profile name: `TidbPovRunnerInstanceRole`
- External ID: `tidbpov-56b91434026898ead6ec93666188eae8`

Suggested Vercel environment variables:
- `AWS_REGION=us-east-1`
- `AWS_EXTERNAL_ID=tidbpov-56b91434026898ead6ec93666188eae8`
- `AWS_CUSTOMER_ASSUME_ROLE_ARN=arn:aws:iam::219248915861:role/TidbPovCustomerRunnerLaunchRole`
- `AWS_CUSTOMER_ACCOUNT_ID=219248915861`
- `AWS_DEFAULT_VPC_ID=vpc-0d2122788fc944c5c`
- `AWS_DEFAULT_SUBNET_ID=subnet-08a374994d1a8c902`
- `AWS_DEFAULT_SECURITY_GROUP_ID=sg-01014111d2efb1ab2`
- `AWS_RUNNER_INSTANCE_PROFILE_NAME=TidbPovRunnerInstanceRole`
- `AWS_RUNNER_ROLE_ARN=arn:aws:iam::219248915861:role/TidbPovRunnerInstanceRole`
