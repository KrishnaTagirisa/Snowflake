#!/usr/bin/env bash
# Landing bucket: encrypted, versioned, private, with lifecycle tiering.
set -euo pipefail

BUCKET="acme-retail-landing"
REGION="eu-west-2"
KMS_KEY_ARN="arn:aws:kms:${REGION}:<AWS_ACCOUNT_ID>:key/<KMS_KEY_ID>"

aws s3api create-bucket --bucket "$BUCKET" --region "$REGION" \
  --create-bucket-configuration LocationConstraint="$REGION"

aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption --bucket "$BUCKET" --server-side-encryption-configuration "{
  \"Rules\": [{
    \"ApplyServerSideEncryptionByDefault\": {\"SSEAlgorithm\": \"aws:kms\", \"KMSMasterKeyID\": \"${KMS_KEY_ARN}\"},
    \"BucketKeyEnabled\": true
  }]
}"

aws s3api put-bucket-lifecycle-configuration --bucket "$BUCKET" --lifecycle-configuration '{
  "Rules": [{
    "ID": "tier-landing-files",
    "Status": "Enabled",
    "Filter": {"Prefix": "retail/"},
    "Transitions": [
      {"Days": 30, "StorageClass": "STANDARD_IA"},
      {"Days": 90, "StorageClass": "GLACIER"}
    ],
    "NoncurrentVersionExpiration": {"NoncurrentDays": 30}
  }]
}'

# Expected layout written by the source system:
#   s3://acme-retail-landing/retail/customers/load_date=2026-11-11/customers_20261111.csv
#   s3://acme-retail-landing/retail/products/load_date=2026-11-11/products_20261111.csv
#   s3://acme-retail-landing/retail/orders/load_date=2026-11-11/orders_20261111.json
#   s3://acme-retail-landing/retail/_control/load_date=2026-11-11/_SUCCESS   <- written last
echo "Bucket ${BUCKET} configured."
