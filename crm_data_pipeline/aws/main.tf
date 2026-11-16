terraform {
  required_version = ">= 1.5.0"
  required_providers { aws = { source = "hashicorp/aws", version = ">= 5.0, < 6.0" } }
}
provider "aws" { region = var.aws_region }
variable "aws_region" { type = string, default = "eu-west-2" }
variable "bucket_name" { type = string, description = "Globally unique landing bucket name" }
variable "kms_key_arn" { type = string, description = "Customer-managed KMS key ARN" }
variable "environment" { type = string, default = "dev" }
resource "aws_s3_bucket" "crm_landing" {
  bucket = var.bucket_name
  tags = { Application="crm-medallion", Environment=var.environment, ManagedBy="terraform" }
}
resource "aws_s3_bucket_public_access_block" "crm_landing" {
  bucket=aws_s3_bucket.crm_landing.id
  block_public_acls=true
  block_public_policy=true
  ignore_public_acls=true
  restrict_public_buckets=true
}
resource "aws_s3_bucket_versioning" "crm_landing" {
  bucket=aws_s3_bucket.crm_landing.id
  versioning_configuration { status="Enabled" }
}
resource "aws_s3_bucket_server_side_encryption_configuration" "crm_landing" {
  bucket=aws_s3_bucket.crm_landing.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm="aws:kms", kms_master_key_id=var.kms_key_arn }
    bucket_key_enabled=true
  }
}
output "landing_bucket_name" { value=aws_s3_bucket.crm_landing.bucket }
