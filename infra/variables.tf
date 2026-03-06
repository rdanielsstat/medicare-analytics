variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project name used for resource naming"
  type        = string
  default     = "medicare-analytics"
}

# -------------------------
# S3
# -------------------------
variable "s3_raw_bucket_name" {
  description = "Name of the S3 bucket for raw data lake storage"
  type        = string
}

# -------------------------
# Redshift Serverless
# -------------------------
variable "redshift_admin_username" {
  description = "Redshift Serverless admin username"
  type        = string
}

variable "redshift_admin_password" {
  description = "Redshift Serverless admin password"
  type        = string
  sensitive   = true
}

variable "redshift_database_name" {
  description = "Name of the Redshift database"
  type        = string
  default     = "medicare_db"
}

variable "redshift_base_capacity" {
  description = "Base RPU capacity for Redshift Serverless (32 minimum)"
  type        = number
  default     = 32
}

# -------------------------
# EC2
# -------------------------
variable "ec2_instance_type" {
  description = "EC2 instance type for Airflow"
  type        = string
  default     = "t3.medium"
}

variable "ec2_key_pair_name" {
  description = "Name of the EC2 key pair for SSH access"
  type        = string
}

variable "allowed_ssh_cidr" {
  description = "Your IP address in CIDR notation for SSH access (e.g. 1.2.3.4/32)"
  type        = string
}
