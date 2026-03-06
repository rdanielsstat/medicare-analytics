terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }

  # Remote state in S3 (bootstrap this manually first — see README)
  # Once your S3 bucket exists, uncomment this block and run tofu init
  # backend "s3" {
  #   bucket = "medicare-analytics-tfstate"
  #   key    = "infra/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "medicare-analytics"
      Environment = var.environment
      ManagedBy   = "opentofu"
    }
  }
}
