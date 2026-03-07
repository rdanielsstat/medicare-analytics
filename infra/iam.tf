# -------------------------
# IAM — EC2 role (allows Airflow to access S3 and Redshift)
# -------------------------
resource "aws_iam_role" "ec2_airflow_role" {
  name = "${var.project}-ec2-airflow-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
    }]
  })
}

# S3 access for EC2
resource "aws_iam_role_policy" "ec2_s3_policy" {
  name = "${var.project}-ec2-s3-policy"
  role = aws_iam_role.ec2_airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.raw.arn,
        "${aws_s3_bucket.raw.arn}/*"
      ]
    }]
  })
}

# Redshift access for EC2
resource "aws_iam_role_policy" "ec2_redshift_policy" {
  name = "${var.project}-ec2-redshift-policy"
  role = aws_iam_role.ec2_airflow_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "redshift-serverless:GetWorkgroup",
        "redshift-serverless:GetNamespace",
        "redshift-serverless:GetCredentials",
        "redshift-data:ExecuteStatement",
        "redshift-data:GetStatementResult",
        "redshift-data:DescribeStatement"
      ]
      Resource = "*"
    }]
  })
}

# Instance profile — attaches the role to EC2
resource "aws_iam_instance_profile" "ec2_airflow_profile" {
  name = "${var.project}-ec2-airflow-profile"
  role = aws_iam_role.ec2_airflow_role.name
}

# -------------------------
# IAM — Redshift role (allows Redshift to read from S3)
# -------------------------
resource "aws_iam_role" "redshift_s3_role" {
  name = "${var.project}-redshift-s3-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy" "redshift_s3_policy" {
  name = "${var.project}-redshift-s3-policy"
  role = aws_iam_role.redshift_s3_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.raw.arn,
        "${aws_s3_bucket.raw.arn}/*"
      ]
    }]
  })
}
