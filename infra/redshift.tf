# -------------------------
# Redshift Serverless
# -------------------------

# Namespace — logical container for database objects and users
resource "aws_redshiftserverless_namespace" "main" {
  namespace_name      = "${var.project}-namespace"
  db_name             = var.redshift_database_name
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password

  iam_roles = [aws_iam_role.redshift_s3_role.arn]

  tags = {
    Name = "${var.project}-namespace"
  }
}

# Workgroup — compute resources for the namespace
resource "aws_redshiftserverless_workgroup" "main" {
  namespace_name = aws_redshiftserverless_namespace.main.namespace_name
  workgroup_name = "${var.project}-workgroup"
  base_capacity  = var.redshift_base_capacity

  subnet_ids         = [
    aws_subnet.public_a.id,
    aws_subnet.public_b.id,
    aws_subnet.public_c.id,
  ]
  security_group_ids = [aws_security_group.redshift.id]

  # Allow public accessibility so you can connect from local machine
  publicly_accessible = true

  tags = {
    Name = "${var.project}-workgroup"
  }
}
