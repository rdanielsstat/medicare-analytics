# -------------------------
# Outputs
# -------------------------

output "s3_raw_bucket_name" {
  description = "Name of the raw data lake S3 bucket"
  value       = aws_s3_bucket.raw.bucket
}

output "s3_raw_bucket_arn" {
  description = "ARN of the raw data lake S3 bucket"
  value       = aws_s3_bucket.raw.arn
}

output "redshift_workgroup_endpoint" {
  description = "Redshift Serverless workgroup endpoint for connections"
  value       = aws_redshiftserverless_workgroup.main.endpoint
}

output "redshift_database_name" {
  description = "Redshift database name"
  value       = var.redshift_database_name
}

output "ec2_public_ip" {
  description = "Public IP of the Airflow EC2 instance"
  value       = aws_eip.airflow.public_ip
}

output "ec2_ssh_command" {
  description = "SSH command to connect to the Airflow EC2 instance"
  value       = "ssh -i ~/.ssh/${var.ec2_key_pair_name}.pem ec2-user@${aws_eip.airflow.public_ip}"
}

output "airflow_ui_url" {
  description = "URL for the Airflow web UI"
  value       = "http://${aws_eip.airflow.public_ip}:8080"
}
