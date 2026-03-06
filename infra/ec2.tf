# -------------------------
# EC2 — Airflow instance
# -------------------------

# Latest Amazon Linux 2023 AMI
data "aws_ami" "amazon_linux_2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "airflow" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.ec2_instance_type
  key_name               = var.ec2_key_pair_name
  subnet_id              = aws_subnet.public_a.id
  vpc_security_group_ids = [aws_security_group.airflow_ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2_airflow_profile.name

  root_block_device {
    volume_size = 20    # GB
    volume_type = "gp3"
    encrypted   = true
  }

  # Bootstrap script — installs Docker and Docker Compose on first boot
  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update system
    dnf update -y

    # Install Docker
    dnf install -y docker
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user

    # Install Docker Compose plugin
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # Install git
    dnf install -y git
  EOF

  tags = {
    Name = "${var.project}-airflow"
  }
}

# Elastic IP — gives your EC2 instance a stable public IP
resource "aws_eip" "airflow" {
  instance = aws_instance.airflow.id
  domain   = "vpc"

  tags = {
    Name = "${var.project}-airflow-eip"
  }
}
