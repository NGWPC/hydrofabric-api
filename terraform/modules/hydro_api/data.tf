# modules/hydro_api/data.tf

# AWS Account
data "aws_caller_identity" "current" {}

# ALB Service Account IDs for S3 Bucket Policy Permissions
variable "alb_service_account_ids" {
  default = {
    "us-east-1"      = "127311923021"
    "us-east-2"      = "033677994240"
    "us-west-1"      = "027434742980"
    "us-west-2"      = "797873946194"
    "us-gov-east-1"  = "190560391635"
    "us-gov-west-1"  = "048591011584"
  }
}

# VPC
data "aws_vpc" "main" {
  tags = {
    Name = var.vpc_name
  }
}

# Subnets
data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.main.id]
  }

  filter {
    name   = "tag:Name"
    values = [var.subnet_name_pattern]
  }
}

# AMI
data "aws_ami" "amazon_linux_2" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["amzn2-ami-hvm-*-x86_64-gp2"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }

  filter {
    name   = "state"
    values = ["available"]
  }
}
