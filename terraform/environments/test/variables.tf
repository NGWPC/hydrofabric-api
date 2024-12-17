variable "environment" {
  description = "Environment name"
  type        = string
  default     = "test"

  validation {
    condition     = var.environment == "test"
    error_message = "For test environment, environment must be 'test'."
  }
}

variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"

  validation {
    condition     = can(regex("^[a-z]{2}-[a-z]+-\\d{1}$", var.aws_region))
    error_message = "Must be a valid AWS region format (e.g., us-east-1)."
  }
}

variable "api_name" {
  description = "API name"
  type        = string
  default     = "hydro-api"

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.api_name))
    error_message = "API name must contain only lowercase letters, numbers, and hyphens."
  }
}

variable "hydro_s3_bucket" {
  description = "S3 location of Hydro data / files for reading and writing"
  type        = string
  default     = "ngwpc-hydrofabric"
}

variable "hosted_zone_id" {
  description = "Route53 hosted zone ID"
  type        = string

  validation {
    condition     = can(regex("^Z[A-Z0-9]+$", var.hosted_zone_id))
    error_message = "Must be a valid Route53 hosted zone ID."
  }
}

variable "enable_deletion_protection" {
  description = "Enable deletion protection for ALB"
  type        = bool
  default     = true
}

variable "ami_id" {
  description = "AMI ID for EC2 instances. If not provided, latest Amazon Linux 2 AMI will be used."
  type        = string
  default     = null # This allows the variable to be optional

  validation {
    condition     = var.ami_id == null || var.ami_id == "" || can(regex("^ami-[a-f0-9]{17}$", var.ami_id))
    error_message = "If provided, AMI ID must be valid (e.g., ami-123456789abcdef01)."
  }
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "t3.micro"

  validation {
    condition     = can(regex("^[a-z][1-6a-z]\\.[\\w]+$", var.instance_type))
    error_message = "Must be a valid EC2 instance type."
  }
}

variable "root_volume_type" {
  description = "Type of root volume (gp2, gp3, io1, etc.)"
  type        = string
  default     = "gp3"

  validation {
    condition     = contains(["gp2", "gp3", "io1", "io2"], var.root_volume_type)
    error_message = "Root volume type must be one of: gp2, gp3, io1, io2."
  }
}

variable "root_volume_size" {
  description = "Size of root volume in GB"
  type        = number
  default     = 100

  validation {
    condition     = var.root_volume_size >= 20 && var.root_volume_size <= 100
    error_message = "Root volume size must be between 20 and 100 GB."
  }
}

variable "vpc_name" {
  description = "Name of the VPC"
  type        = string
  default     = "main"
}

variable "subnet_name_pattern" {
  description = "Pattern to match for target subnets in the VPC"
  type        = string
  default     = "App*"
}

variable "secrets_manager_arn" {
  description = "ARN of the Secrets Manager secret containing RDS credentials"
  type        = string

  validation {
    condition     = can(regex("^arn:aws:secretsmanager:[a-z0-9-]+:\\d{12}:secret:.+", var.secrets_manager_arn))
    error_message = "Must be a valid Secrets Manager ARN."
  }
}

variable "db_host" {
  description = "Database host endpoint"
  type        = string

  validation {
    condition     = can(regex("(^[a-zA-Z0-9.-]+\\.amazonaws\\.com$)|(^[0-9]{1,3}(\\.[0-9]{1,3}){3}$)", var.db_host))
    error_message = "Must be a valid AWS endpoint or an IP address."
  }
}

variable "db_name" {
  description = "Database name"
  type        = string
  default     = "hydro_test"

  validation {
    condition     = can(regex("^[a-zA-Z][a-zA-Z0-9_]*$", var.db_name))
    error_message = "Database name must start with a letter and contain only alphanumeric characters and underscores."
  }
}

variable "db_port" {
  description = "Database port"
  type        = number
  default     = 5432

  validation {
    condition     = var.db_port > 0 && var.db_port < 65536
    error_message = "Database port must be between 1 and 65535."
  }
}

variable "log_retention_days" {
  description = "CloudWatch logs retention period in days"
  type        = number
  default     = 7

  validation {
    condition     = contains([1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1827, 3653], var.log_retention_days)
    error_message = "Must be a valid CloudWatch logs retention period."
  }
}

variable "container_port" {
  description = "Port that the container listens on"
  type        = number
  default     = 8000

  validation {
    condition     = var.container_port > 0 && var.container_port < 65536
    error_message = "Container port must be between 1 and 65535."
  }
}

variable "registry_url" {
  description = "URL of the GitLab registry (e.g., registry.gitlab.com)"
  type        = string
}

variable "registry_secret_arn" {
  description = "ARN of the Secrets Manager secret containing GitLab registry credentials"
  type        = string
}

variable "docker_image" {
  description = "Docker image to run (including registry path and tag)"
  type        = string

  validation {
    condition     = can(regex("^registry\\.[a-zA-Z0-9.-]+\\.[a-zA-Z]+/[\\w-]+(/[\\w-]+)+:[\\w.-]+$", var.docker_image))
    error_message = "Must be a valid registry image URL with tag (e.g., 'registry.sh.nextgenwaterprediction.com/ngwpc/hydrofabric-group/hydrofabric_api:tag')."
  }
}

variable "certificate_arn" {
  description = "ARN of ACM certificate for HTTPS. Optional for test environment."
  type        = string
  default     = null

  validation {
    condition     = var.certificate_arn == null || can(regex("^arn:aws:acm:[a-z0-9-]+:\\d{12}:certificate/[a-zA-Z0-9-]+$", var.certificate_arn))
    error_message = "If provided, must be a valid ACM certificate ARN."
  }
}

variable "sns_alert_topic_arn" {
  description = "SNS topic ARN for CloudWatch alarms. Optional for test environment."
  type        = string
  default     = null

  validation {
    condition     = var.sns_alert_topic_arn == null || can(regex("^arn:aws:sns:[a-z0-9-]+:\\d{12}:[a-zA-Z0-9-_]+$", var.sns_alert_topic_arn))
    error_message = "If provided, must be a valid SNS topic ARN."
  }
}

variable "additional_vpc_cidrs" {
  description = "List of additional VPC CIDR blocks that should have access to the instance in test environment"
  type        = list(string)
  default     = []

  validation {
    condition     = alltrue([for cidr in var.additional_vpc_cidrs : can(regex("^([0-9]{1,3}\\.){3}[0-9]{1,3}/[0-9]{1,2}$", cidr))])
    error_message = "All CIDR blocks must be valid IPv4 CIDR notation (e.g., '10.0.0.0/16')."
  }
}

variable "session_manager_logging_policy_arn" {
  description = "ARN of the Session Manager logging policy"
  type        = string
  default     = "arn:aws:iam::591210920133:policy/AWSAccelerator-SessionManagerLogging"
}

variable "deployment_timestamp" {
  description = "Timestamp to force redeployment of the container (format: YYYYMMDDHHMMSS)"
  type        = string
  default     = null
}
