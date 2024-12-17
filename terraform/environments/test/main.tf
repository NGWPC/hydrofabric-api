provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "HydroFabric"
      ManagedBy   = "Terraform"
      Environment = var.environment
    }
  }
}

module "hydro_api" {
  source = "../../modules/hydro_api"
  aws_region            = var.aws_region
  environment           = var.environment
  additional_vpc_cidrs  = var.additional_vpc_cidrs
  api_name              = var.api_name
  hosted_zone_id        = var.hosted_zone_id
  ami_id                = var.ami_id
  instance_type         = var.instance_type
  root_volume_type      = var.root_volume_type
  root_volume_size      = var.root_volume_size
  is_test_env           = true
  secrets_manager_arn   = var.secrets_manager_arn
  registry_secret_arn   = var.registry_secret_arn
  registry_url          = var.registry_url
  db_host               = var.db_host
  db_name               = var.db_name
  db_port               = var.db_port
  vpc_name              = var.vpc_name
  subnet_name_pattern   = var.subnet_name_pattern
  docker_image          = var.docker_image
  certificate_arn       = var.certificate_arn
  log_retention_days    = var.log_retention_days
  container_port        = var.container_port
  sns_alert_topic_arn   = var.sns_alert_topic_arn
  hydro_s3_bucket       = var.hydro_s3_bucket
  session_manager_logging_policy_arn = var.session_manager_logging_policy_arn
  enable_deletion_protection = var.enable_deletion_protection
  deployment_timestamp  = var.deployment_timestamp
}
