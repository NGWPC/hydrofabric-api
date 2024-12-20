environment         = "oe"
aws_region         = "us-east-1"
api_name           = "hydro-api"
hosted_zone_id     = "Z097759625XX7EFGSV4L0"  # Different zone ID for OE
# ami_id              = "ami-xxxxxxxxxxxxx" # Optional: The most recent AL2 will be used by default
instance_type      = "t3.medium"  # Might want larger instances in OE
root_volume_type   = "gp3"
root_volume_size   = 100
vpc_name           = "optworkloads"
subnet_name_pattern = "OE-App*"
db_host            = "oe-hydrofabric.cudlpusnia2m.us-east-1.rds.amazonaws.com"
db_name            = "hydrofabric_db"
db_port            = 5432
session_manager_logging_policy_arn = "arn:aws:iam::154735606025:policy/AWSAccelerator-SessionManagerLogging"
secrets_manager_arn = "arn:aws:secretsmanager:us-east-1:154735606025:secret:oe-hydroapi-rds-db-credentials-E7yLQj"
certificate_arn    = "arn:aws:acm:us-east-1:154735606025:certificate/0ed4f6d9-2dd5-48b5-a347-9245481a6e55"
log_retention_days = 30  # Longer retention in OE
container_port     = 8000
#sns_alert_topic_arn = "arn:aws:sns:us-east-1:xxxxxxxxxxxx:hydro-api-alerts-oe"
registry_url = "registry.sh.nextgenwaterprediction.com"
registry_secret_arn = "arn:aws:secretsmanager:us-east-1:154735606025:secret:hydroapi-gitlab-registry-credentials-KGYKvn"
docker_image = "registry.sh.nextgenwaterprediction.com/ngwpc/hydrofabric-group/hydrofabric_api:1.0.1"
hydro_s3_bucket     = "ngwpc-hydrofabric-oe"
additional_vpc_cidrs = ["10.105.0.0/16","10.10.0.0/22", "10.6.0.0/22", "10.203.0.0/16"]
enable_deletion_protection = false

# Update this when you want to force a redeployment and don't have a new docker image tag to target.
# Consider versions or git hashes or something else in the future.
deployment_timestamp = "2024-12-20_17:00:00"


