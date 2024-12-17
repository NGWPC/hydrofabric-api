terraform {
  backend "s3" {
    bucket         = "ngwpc-infra-oe"
    key            = "terraform/hydrofabric_api/oe/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true  # Encrypt the state file
    #dynamodb_table = "dynamodb-lock-table"  # Optional / FUTURE for state locking
  }
}
