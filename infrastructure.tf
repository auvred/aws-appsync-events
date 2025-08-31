terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

variable "aws_region" {
  type = string
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      managed-by = "terraform"
      project = "aws-appsync-events"
    }
  }
}

resource "aws_appsync_api" "api" {
  name = "aws-appsync-client-tests-infra"

  event_config {
    auth_provider {
      auth_type = "API_KEY"
    }

    connection_auth_mode {
      auth_type = "API_KEY"
    }

    default_publish_auth_mode {
      auth_type = "API_KEY"
    }

    default_subscribe_auth_mode {
      auth_type = "API_KEY"
    }
  }
}

resource "aws_appsync_api_key" "key" {
  api_id = aws_appsync_api.api.api_id
}

resource "aws_appsync_channel_namespace" "default_channel" {
  api_id = aws_appsync_api.api.api_id
  name = "default"
}

output "api_endpoint" {
  value = aws_appsync_api.api.dns.HTTP
}

output "api_key" {
  value = aws_appsync_api_key.key.key
  sensitive = true
}
