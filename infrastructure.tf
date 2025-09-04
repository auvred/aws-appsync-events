terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.9"
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

    auth_provider {
      auth_type = "AWS_IAM"
    }

    connection_auth_mode {
      auth_type = "API_KEY"
    }

    connection_auth_mode {
      auth_type = "AWS_IAM"
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

resource "aws_appsync_channel_namespace" "iam_auth_channel" {
  api_id = aws_appsync_api.api.api_id
  name = "iam-auth"

  publish_auth_mode {
    auth_type = "AWS_IAM"
  }
  subscribe_auth_mode {
    auth_type = "AWS_IAM"
  }
}

resource "aws_iam_user" "user" {
  name = "aws-appsync-events-test-user"
  force_destroy = true
}

resource "aws_iam_policy" "user_policy" {
  policy = jsonencode({
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": [
          "appsync:EventConnect"
        ],
        "Resource": [
          aws_appsync_api.api.api_arn
        ]
      },
      {
        "Effect": "Allow",
        "Action": [
          "appsync:EventPublish",
          "appsync:EventSubscribe"
        ],
        "Resource": [
          "${aws_appsync_api.api.api_arn}/channelNamespace/iam-auth"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "user_policy_attachment" {
  user = aws_iam_user.user.name
  policy_arn = aws_iam_policy.user_policy.arn
}

resource "aws_iam_access_key" "iam_key" {
  user = aws_iam_user.user.name
}

output "api_endpoint" {
  value = aws_appsync_api.api.dns.HTTP
}

output "api_key" {
  value = aws_appsync_api_key.key.key
  sensitive = true
}

output "access_key_id" {
  value = aws_iam_access_key.iam_key.id
}

output "secret_access_key" {
  value = aws_iam_access_key.iam_key.secret
  sensitive = true
}

output "region" {
  value = var.aws_region
}
