terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
  }

  backend "s3" {
    bucket  = "s3-jamie-general-config"
    key     = "crypto/infra/terraform.state"
    region  = "eu-west-1"
    encrypt = true
  }
}

/* ----------------------------- Input variables ---------------------------- */

variable "project" {
  type        = string
  description = "An abbreviation for the project the resources relate to."

  validation {
    condition     = can(regex("[a-z]*", var.project))
    error_message = "Project abbreviation must be lower case letters only."
  }
}

variable "created_by" {
  type        = string
  description = "The name of the user who created the resource."
}

variable "data_lake" {
  type        = string
  description = "The name of the S3 bucket which serves as the data lake."
}

/* -------------------------------- Providers ------------------------------- */

provider "aws" {
  region  = "eu-west-1"

  default_tags { 
    tags = {
      Project     = var.project,
      CreatedBy   = var.created_by
    }
  }
}

/* ------------------------------ Data sources ------------------------------ */

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

data "aws_s3_bucket" "data_lake" {
    bucket = var.data_lake
}

/* -------------------------------- Resources ------------------------------- */

resource "aws_cloudwatch_log_group" "log_group" {
  name = "/aws/jamie/${var.project}"
}

resource "aws_ecs_cluster" "cluster" {
  name               = "ecs-jamie-${var.project}-fargate"
  capacity_providers = ["FARGATE"]

  configuration {
    execute_command_configuration {
      logging = "OVERRIDE"

      log_configuration {
        cloud_watch_encryption_enabled = true
        cloud_watch_log_group_name     = aws_cloudwatch_log_group.log_group.name
      }
    }
  }

  setting {
    name  = "containerInsights"
    value = "enabled"
  }
}

resource "aws_ecr_repository" "repo" {
  name                 = "ecr-jamie-${var.project}-firehose-producer"
  image_tag_mutability = "MUTABLE"

  encryption_configuration {
    encryption_type = "AES256"
  }
}

resource "aws_ecr_lifecycle_policy" "repo_policy" {
  repository = aws_ecr_repository.repo.name

  policy = <<EOF
{
  "rules": [
    {
      "rulePriority": 1,
      "description": "Expires untagged containers after 1 day.",
      "selection": {
        "tagStatus": "untagged",
        "countType": "sinceImagePushed",
        "countUnit": "days",
        "countNumber": 1
      },
      "action": {
        "type": "expire"
      }
    }
  ]
}
EOF
}

resource "aws_iam_role" "firehose_iam_role" {
  name               = "iam-${data.aws_region.current.name}-jamie-${var.project}-firehose"
  assume_role_policy = file("policies/firehose_assume_role.json")
}

resource "aws_iam_role_policy" "firehose_iam_policy" {
  name = "policy-jamie-${var.project}-firehose"
  role = aws_iam_role.firehose_iam_role.id

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "FirehoseCreateAndWriteLogStreams",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "${aws_cloudwatch_log_group.log_group.arn}:*"
    },
    {
      "Sid": "FirehoseS3Access",
      "Effect": "Allow",
      "Action": [
        "s3:AbortMultipartUpload",
        "s3:GetBucketLocation",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:ListBucketMultipartUploads",
        "s3:PutObject"
      ],
      "Resource": [
        "${data.aws_s3_bucket.data_lake.arn}",
        "${data.aws_s3_bucket.data_lake.arn}/*"
      ]
    }
  ]
}
POLICY
}

resource "aws_kinesis_firehose_delivery_stream" "firehose" {
  name        = "firehose-jamie-${var.project}-stream"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_iam_role.arn
    bucket_arn         = data.aws_s3_bucket.data_lake.arn
    buffer_interval    = 60
    buffer_size        = 5
    compression_format = "UNCOMPRESSED"
    prefix             = "bronze/${var.project}/"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.log_group.name
      log_stream_name = "firehose"
    }
  }
}
