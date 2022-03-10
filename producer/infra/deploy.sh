#!/bin/bash
echo Setting environment variables...
GIT_COMMIT_HASH=$(git rev-parse --short HEAD)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region)
ECR_URI=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
ECR_REPO_NAME=ecr-jamie-crypto-firehose-producer

echo Building Docker image...
docker build --platform linux/arm64 -t firehose-producer:latest ../app

echo Authenticating Docker to ECR registry...
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $ECR_URI

echo Tagging Docker image with ECR registry...
IMAGE_ID=$(docker images firehose-producer:latest --quiet)
docker tag $IMAGE_ID $ECR_URI/$ECR_REPO_NAME:latest

echo Pushing Docker image to ECR...
docker push $ECR_URI/$ECR_REPO_NAME:latest
docker tag $IMAGE_ID $ECR_URI/$ECR_REPO_NAME:$GIT_COMMIT_HASH
docker push $ECR_URI/$ECR_REPO_NAME:$GIT_COMMIT_HASH

echo Synthesizing and deploying app infrastructure...
cdk synth --app "python ./src/app.py" --output ./src/cdk.out
cdk deploy --app "python ./src/app.py" --require-approval never

# echo Restarting Fargate task...

