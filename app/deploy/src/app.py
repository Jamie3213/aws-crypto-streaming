import os

import yaml
from aws_cdk import App, Environment, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesisfirehose_alpha as firehose
from aws_cdk import aws_kinesisfirehose_destinations as destination
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk.core import Duration, RemovalPolicy, Size
from constructs import Construct

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

project_abrv = config["Variables"]["ProjectAbrv"]
s3_data_lake_name = config["Resources"]["S3DataLake"]


class CryptoStreamingStack(Stack):
    """Defines a CloudFormation stack for the Tiingo crypto streaming application."""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # External AWS resources
        default_vpc = ec2.Vpc.from_lookup(self, id="Vpc", is_default=True)
        s3_data_lake = s3.Bucket.from_bucket_name(
            self, id="S3DataLake", bucket_name=s3_data_lake_name
        )

        # Stack resources
        log_group = logs.LogGroup(
            self,
            id="LogGroup",
            log_group_name=f"/aws/jamie/{project_abrv}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        fargate_cluster = ecs.Cluster(
            self,
            id="EcsFargateCluster",
            cluster_name=f"ecs-jamie-{project_abrv}-fargate",
            enable_fargate_capacity_providers=True,
            container_insights=True,
            execute_command_configuration=ecs.ExecuteCommandConfiguration(
                log_configuration=ecs.ExecuteCommandLogConfiguration(
                    cloud_watch_encryption_enabled=True, cloud_watch_log_group=log_group
                ),
                logging=ecs.ExecuteCommandLogging.OVERRIDE,
            ),
            vpc=default_vpc,
        )

        image_repo = ecr.Repository(
            self,
            id=f"FirehoseProducerEcrRepo",
            repository_name=f"ecr-jamie-{project_abrv}-firehose-producer",
            removal_policy=RemovalPolicy.DESTROY,
            lifecycle_rules=[
                ecr.LifecycleRule(
                    description="Expires untagged containers after 1 day.",
                    max_image_age=Duration.days(1),
                    tag_status=ecr.TagStatus.UNTAGGED,
                )
            ],
        )

        firehose_iam_role = iam.Role(
            self,
            id="FirehoseProducerIamRole",
            role_name=f"iam-{kwargs['env'].region}-firehose-producer",
            path=f"/aws/jamie/{project_abrv}",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        firehose_iam_policy = iam.Policy(
            self,
            id="FirehoseProducerIamPolicy",
            policy_name=f"policy-jamie-{project_abrv}-firehose-producer",
            statements=[
                iam.PolicyStatement(
                    sid="FirehoseCreateAndWriteLogStreams",
                    effect=iam.Effect.ALLOW,
                    actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                    resources=[f"{log_group.log_group_arn}:*"],
                ),
                iam.PolicyStatement(
                    sid="FirehoseS3Access",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:AbortMultipartUpload",
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListBucketMultipartUploads",
                        "s3:PutObject",
                    ],
                    resources=[s3_data_lake.bucket_arn, f"{s3_data_lake.bucket_arn}/*"],
                ),
            ],
            roles=[firehose_iam_role],
        )

        firehose_s3_destination = destination.S3Bucket(
            bucket=s3_data_lake,
            buffering_interval=Duration.seconds(60),
            buffering_size=Size.mebibytes(5),
        )

        firehose_delivery_stream = firehose.DeliveryStream(
            self,
            id="FirehoseDeliveryStream",
            delivery_stream_name=f"firehose-jamie-{project_abrv}-stream",
            destinations=[firehose_s3_destination],
            role=firehose_iam_role,
        )


if __name__ == "__main__":
    app = App()

    aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
    aws_region = os.environ["CDK_DEFAULT_REGION"]
    environment = Environment(account=aws_account, region=aws_region)
    CryptoStreamingStack(app, f"stack-jamie-{project_abrv}", env=environment)

    app.synth()
