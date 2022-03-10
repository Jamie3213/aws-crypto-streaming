import os
from datetime import datetime

import yaml
from aws_cdk import App, Environment, Stack, Tags
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_ecs as ecs
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_secretsmanager as secretsmanager
from constructs import Construct

current_directory = os.path.dirname(__file__)
config_file = os.path.join(current_directory, "config.yml")

with open(config_file, "r") as file:
    config = yaml.safe_load(file)

project_abrv = config["Variables"]["ProjectAbrv"]
s3_data_lake_name = config["Resources"]["S3DataLake"]
log_group_name = config["Resources"]["LogGroup"]
ecs_cluster_name = config["Resources"]["EcsCluster"]
ecr_repo_name = config["Resources"]["EcrRepo"]
secret_name = config["Resources"]["SecretsManagerSecret"]

aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
aws_region = os.environ["CDK_DEFAULT_REGION"]


class CryptoStreamingStack(Stack):
    """Defines a CloudFormation stack for the Tiingo crypto streaming application."""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        default_vpc = ec2.Vpc.from_lookup(self, "Vpc", is_default=True)
        s3_data_lake = s3.Bucket.from_bucket_name(self, "S3DataLake", s3_data_lake_name)
        log_group = logs.LogGroup.from_log_group_name(self, "LogGroup", log_group_name)
        ecr_repo = ecr.Repository.from_repository_name(self, "EcrRepo", ecr_repo_name)

        security_group = ec2.SecurityGroup(self, "SecurityGroup", vpc=default_vpc)
        security_group.connections.allow_internally(port_range=ec2.Port.all_traffic())

        ecs_cluster = ecs.Cluster.from_cluster_attributes(
            self,
            "EcsCluster",
            cluster_name=ecs_cluster_name,
            security_groups=[security_group],
            vpc=default_vpc,
        )

        secrets_manager_secret = secretsmanager.Secret.from_secret_name_v2(
            self, "SecretsManagerSecret", secret_name
        )

        firehose_iam_role = iam.Role(
            self,
            "FirehoseProducerIamRole",
            role_name=f"iam-{kwargs['env'].region}-{project_abrv}-firehose-producer",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        firehose_iam_policy = iam.Policy(
            self,
            "FirehoseProducerIamPolicy",
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

        firehose_delivery_stream = firehose.CfnDeliveryStream(
            self,
            "FirehoseDeliveryStream",
            delivery_stream_name=f"firehose-jamie-{project_abrv}-stream",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=s3_data_lake.bucket_arn,
                role_arn=firehose_iam_role.role_arn,
                compression_format="UNCOMPRESSED",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60, size_in_m_bs=5
                ),
                prefix=f"bronze/{project_abrv}/",
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=log_group.log_group_name,
                    log_stream_name="firehose",
                ),
            ),
        )

        fargate_iam_execution_role = iam.Role(
            self,
            "FargateExecutionIamRole",
            role_name=f"iam-{kwargs['env'].region}-{project_abrv}-fargate-execution",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        fargate_iam_execution_policy = iam.Policy(
            self,
            "FargateExecutionIamPolicy",
            policy_name=f"policy-jamie-{project_abrv}-fargate-execution",
            statements=[
                iam.PolicyStatement(
                    sid="FargateCreateAndWriteLogStreams",
                    effect=iam.Effect.ALLOW,
                    actions=["logs:CreateLogStream", "logs:PutLogEvents"],
                    resources=[f"{log_group.log_group_arn}:*"],
                ),
                iam.PolicyStatement(
                    sid="FargatePullEcrImage",
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer",
                        "ecr:BatchGetImage",
                    ],
                    resources=[ecr_repo.repository_arn],
                ),
                iam.PolicyStatement(
                    sid="FargatePullEcrImage",
                    effect=iam.Effect.ALLOW,
                    actions=["ecr:GetAuthorizationToken"],
                    resources=["*"],
                ),
            ],
            roles=[fargate_iam_execution_role],
        )

        fargate_iam_task_role = iam.Role(
            self,
            "FargateTaskIamRole",
            role_name=f"iam-{kwargs['env'].region}-{project_abrv}-fargate-task",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        fargate_iam_task_policy = iam.Policy(
            self,
            "FargateTaskIamPolicy",
            policy_name=f"policy-jamie-{project_abrv}-fargate-task",
            statements=[
                iam.PolicyStatement(
                    sid="FargateGetSecretsManagerSecret",
                    effect=iam.Effect.ALLOW,
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[secrets_manager_secret.secret_arn],
                ),
                iam.PolicyStatement(
                    sid="FargateWriteToFirehose",
                    effect=iam.Effect.ALLOW,
                    actions=["firehose:PutRecord"],
                    resources=[firehose_delivery_stream.attr_arn],
                ),
            ],
            roles=[fargate_iam_task_role],
        )

        fargate_task_definition = ecs.FargateTaskDefinition(
            self,
            "FargateTaskDefintion",
            cpu=512,
            memory_limit_mib=1024,
            runtime_platform=ecs.RuntimePlatform(
                cpu_architecture=ecs.CpuArchitecture.ARM64,
                operating_system_family=ecs.OperatingSystemFamily.LINUX,
            ),
            execution_role=fargate_iam_execution_role,
            task_role=fargate_iam_task_role,
        )

        fargate_task_definition.add_container(
            "FirehoseProducerContainer",
            image=ecs.ContainerImage.from_registry(
                ecr_repo.repository_uri_for_tag("latest")
            ),
            container_name="container-firehose-producer",
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="fargate", log_group=log_group
            ),
        )

        fargate_service = ecs.FargateService(
            self,
            "FargateService",
            service_name=f"service-jamie-{project_abrv}-firehose-producer",
            cluster=ecs_cluster,
            task_definition=fargate_task_definition,
            assign_public_ip=True,
            security_groups=[security_group],
            vpc_subnets=ec2.SubnetSelection(subnets=default_vpc.public_subnets),
            desired_count=1,
        )


if __name__ == "__main__":
    app = App()

    environment = Environment(account=aws_account, region=aws_region)
    CryptoStreamingStack(app, f"stack-jamie-{project_abrv}-firehose-producer", env=environment)

    Tags.of(app).add("Project", project_abrv)
    Tags.of(app).add("CreatedBy", "jamie")
    Tags.of(app).add("LastUpdated", datetime.strftime(datetime.utcnow(), "%Y-%m-%dT%H:%M:%S"))

    app.synth()
