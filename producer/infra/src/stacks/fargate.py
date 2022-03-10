from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_ecs as ecs
from aws_cdk.aws_ecr import Repository
from aws_cdk.aws_kinesisfirehose import CfnDeliveryStream
from aws_cdk.aws_logs import LogGroup
from aws_cdk.aws_secretsmanager import Secret
from constructs import Construct


class FargateStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        org: str,
        project: str,
        log_group_name: str,
        ecs_cluster_name: str,
        ecr_repo_name: str,
        secret_name: str,
        delivery_stream: CfnDeliveryStream,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        env = kwargs["env"]

        default_vpc = ec2.Vpc.from_lookup(self, "Vpc", is_default=True)
        log_group = LogGroup.from_log_group_name(self, "LogGroup", log_group_name)
        ecr_repo = Repository.from_repository_name(self, "EcrRepo", ecr_repo_name)
        secrets_manager_secret = Secret.from_secret_name_v2(self, "SecretsManagerSecret", secret_name)

        security_group = ec2.SecurityGroup(self, "SecurityGroup", vpc=default_vpc, allow_all_outbound=True)
        security_group.connections.allow_internally(port_range=ec2.Port.all_traffic())

        print(secrets_manager_secret.secret_arn)

        ecs_cluster = ecs.Cluster.from_cluster_attributes(
            self,
            "EcsCluster",
            cluster_name=ecs_cluster_name,
            security_groups=[security_group],
            vpc=default_vpc,
        )

        fargate_iam_execution_role = iam.Role(
            self,
            "FargateExecutionIamRole",
            role_name=f"iam-{kwargs['env'].region}-{org}-{project}-fargate-execution",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        fargate_iam_execution_policy = iam.Policy(
            self,
            "FargateExecutionIamPolicy",
            policy_name=f"policy-{org}-{project}-fargate-execution",
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
                    sid="FargateGetEcrAuthToken",
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
            role_name=f"iam-{env.region}-{org}-{project}-fargate-task",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )

        fargate_iam_task_policy = iam.Policy(
            self,
            "FargateTaskIamPolicy",
            policy_name=f"policy-{org}-{project}-fargate-task",
            statements=[
                iam.PolicyStatement(
                    sid="FargateGetSecretsManagerSecret",
                    effect=iam.Effect.ALLOW,
                    actions=["secretsmanager:GetSecretValue"],
                    resources=[secrets_manager_secret.secret_full_arn],
                ),
                iam.PolicyStatement(
                    sid="FargateWriteToFirehose",
                    effect=iam.Effect.ALLOW,
                    actions=["firehose:PutRecord"],
                    resources=[delivery_stream.attr_arn],
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

        fargate_container_definition = ecs.ContainerDefinition(
            self,
            "FargateContainerDefinition",
            task_definition=fargate_task_definition,
            image=ecs.ContainerImage.from_registry(
                ecr_repo.repository_uri_for_tag("latest")
            ),
            container_name="container-firehose-producer",
            secrets={"TIINGO_API_TOKEN": secrets_manager_secret},
            logging=ecs.LogDriver.aws_logs(
                stream_prefix="fargate", log_group=log_group
            ),
        )

        fargate_service = ecs.FargateService(
            self,
            "FargateService",
            service_name=f"service-{org}-{project}-firehose-producer",
            cluster=ecs_cluster,
            task_definition=fargate_task_definition,
            assign_public_ip=True,
            security_groups=[security_group],
            vpc_subnets=ec2.SubnetSelection(subnets=default_vpc.public_subnets),
            desired_count=1,
            min_healthy_percent=0,
            max_healthy_percent=100,
        )
