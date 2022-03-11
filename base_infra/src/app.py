import os

import yaml
from aws_cdk import App, Duration, Environment, RemovalPolicy, Stack, Tags
from aws_cdk import aws_ecr as ecr
from aws_cdk import aws_logs as logs
from constructs import Construct

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

project_abrv = config["Variables"]["ProjectAbrv"]


class BaseStack(Stack):
    """Defines a CloudFormation stack for the Tiingo crypto streaming application."""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        log_group = logs.LogGroup(
            self,
            "LogGroup",
            log_group_name=f"/aws/jamie/{project_abrv}",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        image_repo = ecr.Repository(
            self,
            "FirehoseProducerEcrRepo",
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


if __name__ == "__main__":
    app = App()

    aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
    aws_region = os.environ["CDK_DEFAULT_REGION"]
    environment = Environment(account=aws_account, region=aws_region)
    BaseStack(app, f"stack-jamie-{project_abrv}-base", env=environment)

    Tags.of(app).add("Project", project_abrv)
    Tags.of(app).add("CreatedBy", "jamie")

    app.synth()
