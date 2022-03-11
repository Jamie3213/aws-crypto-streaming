import os

import yaml
from aws_cdk import App, Environment, Tags
from stacks.fargate import FargateStack
from stacks.firehose import FirehoseStack

current_directory = os.path.dirname(__file__)
config_file = os.path.join(current_directory, "config.yml")

with open(config_file, "r") as file:
    config = yaml.safe_load(file)

project = config["Variables"]["Project"]
org = config["Variables"]["Org"]

resources = config["Resources"]
s3_destination_name = resources["S3Destination"]
log_group_name = resources["LogGroup"]
ecs_cluster_name = resources["EcsCluster"]
ecr_repo_name = resources["EcrRepo"]
secret_name = resources["SecretsManagerSecret"]

tiingo_api_url = config["Producer"]["TiingoApiUrl"]
firehose_batch_size = config["Producer"]["FirehoseBatchSize"]

aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
aws_region = os.environ["CDK_DEFAULT_REGION"]


if __name__ == "__main__":
    app = App()

    environment = Environment(account=aws_account, region=aws_region)
    stack_name_base = f"stack-{org}-{project}-producer"

    firehose_stack = FirehoseStack(
        app,
        f"{stack_name_base}-firehose",
        env=environment,
        org=org,
        project=project,
        log_group_name=log_group_name,
        s3_destination_name=s3_destination_name,
    )

    fargate_stack = FargateStack(
        app,
        f"{stack_name_base}-fargate",
        env=environment,
        org=org,
        project=project,
        log_group_name=log_group_name,
        ecs_cluster_name=ecs_cluster_name,
        ecr_repo_name=ecr_repo_name,
        secret_name=secret_name,
        delivery_stream=firehose_stack.firehose_delivery_stream,
        tiingo_api_url=tiingo_api_url,
        firehose_batch_size=firehose_batch_size,
    )

    Tags.of(app).add("Project", project)
    Tags.of(app).add("CreatedBy", "jamie")

    app.synth()
