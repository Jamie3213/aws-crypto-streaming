import os

import yaml
from aws_cdk import App, Duration, Environment, RemovalPolicy, Size, Stack
from aws_cdk import aws_lambda as _lambda
from constructs import Construct

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

variables = config["Variables"]
project = variables["Project"]
org = variables["Org"]


class TradeRecordProcessingStack(Stack):
    """
    Defines a CloudFormation stack which uses a Lambda function to process record batches
    written from Kinesis Data Firehose and write them to a DynamoDB table.
    """

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        lambda_function = _lambda.Function(
            self,
            "LambdaFunction",
            function_name=f"lambda-{org}-{project}-trade-processor",
            code=_lambda.Code.from_asset("../../app/lambda.zip"),
            handler="app.lambda_handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            memory_size=128,
            timeout=Duration.seconds(30),
        )


if __name__ == "__main__":
    app = App()

    aws_account = os.environ["CDK_DEFAULT_ACCOUNT"]
    aws_region = os.environ["CDK_DEFAULT_REGION"]
    environment = Environment(account=aws_account, region=aws_region)
    TradeRecordProcessingStack(
        app, f"stack-{org}-{project}-trade-processor", env=environment
    )

    app.synth()
