import os

import yaml
from aws_cdk import App, Duration, Environment, RemovalPolicy, Stack
from aws_cdk import aws_dynamodb as dynamodb
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_s3_notifications as s3n
from constructs import Construct

with open("config.yml", "r") as file:
    config = yaml.safe_load(file)

variables = config["Variables"]
project = variables["Project"]
org = variables["Org"]

resources = config["Resources"]
table_name = resources["DynamoDbTableName"]
s3_source_name = resources["S3Source"]


class TradeRecordProcessingStack(Stack):
    """
    Defines a CloudFormation stack which uses a Lambda function to process record batches
    written from Kinesis Data Firehose and write them to a DynamoDB table.
    """

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        env = kwargs["env"]

        s3_source = s3.Bucket.from_bucket_name(self, "S3DataLake", s3_source_name)

        dynamodb_table = dynamodb.Table(
            self,
            "DynamoDbTable",
            table_name=table_name,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            partition_key=dynamodb.Attribute(
                name="Ticker", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="ProducerProcessedAt", type=dynamodb.AttributeType.NUMBER
            ),
        )

        dynamodb_table.add_local_secondary_index(
            sort_key=dynamodb.Attribute(
                name="ExecutedAt", type=dynamodb.AttributeType.NUMBER
            ),
            index_name="ExecutedAt",
        )

        dynamodb_table.apply_removal_policy(RemovalPolicy.DESTROY)

        lambda_iam_role = iam.Role(
            self,
            "LambdaProcessorIamRole",
            role_name=f"iam-{env.region}-{org}-{project}-lambda-trade-processor",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
        )

        lambda_iam_policy = iam.Policy(
            self,
            "FirehoseProducerIamPolicy",
            policy_name=f"policy-{org}-{project}-lambda-trade-processor",
            statements=[
                iam.PolicyStatement(
                    sid="LambdaS3Read",
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject"],
                    resources=[f"{s3_source.bucket_arn}/bronze/{project}/*"],
                ),
                iam.PolicyStatement(
                    sid="LambdaDynamoDbWrite",
                    effect=iam.Effect.ALLOW,
                    actions=["dynamodb:BatchWriteItem"],
                    resources=[f"{dynamodb_table.table_arn}"],
                ),
            ],
            roles=[lambda_iam_role],
        )

        lambda_function = _lambda.Function(
            self,
            "LambdaFunction",
            function_name=f"lambda-{org}-{project}-trade-processor",
            code=_lambda.Code.from_asset("../../app/lambda.zip"),
            handler="app.lambda_handler",
            runtime=_lambda.Runtime.PYTHON_3_9,
            architecture=_lambda.Architecture.ARM_64,
            memory_size=128,
            environment={"DYNAMODB_TABLE_NAME": table_name},
            role=lambda_iam_role,
            timeout=Duration.minutes(5),
        )

        s3_source.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(lambda_function),
            s3.NotificationKeyFilter(prefix=f"bronze/{project}"),
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
