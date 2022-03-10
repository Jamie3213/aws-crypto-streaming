from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from aws_cdk import aws_kinesisfirehose as firehose
from aws_cdk.aws_logs import LogGroup
from aws_cdk.aws_s3 import Bucket
from constructs import Construct


class FirehoseStack(Stack):
    def __init__(
        self,
        scope: Construct,
        id: str,
        org: str,
        project: str,
        log_group_name: str,
        s3_destination_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        env = kwargs["env"]

        log_group = LogGroup.from_log_group_name(self, "LogGroup", log_group_name)
        s3_destination = Bucket.from_bucket_name(self, "S3DataLake", s3_destination_name)

        firehose_iam_role = iam.Role(
            self,
            "FirehoseProducerIamRole",
            role_name=f"iam-{env.region}-{org}-{project}-firehose-producer",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        firehose_iam_policy = iam.Policy(
            self,
            "FirehoseProducerIamPolicy",
            policy_name=f"policy-{org}-{project}-firehose-producer",
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
                    resources=[
                        s3_destination.bucket_arn,
                        f"{s3_destination.bucket_arn}:/*",
                    ],
                ),
            ],
            roles=[firehose_iam_role],
        )

        self.firehose_delivery_stream = firehose.CfnDeliveryStream(
            self,
            "FirehoseDeliveryStream",
            delivery_stream_name=f"firehose-{org}-{project}-stream",
            delivery_stream_type="DirectPut",
            extended_s3_destination_configuration=firehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty(
                bucket_arn=s3_destination.bucket_arn,
                role_arn=firehose_iam_role.role_arn,
                compression_format="UNCOMPRESSED",
                buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    interval_in_seconds=60, size_in_m_bs=5
                ),
                prefix=f"bronze/{project}/",
                cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                    enabled=True,
                    log_group_name=log_group.log_group_name,
                    log_stream_name="firehose",
                ),
            ),
        )
