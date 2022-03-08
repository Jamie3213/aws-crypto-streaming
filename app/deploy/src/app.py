from aws_cdk import App, Stack
from aws_cdk import aws_kinesisfirehose as firehose
from constructs import Construct


class CryptoStreamingStack(Stack):
    def __init__(self, scope: Construct, id: str) -> None:
        super().__init__(scope, id)
        

if __name__ == "__main__":
    app = App()
