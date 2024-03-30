import boto3

from scalable_python_lambda_sdk.scalable.execption.infrastructure_error_execption import InfrastructureError

class S3Connector:
    # TODO: Get configuration from a centralised placed (company wide configuration)
    _client = boto3.client("s3")

    @staticmethod
    def get_file_contents_as_stream(bucket: str, key: str):
        try:
            response = S3Connector._client.get_object(Bucket=bucket, Key=key)
        except Exception as exc:
            raise InfrastructureError(
                "s3", f"Error while trying to read s3://{bucket}/{key}: {exc}"
            )

        return response["Body"]
