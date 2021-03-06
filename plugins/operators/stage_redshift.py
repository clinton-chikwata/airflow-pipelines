#from airflow.typing_extensions import TypeVarTuple
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="us-west-2",
                 extra_params="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.extra_params = extra_params

    def execute(self, context):
        """
        Stage data from S3 to a given Redshift table.
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"

        self.log.info("Copying data from S3 to Redshift staging table.")

        copy_sql = f"""COPY {self.table} FROM '{s3_path}' 
        ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY '{credentials.secret_key}' 
        REGION AS '{self.region}' 
        {self.extra_params};"""

        self.log.info(f"Executing query to copy data from '{s3_path}' to '{self.table}'")
        redshift.run(copy_sql)
