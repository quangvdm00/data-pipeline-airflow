import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql_time = """
        COPY {} 
        FROM '{}/{}/{}/'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        JSON '{}';
    """

    copy_sql = """
        COPY {} 
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        JSON '{}';
    """

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                aws_credentials_id="",
                table="",
                s3_path="",
                json_path="",
                region="us-west-2",
                *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.json_path = json_path
        self.region=region
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("---- Clearing data from destination Redshift table ----")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("---- Copying data from Stage to Redshift ----")
        # rendered_key = self.s3_key.format(**context)
        # s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # formatted_sql = StageToRedshiftOperator.copy_sql.format(
        #     self.table,
        #     s3_path,
        #     credentials.access_key,
        #     credentials.secret_key,
        #     self.region,
        #     self.extra_params
        # )

        if self.execution_date:
            formatted_sql = StageToRedshiftOperator.copy_sql_time.format(
                self.table,
                self.s3_path,
                self.execution_date.strftime("%Y"),
                self.execution_date.strftime("%d"),
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.json_path,
                self.execution_date
            )
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.json_path,
                self.execution_date
            )
        logging.info(f"Executing query to copy data from '{self.s3_path}' to '{self.table}'")
        redshift.run(formatted_sql)