from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator to copy data from S3 to Redshift staging tables.
    """
    ui_color = '#358140'
    template_fields = ('s3_key',)

    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        JSON '{json_path}'
        REGION 'us-east-1';
    """

    def __init__(
        self,
        *,
        redshift_conn_id: str,
        aws_conn_id: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        json_path: str = 'auto',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('Starting StageToRedshiftOperator: Copying data from S3 to Redshift')

        # Get AWS credentials
        aws_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear destination table
        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        # Build S3 path
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        self.log.info(f"Loading data from S3 path: {s3_path}")

        # Format and execute COPY command
        formatted_sql = self.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json_path=self.json_path,
        )

        self.log.info(f"Running COPY command for table {self.table}")
        self.log.info(f"JSON path: {self.json_path}")
        
        try:
            redshift.run(formatted_sql)
            self.log.info('StageToRedshiftOperator: COPY complete')
        except Exception as e:
            self.log.error(f"Error loading data: {str(e)}")
            self.log.info("Querying sys_load_error_detail for error details...")
            error_query = """
                SELECT TOP 10 * FROM sys_load_error_detail 
                ORDER BY start_time DESC;
            """
            try:
                error_records = redshift.get_records(error_query)
                for record in error_records:
                    self.log.error(f"Load error detail: {record}")
            except Exception as query_error:
                self.log.warning(f"Could not query error details: {str(query_error)}")
            raise
