from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Starting data quality checks')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for check in self.dq_checks:
            sql = check.get('sql')
            expected_result = check.get('expected_result')

            self.log.info(f'Running SQL: {sql}')
            records = redshift.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. SQL returned no results: {sql}")

            actual_result = records[0][0]

            if actual_result != expected_result:
                raise ValueError(f"Data quality check failed for SQL: {sql}. "
                                 f"Expected: {expected_result}, Got: {actual_result}")

            self.log.info(f"Data quality check passed for SQL: {sql}. Result: {actual_result}")

        self.log.info('All data quality checks passed')