from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_statement='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'Loading dimension table {self.table} in Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Truncating table {self.table} before insert')
            redshift.run(f'TRUNCATE TABLE {self.table};')

        self.log.info(f'Inserting data into {self.table}')
        insert_sql = f"INSERT INTO {self.table} {self.sql_statement}"
        self.log.info(f"Running: INSERT INTO {self.table}...")
        redshift.run(insert_sql)
        
        self.log.info(f'Dimension table {self.table} loaded successfully')
