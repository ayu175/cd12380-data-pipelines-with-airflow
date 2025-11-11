from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_statement='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement

    def execute(self, context):
        self.log.info(f'Loading fact table {self.table} in Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Insert data from the SQL statement into the fact table
        insert_sql = f"INSERT INTO {self.table} {self.sql_statement}"
        self.log.info(f"Running: INSERT INTO {self.table}...")
        redshift.run(insert_sql)
        
        self.log.info(f'Fact table {self.table} loaded successfully')