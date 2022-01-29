from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 truncate_table=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_stmt = load_sql_stmt
        self.truncate_table = truncate_table

    def execute(self, context):
        """
               This operator will load fact tables.
               Given the query and destination table as input.
               The truncate table parameter must be set to True,
               if the table needs to be cleaned before load.
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            truncate_sql = f"""TRUNCATE TABLE {self.table};"""
            redshift.run(truncate_sql)

        insert_sql = f"""INSERT INTO {self.table} {self.load_sql_stmt};"""

        self.log.info(f"Loading dimension table '{self.table}' into Redshift")
        redshift.run(insert_sql)