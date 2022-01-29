from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_stmt="",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_stmt = load_sql_stmt

    def execute(self, context):
        '''
        This operator will load fact data into a given target table.
        The SQL is contained in the hepler class sql_queries
        '''

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        insert_sql = f"""INSERT INTO {self.table} {self.load_sql_stmt};"""

        self.log.info(f"Loading fact table '{self.table}' into Redshift")
        redshift.run(insert_sql)
