from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if len(self.dq_checks) <= 0:
            self.log.info("No data quality checks provided")
            return

        redshift_hook = PostgresHook(self.redshift_conn_id)
        error_count = 0
        failed_tests = []

        for check in self.dq_checks:
            try:
                self.log.info(f"Running query: {check['sql']}")
                records = redshift_hook.get_records(check['sql'])[0]
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")

            if check['expected_result'] != records[0]:
                error_count += 1
                failed_tests.append(check['sql'])

        if error_count > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed')
        else:
            self.log.info("All data quality checks passed")