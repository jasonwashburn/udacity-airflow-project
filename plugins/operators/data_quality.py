from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test_case in self.tests:
            sql = test_case.get('test')
            expected_result = test_case.get('expected_result')

            record = redshift.get_records(sql)[0][0]

            if record == expected_result:
                self.log.info(f'Test passed: result: {record} / expected_result: {expected_result}')
            else:
                self.log.info(f'Test failed: result: {record} / expected_result: {expected_result}')
        