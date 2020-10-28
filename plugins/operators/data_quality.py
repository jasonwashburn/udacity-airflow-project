from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):
    """ Performs data quality checks by executing provided SQL statements
        and comparing results to expected results.

        INPUTS:
        redshift_conn_id = Airflow Connection to Redshift
        tests = List of Dictionarys including an SQL test and expected result
            example ' [{'test': 'SELECT ... FROM', 'expected_result': '0'}])
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tests=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests

    def execute(self, context):
        test_number = 0
        num_failed = 0

        # Get PostgresHook using provided redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Cycle through provided test SQL statements and compare results to
        # expected result. Raises AirflowException if result does not match expected.
        
        for test_case in self.tests:
            # Get test case sql and expected result
            sql = test_case.get('test')
            expected_result = test_case.get('expected_result')

            # Execute SQL and record result
            record = redshift.get_records(sql)[0][0]

            # Compare result to expected
            if record == expected_result:
                self.log.info(f'Test #{test_number} passed!')
            else:
                num_failed += 1
                self.log.info (
                    f'\nTest #{test_number} failed:' +
                    f'\n\tTest SQL: {sql}' +
                    f'\n\tExpected Result: {expected_result}' +
                    f'\n\tActual Result: {record}'
                )
            test_number += 1
        
        if num_failed > 0:
            raise AirflowException(f'Data Quality Check failed. {num_failed} test(s) failed.')