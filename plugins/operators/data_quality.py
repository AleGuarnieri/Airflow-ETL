from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Defines data quality checks class
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 dq_checks = [],
                 redshift_conn_id = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Performs data quality check queries defined in the main dag.
        Each query is executed and the number of retrieved records
        is compared with the expected one: if there is no match,
        the pipeline fails.
        """
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = ""
        error_count = 0
        
        self.log.info("Performing data quality checks on fact and dimension tabels")
        for check in self.dq_checks:
            sql = check.get('check')
            exp_result = check.get('expected_result')
            self.log.info(sql)
            try:
                records = redshift.get_records(sql)[0]
                self.log.info(records)
            except Exception as e:
                self.log.info(f"Data quality check failed with exception: {e}")

            if exp_result != records[0]:
                error_count += 1
                failing_tests.append(sql)

        if error_count > 0:
            self.log.info('Some tests failed:')
            self.log.info(failing_tests)
            raise ValueError('Data quality checks failed')
        else:
            self.log.info("Data quality checks passed")
