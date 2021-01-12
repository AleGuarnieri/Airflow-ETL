from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults 


class LoadFactOperator(BaseOperator):
"""
Defines loading class for fact tabels
"""

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql

    def execute(self, context):
    """
    Connection to redshift cluster is performed and then 
    data are transformed and loaded into fact table
    """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Transforming and loading data from staging tables to fact table")
        redshift.run(self.sql)
