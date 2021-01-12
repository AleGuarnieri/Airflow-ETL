from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
"""
Defines loading class for dimension tabels
"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table
        self.sql=sql
        self.truncate=truncate

    def execute(self, context):
    """
    Connection to redshift cluster is performed and then 
    data are transformed and loaded into dimension tables
    Parameter "truncate" allows control over truncating
    dimension table before loading
    """
    
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Clearing data from Redshift dimension table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Transforming and loading data from staging tables to dimension tables")
        redshift.run(self.sql)
