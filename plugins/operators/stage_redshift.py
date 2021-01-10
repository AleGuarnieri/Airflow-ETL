from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    
    ui_color = '#358140'
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS {} 'auto'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 redshift_sink_table="",
                 s3_origin_bucket="",
                 s3_key="",
                 region="us-west-2",
                 data_format = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_sink_table = redshift_sink_table
        self.s3_key = s3_key
        self.s3_origin_bucket = s3_origin_bucket
        self.region = region
        self.data_format = data_format
        
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws = AwsHook(self.aws_credentials_id)
        credentials = aws.get_credentials()
        
        self.log.info("Clearing data from destination Redshift table")
        #redshift.run("DELETE FROM {}".format(self.redshift_sink_table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_origin_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.redshift_sink_table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.data_format
        )
        
        #redshift.run(formatted_sql)
        
        




