from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 append_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_mode:
            self.log.info(f"Clearing {self.table} table.")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Building {self.table} table.")
        formatted_sql =  LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_statement
        )

        self.log.info(f"Executing sql: {formatted_sql}")
        redshift.run(formatted_sql)