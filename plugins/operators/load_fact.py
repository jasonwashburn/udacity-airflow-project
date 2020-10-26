from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {}
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Building {self.table} table")
        formatted_sql =  LoadFactOperator.insert_sql.format(
            self.table,
            self.sql_statement
        )

        self.log.info(f"Executing sql: {formatted_sql}")
        redshift.run(formatted_sql)
