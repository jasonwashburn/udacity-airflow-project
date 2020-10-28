from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """ Inserts data into a dimension table utilizing a provided SQL statement.

        INPUTS:
        redshift_conn_id = Airflow Connection to Redshift
        table = target dimension table
        sql_statement = SQL statement to perform INSERTS
        append_mode = (Boolean) If True, insert statement appends data to 
            existing table data,otherwise table is wiped clean prior to 
            performing inserts.
    """

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
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not self.append_mode:
            self.log.info(f"Wiping {self.table} table clean!")
            redshift.run(f"DELETE FROM {self.table}")

        self.log.info(f"Building {self.table} table.")
        formatted_sql =  LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_statement
        )

        self.log.info(f"Executing sql: {formatted_sql}")
        redshift.run(formatted_sql)