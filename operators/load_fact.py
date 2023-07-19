import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    insert_fact_table_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 load_sql_statement="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_statement = load_sql_statement

    def execute(self, context):
        logging.info("---- LoadFactOperator Process Starting -----")
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        logging.info(f"---- Loading Fact Table {self.table} ----")
        formatted_insert_fact_table_sql = LoadFactOperator.insert_fact_table_sql.format(
            self.table,
            self.load_sql_statement
        )

        logging.info(f"---- Executing Inserting Query On Fact Table: {formatted_insert_fact_table_sql} ----")
        redshift_hook.run(formatted_insert_fact_table_sql)