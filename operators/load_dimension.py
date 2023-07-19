import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    insert_table_sql = """
        INSERT INTO {}
        {};
    """

    truncate_table_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 load_sql_statement="",
                 table="",
                 truncate_table=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.load_sql_statement = load_sql_statement
        self.table = table
        self.truncate_table = truncate_table

    def execute(self, context):
        logging.info("---- LoadDimensionOperator Process Starting -----")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Truncate table if exists
        if self.truncate_table:
            logging.info(f"---- Truncating Dimension Table {self.table} ----")
            redshift_hook.run(LoadDimensionOperator.truncate_table_sql.format(self.table))

        # load dimension table
        logging.info(f"---- Loading Dimensioin Table {self.table} ----")
        formatted_insert_table_sql = LoadDimensionOperator.insert_table_sql.format(
            self.table,
            self.load_sql_statement
        )

        logging.info(f"---- Executing Inserting Query On Dimensions Tables: {formatted_insert_table_sql} ----")
        redshift_hook.run(formatted_insert_table_sql)