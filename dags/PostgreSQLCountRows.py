from typing import Mapping, Callable, Any, Iterable

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.hooks.sql import fetch_all_handler


class PostgreSQLCountRowsOperator(SQLExecuteQueryOperator):
    def __init__(self,
                 table_name: str,
                 conn_id: str | None = None,
                 parameters: Mapping | Iterable | None = None,
                 handler: Callable[[Any], Any] = fetch_all_handler,
                 database: str | None = None,
                 split_statements: bool | None = None,
                 return_last: bool = True,
                 show_return_value_in_logs: bool = False,
                 **kwargs) -> None:
        self.conn_id = conn_id
        self.table_name = table_name
        self.sql = f"SELECT COUNT(*) FROM {table_name}"
        self.parameters = parameters
        self.handler = handler
        self.database = database
        self.split_statements = split_statements
        self.return_last = return_last
        self.show_return_value_in_logs = show_return_value_in_logs
        super().__init__(sql=self.sql, conn_id=self.conn_id,
                         parameters=self.parameters, handler=self.handler, database=self.database,
                         split_statements=self.split_statements, return_last=self.return_last,
                         show_return_value_in_logs=self.show_return_value_in_logs, **kwargs)

    def execute(self, context):
        pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

        query = pg_hook.get_first(self.sql)
        context['task_instance'].xcom_push(key='count', value=query[0])
