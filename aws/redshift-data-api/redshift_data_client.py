import json
import time


class RedshiftDataClient:
    def __init__(
        self, data_client, cluster_identifier: str, database: str, db_user: str
    ):
        self.data_client = data_client
        self.cluster_identifier = cluster_identifier
        self.database = database
        self.db_user = db_user

    def execute_query(self, sql: str, interval_sec: int = 1):
        query_id = self.__execute_statement(sql)["Id"]
        self.__wait_for_result(query_id=query_id, interval_sec=interval_sec)

        return self.__get_statement_result(query_id=query_id)

    def execute_write_query(self, sql: str, interval_sec: int = 1):
        query_id = self.__execute_statement(sql)["Id"]

        return self.__wait_for_result(query_id=query_id, interval_sec=interval_sec)

    def __execute_statement(self, sql: str):
        return self.data_client.execute_statement(
            ClusterIdentifier=self.cluster_identifier,
            Database=self.database,
            DbUser=self.db_user,
            Sql=sql,
        )

    def __get_statement_result(self, query_id: str):
        return self.data_client.get_statement_result(Id=query_id)

    def __wait_for_result(self, query_id: str, interval_sec=1):
        while True:
            time.sleep(interval_sec)
            statement = self.data_client.describe_statement(Id=query_id)

            status = statement["Status"]
            if status == "FINISHED":
                return statement
            elif status == "FAILED":
                raise Exception(f"{status}: {statement}")
            elif status == "ABORTED":
                raise Exception(f"{status}: The query run was stopped by the user.")
            else:
                interval_sec = interval_sec * 2
