from psycopg2 import connect
from psycopg2.extensions import connection, cursor
from loguru import logger
from pandas import DataFrame


class PostgreAdminConnector:
    """
        A class for interacting with a PostgreSQL database.

        Attributes:
            connection (psycopg2.extensions.connection): The connection object to the PostgreSQL database.
            cursor (psycopg2.extensions.cursor): The cursor object for executing SQL queries.
            tables_columns (dict): A dictionary that maps table names to their respective column names.

        Methods:
            set_connection(user: str, password: str, host: str, port: str, database: str) -> None:
                Establishes a connection to the specified PostgreSQL database.

            execute_query(query: str) -> list[tuple[any, ...]]:
                Executes the given SQL query and returns the result as a list of tuples.

            query_to_df(qrt: str) -> pandas.DataFrame:
                Executes the given SQL query and returns the result as a pandas DataFrame.

            query_replace(query: str, words_to_replace: dict, to_df: bool = True) -> None:
                Replaces specified words in the given SQL query and executes it. If to_df is True,
                returns the result as a pandas DataFrame.

            get_col_names(table_name: str) -> None:
                Retrieves the column names for the specified table.

            get_record_subset(table_name: str, col_names: list[str]) -> pandas.DataFrame:
                Retrieves a subset of columns for the specified table.

            get_record(self, table_name: str, where: dict[str: any]) -> list[tuple[any, ...]]:
                Retrieves a record from the specified table based on the given WHERE clause.

            get_record_df(self, table_name: str, where: dict[str: any]) -> pandas.DataFrame:
                Retrieves a record from the specified table based on the given WHERE clause and
                returns the result as a pandas DataFrame.

            insert_record(self, table_name: str, to_insert: dict[str: any]) -> None:
                Inserts a new record into the specified table with the given values.

            update_record(self, table_name: str, to_update: dict[str: any], update_on_field: dict[str: any]) -> None:
                Updates a record in the specified table based on the given WHERE clause and sets the given values.

            delete_record(self, table_name: str, field_identifier: dict[str: any]) -> None:
                Deletes a record from the specified table based on the given WHERE clause.

            delete_record_condition_and(self, table_name: str, field_identifier: dict[str: any]) -> None:
                Deletes a record from the specified table based on two given WHERE clauses.

            terminate_connection(self) -> None:
                Closes the connection to the PostgreSQL database.

            __del__(self):
                Closes the connection to the PostgreSQL database when the instance is destroyed.
    """
    tables_columns = dict({})
    connection: connection
    cursor: cursor

    def set_connection(self, user: str, password: str, host: str, port: str, database: str) -> None:
        """
        Establishes a connection to the specified PostgreSQL database.

        Parameters:
            user (str): The username for the database connection.
            password (str): The password for the database connection.
            host (str): The hostname for the database connection.
            port (str): The port number for the database connection.
            database (str): The name of the database to connect to.

        Returns:
            None
        """
        self.connection = connect(
                                  user=user,
                                  password=password,
                                  host=host,
                                  port=port,
                                  database=database)
        self.cursor = self.connection.cursor()
        logger.info(f"Connection to {database} database established")

    def execute_query(self, query: str) -> list[tuple[any, ...]]:
        self.cursor.execute(query)
        record = self.cursor.fetchall()
        return record

    def query_to_df(self, qrt: str) -> DataFrame:
        self.cursor.execute(qrt)
        record = self.cursor.fetchall()
        columns = [desc[0] for desc in self.cursor.description]
        df = DataFrame(record, columns=columns)
        return df.copy(deep=True)

    def query_replace(self, query: str, words_to_replace: dict, to_df: bool = True):
        qry = query
        for key, value in words_to_replace.items():
            qry = qry.replace(key, value)
        if to_df:
            self.query_to_df(qry)
        else:
            self.execute_query(qry)

    def get_col_names(self, table_name: str) -> None:
        schema = table_name.split(".")[0]
        table = table_name.split(".")[1]
        self.cursor.execute(f"SELECT column_name FROM information_schema.columns"
                            f" WHERE table_schema = '{schema}'"
                            f"AND table_name = '{table}';")
        self.tables_columns[table_name] = [row[0] for row in self.cursor.fetchall()]

    def get_record_subset(self, table_name: str, col_names: list[str]) -> DataFrame:
        if table_name not in self.tables_columns:
            self.get_col_names(table_name)
        cols = ", ".join(['"' + col_name + '"' for col_name in col_names])
        return self.query_to_df(f"SELECT {cols} FROM {table_name};")

    def get_record(self, table_name: str, where: dict[str: any]) -> list[tuple[any, ...]]:
        key_cond = '"' + list(where.keys())[0] + '"'
        val_cond = "'" + list(where.values())[0] + "'"
        qry = f'SELECT * FROM {table_name} WHERE {key_cond}={val_cond}'
        return self.execute_query(qry)

    def get_record_df(self, table_name: str, where: dict[str: any]) -> DataFrame:
        key_cond = '"' + list(where.keys())[0] + '"'
        val_cond = "'" + list(where.values())[0] + "'"
        qry = f'SELECT * FROM {table_name} WHERE {key_cond}={val_cond}'
        return self.query_to_df(qry)

    def insert_record(self, table_name: str, to_insert: dict[str: any]) -> None:
        if table_name not in self.tables_columns:
            self.get_col_names(table_name)

        columns = ", ".join(['"' + field_name + '"' for field_name in self.tables_columns[table_name]])
        values = ", ".join(["'" + to_insert[key_name] + "'"
                            if key_name in to_insert.keys()
                            else 'NULL'
                            for key_name in self.tables_columns[table_name]]).replace("'NULL'", "NULL")

        qrt = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
        self.cursor.execute(qrt)
        self.connection.commit()

    def update_record(self, table_name: str, to_update: dict[str: any], update_on_field: dict[str: any]) -> None:

        update = ",".join(['"' + key + '"' + '=' + "'" + val + "'" for key, val in to_update.items()])

        where_key = '"' + str(list(update_on_field.keys())[0]) + '"'
        where_val = "'" + str(list(update_on_field.values())[0]) + "'"

        qrt = f'UPDATE {table_name} SET {update} WHERE {where_key}={where_val}'.replace("'NULL'", "NULL")
        self.cursor.execute(qrt)
        self.connection.commit()

    def delete_record(self, table_name: str, field_identifier: dict[str: any]) -> None:
        field_name = '"' + str(list(field_identifier.keys())[0]) + '"'
        field_value = "'" + str(list(field_identifier.values())[0]) + "'"

        qry = f"DELETE FROM {table_name} WHERE {field_name}={field_value};"
        self.cursor.execute(qry)
        self.connection.commit()

    def delete_record_condition_and(self, table_name: str, field_identifier: dict[str: any]) -> None:
        field_names = ['"' + str(list(field_identifier.keys())[i]) + '"' for i in range(2)]
        field_values = ["'" + str(list(field_identifier.values())[i]) + "'" for i in range(2)]

        qry = (f"DELETE FROM {table_name} WHERE {field_names[0]}={field_values[0]}"
               f" AND {field_names[1]}={field_values[1]};")
        self.cursor.execute(qry)
        self.connection.commit()

    def terminate_connection(self) -> None:
        if hasattr(self, 'cursor'):
            if self.cursor:
                self.cursor.close()
        if hasattr(self, 'connection'):
            if self.connection:
                self.connection.close()

    def __del__(self):
        self.terminate_connection()


if __name__ == "__main__":
    pass
