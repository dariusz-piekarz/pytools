from pyodbc import Connection, connect
from pandas import DataFrame, read_sql_query
from codecs import open
import re
from loguru import logger


class SqlServerConnection:
    """
        Class for connecting to a SQL Server database and extracting data.

        Attributes:
            conn (Connection): The connection object to the SQL Server database.

        Methods:
            __init__(self, database: str, server: str, driver: str = 'ODBC Driver 17 for SQL Server',
                     authentication: str = 'ActiveDirectoryIntegrated', encrypted: str = 'No', tr_conn: str = 'No'):
                Connects to the specified SQL Server database.

            @staticmethod
            read_query(query_path: str, words_to_replace: dict[str: any] | None = None) -> str:
                Reads a query from a file path and replaces specified words with their corresponding values.

            def connect_to_db(self, database: str, server: str, driver: str = 'ODBC Driver 17 for SQL Server',
                              authentication: str = 'ActiveDirectoryIntegrated', encrypted: str = 'No',
                              tr_conn: str = 'No') -> None:
                Establishes a connection to the specified SQL Server database.

            def extract_df(self, query: str) -> DataFrame:
                Extracts data from the SQL Server database using the provided query and returns it as a
                pandas DataFrame.

            def extract_df_from_path(self, query_path: str, words_to_replace: dict[str: any] = None) -> DataFrame:
                Reads a query from a file path, replaces specified words with their corresponding values, and extracts
                data from the SQL Server database using the modified query.
                The extracted data is returned as a pandas DataFrame.

            __del__(self):
                Closes the connection to the SQL Server database.
        """
    conn: Connection

    def __init__(self, database: str, server: str, driver: str = 'ODBC Driver 17 for SQL Server',
                 authentication: str = 'ActiveDirectoryIntegrated', encrypted: str = 'No', tr_conn: str = 'No'):
        self.connect_to_db(database, server, driver, authentication, encrypted, tr_conn)

    @staticmethod
    def read_query(query_path: str, words_to_replace: dict[str: any] | None = None) -> str:
        """
        Reads a query from a file path and replaces specified words with their corresponding values.

        Args:
            query_path (str): The path to the file containing the query.
            words_to_replace (dict[str: any] | None): A dictionary containing words to be replaced in the query
            and their corresponding values. If None, no replacements will be made.

        Returns:
            str: The modified query with specified words replaced.
        """
        logger.info(f"Reading query from the path '{query_path}'.")
        query = open(query_path, mode='r', encoding='utf-8-sig').read()
        logger.info(f"Query received.")
        if words_to_replace is not None:
            rough_query = re.split("#*#", query)
            query = str()
            for sentence in rough_query:
                if any([key == sentence for key in words_to_replace.keys()]):
                    for key, val in words_to_replace.items():
                        if key == sentence:
                            query += val
                else:
                    query += sentence
        return query

    def connect_to_db(self, database: str, server: str, driver: str = 'ODBC Driver 17 for SQL Server',
                      authentication: str = 'ActiveDirectoryIntegrated', encrypted: str = 'No',
                      tr_conn: str = 'No') -> None:
        """
        Establishes a connection to the specified SQL Server database.

        Args:
            database (str): The name of the database to connect to.
            server (str): The server where the database is located.
            driver (str): The ODBC driver to use for connecting to the database.
            Default is 'ODBC Driver 17 for SQL Server'.
            authentication (str): The authentication method to use for connecting to the database.
            Default is 'ActiveDirectoryIntegrated'.
            encrypted (str): A flag indicating whether the connection should be encrypted. Default is 'No'.
            tr_conn (str): A flag indicating whether the connection should use trusted connections. Default is 'No'.

        Returns:
            None
        """
        logger.info(f"Connecting to the database {database} in the server {server}.")
        self.conn = connect(f'DRIVER={driver};'
                            f'SERVER={server};'
                            f'AUTHENTICATION={authentication};'
                            f'DATABASE={database};'
                            f'Encrypt={encrypted};'
                            f'Trusted_Connection={tr_conn};')
        logger.info(f"Connection to the server {server} established.")

    def extract_df(self, query: str) -> DataFrame:
        """
        Extracts data from the SQL Server database using the provided query and returns it as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute against the database.

        Returns:
            DataFrame: A pandas DataFrame containing the results of the query.
        """
        df = read_sql_query(query, con=self.conn)
        logger.info(f"Data collected successfully.")
        return df

    def extract_df_from_path(self, query_path: str, words_to_replace: dict[str: any] = None) -> DataFrame:
        """
        Reads a query from a file path, replaces specified words with their corresponding values,
        and extracts data from the SQL Server database using the modified query.
        The extracted data is returned as a pandas DataFrame.

        Args:
            query_path (str): The path to the file containing the query.
            words_to_replace (dict[str: any] | None): A dictionary containing words to be replaced in the query
            and their corresponding values. If None, no replacements will be made.

        Returns:
            DataFrame: A pandas DataFrame containing the results of the query.
        """
        query = SqlServerConnection.read_query(query_path, words_to_replace)
        df = read_sql_query(query, con=self.conn)
        logger.info(f"Data collected successfully.")
        return df

    def __del__(self):
        """
        Closes the connection to the SQL Server database.

        Returns:
            None
        """
        self.conn.close()
        logger.info("Connection to server terminated.")


class MSAccessConnection:
    """
    Class for connecting to an MS Access database and extracting data.

    Attributes:
        conn (Connection): The connection object to the MS Access database.

    Methods:
        __init__(self, dbpath: str, driver: str = 'Microsoft Access Driver (*.mdb, *.accdb)'):
            Connects to the specified MS Access database.

        connect_to_db(self, dbpath: str, driver: str) -> None:
            Establishes a connection to the specified MS Access database.

        extract_df(self, query: str) -> DataFrame:
            Extracts data from the MS Access database using the provided query and returns it as a pandas DataFrame.

        extract_df_from_path(self, query_path: str, words_to_replace: dict[str: any] = None) -> DataFrame:
            Reads a query from a file path, replaces specified words with their corresponding values,
             and extracts data from the MS Access database using the modified query.
            The extracted data is returned as a pandas DataFrame.

        __del__(self):
            Closes the connection to the MS Access database.
    """
    conn: Connection

    def __init__(self, dbpath: str, driver: str = 'Microsoft Access Driver (*.mdb, *.accdb)'):
        """
        Connects to the specified MS Access database.

        Args:
            dbpath (str): The path to the MS Access database file.
            driver (str): The ODBC driver to use for connecting to the database.
            Default is 'Microsoft Access Driver (*.mdb, *.accdb)'.

        Returns:
            None
        """
        self.connect_to_db(dbpath, driver)

    def connect_to_db(self, dbpath: str, driver: str) -> None:
        """
        Establishes a connection to the specified MS Access database.

        Args:
            dbpath (str): The path to the MS Access database file.
            driver (str): The ODBC driver to use for connecting to the database.

        Returns:
            None
        """
        logger.info("Connecting to the MS Access database.")
        self.conn = connect(f"DRIVER={driver}; DBQ={dbpath};")
        logger.info("Connection  to the MS Access database established.")

    def extract_df(self, query: str) -> DataFrame:
        """
        Extracts data from the MS Access database using the provided query and returns it as a pandas DataFrame.

        Args:
            query (str): The SQL query to execute against the database.

        Returns:
            DataFrame: A pandas DataFrame containing the results of the query.
        """
        df = read_sql_query(query, con=self.conn)
        logger.info("Data collected successfully.")
        return df

    def extract_df_from_path(self, query_path: str, words_to_replace: dict[str: any] = None) -> DataFrame:
        """
        Reads a query from a file path, replaces specified words with their corresponding values,
        and extracts data from the MS Access database using the modified query.
        The extracted data is returned as a pandas DataFrame.

        Args:
            query_path (str): The path to the file containing the query.
            words_to_replace (dict[str: any] | None): A dictionary containing words to be replaced in the query
            and their corresponding values. If None, no replacements will be made.

        Returns:
            DataFrame: A pandas DataFrame containing the results of the query.
        """
        query = SqlServerConnection.read_query(query_path, words_to_replace)
        df = read_sql_query(query, con=self.conn)
        logger.info("Data collected successfully.")
        return df

    def __del__(self):
        """
        Closes the connection to the MS Access database.

        Returns:
            None
        """
        self.conn.close()
        logger.info("Connection to MS Access terminated.")


if __name__ == "__main__":
    pass
