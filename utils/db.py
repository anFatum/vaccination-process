import sqlite3
from utils.abc import Singleton
from sqlite3 import Error


class DatabaseConnector(metaclass=Singleton):
    conn = None
    filename = None

    def __init__(self, filename):
        self.filename = filename

    def _get_conn(self):
        """Creates and returns a new database connection"""

        if not self.conn:
            self.conn = sqlite3.connect(self.filename)

        return self.conn

    def query(self, sql: str, num_rows: int = None, commit: bool = False):
        """Retrieve records from a db table"""

        # Get the db connection
        conn = self._get_conn()
        result = None

        if conn is not None:
            try:
                # Open a cursor to perform database operations
                cursor = conn.cursor()

                # Execute a command: this fetches all rows from a table
                if num_rows is not None:
                    cursor.execute(f"{sql} LIMIT {num_rows};")
                else:
                    cursor.execute(f"{sql}")

                if commit:
                    self.conn.commit()

                result = cursor.fetchall()
                # Close the cursor
                cursor.close()
            except Error as e:
                pass
            finally:
                self.close_connection()
        return result

    def close_connection(self) -> None:
        """Closes the database connection"""

        if self.conn is not None:
            self.conn.close()
            self.conn = None
