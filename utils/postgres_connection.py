import psycopg2
from psycopg2 import pool
from psycopg2 import sql

class PostgresDB:
    def __init__(self, dbname, user, password, host='localhost', port=5432, minconn=1, maxconn=10):
        """
        Initializes the PostgresDB instance and sets up connection parameters.

        Args:
            dbname (str): Name of the database.
            user (str): Database username.
            password (str): Password for the database user.
            host (str): Database server address (default is 'localhost').
            port (int): Port number (default is 5432).
            minconn (int): Minimum number of connections in the pool.
            maxconn (int): Maximum number of connections in the pool.
        """
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection_pool = None

    def initialize_pool(self):
        """
        Initializes the connection pool for PostgreSQL connections.
        """
        try:
            self.connection_pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn,
                self.maxconn,
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            print("Connection pool created successfully")
        except Exception as e:
            print(f"Error creating connection pool: {e}")

    def close_pool(self):
        """
        Closes all connections in the pool.
        """
        if self.connection_pool:
            self.connection_pool.closeall()
        print("Connection pool closed")

    def get_connection(self):
        """
        Retrieves a connection from the connection pool.

        Returns:
            A connection object from the pool, or None if an error occurs.
        """
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            print(f"Error getting connection from pool: {e}")
            return None

    def put_connection(self, conn):
        """
        Returns a connection back to the connection pool.

        Args:
            conn: The connection object to return.
        """
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            print(f"Error returning connection to pool: {e}")

    def insert(self, table, data, id_name):
        """
        Inserts a single row into the specified table.

        Args:
            table (str): The name of the table.
            data (dict): A dictionary representing the row to be inserted.
            id_name (str): The name of the ID column to be returned.

        Returns:
            The ID of the inserted row, or None if an error occurs.
        """
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            columns = data.keys()            
            values = [data[column] for column in columns]
            insert_statement = f"""
                    INSERT INTO {table} ({', '.join(columns)}) 
                    VALUES ({', '.join(['%s'] * len(values))})
                    RETURNING {id_name}
                """                
            cursor.execute(insert_statement, values)
            conn.commit()
            inserted_id = cursor.fetchone()[0]
            cursor.close()
            return inserted_id
        except Exception as e:
            conn.rollback()
            print(f"Error inserting data: {e}")
            return None
        finally:
            self.put_connection(conn)

    def insert_many(self, table, data_list, id_name=None):
        """
        Inserts multiple rows into the specified table.

        Args:
            table (str): The table name.
            data_list (list of dict): A list of dictionaries, each representing a row to be inserted.
            id_name (str, optional): If specified, returns the IDs of inserted rows.

        Returns:
            list: A list of IDs of inserted rows, if `id_name` is provided.
        """
        if not data_list:
            print("No data to insert.")
            return []

        conn = self.get_connection()
        if not conn:
            return []

        try:
            cursor = conn.cursor()
            columns = data_list[0].keys()
            values_template = f"({', '.join(['%s'] * len(columns))})"
            values = []

            for data in data_list:
                values.append([data[column] for column in columns])

            insert_statement = f"""
                INSERT INTO {table} ({', '.join(columns)}) 
                VALUES {', '.join([values_template for _ in data_list])}
            """

            flattened_values = [item for sublist in values for item in sublist]
            if id_name:
                insert_statement += f" RETURNING {id_name}"

            cursor.execute(insert_statement, flattened_values)
            conn.commit()

            if id_name:
                inserted_ids = [row[0] for row in cursor.fetchall()]
            else:
                inserted_ids = []

            cursor.close()
            return inserted_ids
        except Exception as e:
            conn.rollback()
            print(f"Error inserting multiple rows: {e}")
            return []
        finally:
            self.put_connection(conn)

    def update(self, table, data, condition):
        """
        Updates rows in the specified table that match the given condition.

        Args:
            table (str): The name of the table.
            data (dict): A dictionary containing the columns and their new values.
            condition (dict): A dictionary representing the condition to match rows.

        Returns:
            int: The number of rows updated.
        """
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            set_statement = ", ".join([f"{key} = %s" for key in data.keys()])
            where_statement = " AND ".join([f"{key} = %s" for key in condition.keys()])
            values = list(data.values()) + list(condition.values())
            update_statement = f"UPDATE {table} SET {set_statement} WHERE {where_statement}"
            cursor.execute(update_statement, values)
            conn.commit()
            updated_rows = cursor.rowcount
            cursor.close()
            return updated_rows
        except Exception as e:
            conn.rollback()
            print(f"Error updating data: {e}")
            return None
        finally:
            self.put_connection(conn)

    def delete(self, table, condition):
        """
        Deletes rows from the specified table that match the given condition.

        Args:
            table (str): The name of the table.
            condition (dict): A dictionary representing the condition to match rows.

        Returns:
            int: The number of rows deleted.
        """
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            where_statement = " AND ".join([f"{key} = %s" for key in condition.keys()])
            values = list(condition.values())
            delete_statement = f"DELETE FROM {table} WHERE {where_statement}"
            cursor.execute(delete_statement, values)
            conn.commit()
            deleted_rows = cursor.rowcount
            cursor.close()
            return deleted_rows
        except Exception as e:
            conn.rollback()
            print(f"Error deleting data: {e}")
            return None
        finally:
            self.put_connection(conn)

    def select(self, table, columns='*', condition=None):
        """
        Selects rows from the specified table, with optional conditions.

        Args:
            table (str): The name of the table.
            columns (str or list): Columns to be selected (default is '*').
            condition (dict, optional): A dictionary representing the condition to match rows.

        Returns:
            list: A list of tuples representing the selected rows.
        """
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            if condition:
                where_statement = " AND ".join([f"{key} = %s" for key in condition.keys()])
                values = list(condition.values())
                select_statement = f"SELECT {columns} FROM {table} WHERE {where_statement}"
                cursor.execute(select_statement, values)
            else:
                select_statement = f"SELECT {columns} FROM {table}"
                cursor.execute(select_statement)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            print(f"Error selecting data: {e}")
            return None
        finally:
            self.put_connection(conn)
            
    def truncate_table(self, table):
        """
        Truncates the specified table, removing all data.

        Args:
            table (str): The name of the table to truncate.
        """
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            truncate_statement = f"TRUNCATE TABLE {table}"
            cursor.execute(truncate_statement)
            conn.commit()
            cursor.close()
            print(f"Table {table} truncated successfully")
        except Exception as e:
            conn.rollback()
            print(f"Error truncating table: {e}")
        finally:
            self.put_connection(conn)

### Example Usage for All Functions

if __name__ == "__main__":
    # Initialize and set up the connection pool
    db = PostgresDB(
        dbname='your_dbname',
        user='your_user',
        password='your_password',
        host='your_host',
        port='your_port'
    )
    db.initialize_pool()

    # Insert Example
    data_to_insert = {"column1": "value1", "column2": "value2"}
    inserted_id = db.insert("your_table", data_to_insert, id_name="id")
    print(f"Inserted record ID: {inserted_id}")

    # Insert Many Example
    data_list_to_insert = [
        {"column1": "value1", "column2": "value2"},
        {"column1": "value3", "column2": "value4"},
        {"column1": "value5", "column2": "value6"}
    ]
    inserted_ids = db.insert_many("your_table", data_list_to_insert, id_name="id")
    print(f"Inserted record IDs: {inserted_ids}")

    # Update Example
    data_to_update = {"column1": "updated_value1"}
    condition_to_update = {"column2": "value2"}
    updated_rows = db.update("your_table", data_to_update, condition_to_update)
    print(f"Number of rows updated: {updated_rows}")

    # Delete Example
    condition_to_delete = {"column1": "value1"}
    deleted_rows = db.delete("your_table", condition_to_delete)
    print(f"Number of rows deleted: {deleted_rows}")

    # Select Example
    selected_data = db.select("your_table", columns="column1, column2", condition={"column2": "value4"})
    print(f"Selected data: {selected_data}")

    # Truncate Table Example
    db.truncate_table("your_table")

    # Close the connection pool
    db.close_pool()