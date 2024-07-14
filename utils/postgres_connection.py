import psycopg2
from psycopg2 import pool
from psycopg2 import sql

class PostgresDB:
    def __init__(self, dbname, user, password, host='localhost', port=5432, minconn=1, maxconn=10):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.minconn = minconn
        self.maxconn = maxconn
        self.connection_pool = None

    def initialize_pool(self):
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
        if self.connection_pool:
            self.connection_pool.closeall()
        print("Connection pool closed")

    def get_connection(self):
        try:
            return self.connection_pool.getconn()
        except Exception as e:
            print(f"Error getting connection from pool: {e}")
            return None

    def put_connection(self, conn):
        try:
            self.connection_pool.putconn(conn)
        except Exception as e:
            print(f"Error returning connection to pool: {e}")

    def insert(self, table, data):
        conn = self.get_connection()
        if not conn:
            return None
        try:
            cursor = conn.cursor()
            columns = data.keys()
            values = [data[column] for column in columns]
            insert_statement = sql.SQL(
                "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING id"
            ).format(
                table=sql.Identifier(table),
                fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                values=sql.SQL(', ').join(sql.Placeholder() * len(values))
            )
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

    def update(self, table, data, condition):
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

# Example usage:
# if __name__ == "__main__":
#     db = PostgresDB(dbname='your_dbname', user='your_user', password='your_password', host='your_host', port='your_port')
    
#     db.initialize_pool()
    
#     # Insert example
#     data_to_insert = {"column1": "value1", "column2": "value2"}
#     inserted_id = db.insert("your_table", data_to_insert)
#     print(f"Inserted record ID: {inserted_id}")

#     # Update example
#     data_to_update = {"column1": "new_value1"}
#     condition_to_update = {"column2": "value2"}
#     updated_rows = db.update("your_table", data_to_update, condition_to_update)
#     print(f"Number of rows updated: {updated_rows}")

#     # Delete example
#     condition_to_delete = {"column1": "value1"}
#     deleted_rows = db.delete("your_table", condition_to_delete)
#     print(f"Number of rows deleted: {deleted_rows}")

#     # Select example
#     selected_data = db.select("your_table", columns="column1, column2", condition={"column2": "value2"})
#     print(f"Selected data: {selected_data}")
    
#     db.close_pool()
