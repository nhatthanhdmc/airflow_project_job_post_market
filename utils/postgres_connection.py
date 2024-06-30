import psycopg2
from psycopg2 import sql

class PostGresDBManager:
    """ 
        Connect to a PostgreSQL database and perform the following operations: insert, delete, update, select, and truncate
    """
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.cursor = self.connection.cursor()
            print("Connection successful")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed")

    def insert(self, table, data):
        try:
            columns = data.keys()
            values = [data[column] for column in columns]
            insert_statement = sql.SQL(
                'INSERT INTO {table} ({fields}) VALUES ({values})'
            ).format(
                table=sql.Identifier(table),
                fields=sql.SQL(',').join(map(sql.Identifier, columns)),
                values=sql.SQL(',').join(sql.Placeholder() * len(columns))
            )
            self.cursor.execute(insert_statement, values)
            self.connection.commit()
            print("Insert successful")
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.connection.rollback()

    def delete(self, table, condition):
        try:
            delete_statement = sql.SQL('DELETE FROM {table} WHERE {condition}').format(
                table=sql.Identifier(table),
                condition=sql.SQL(condition)
            )
            self.cursor.execute(delete_statement)
            self.connection.commit()
            print("Delete successful")
        except Exception as e:
            print(f"Error deleting data: {e}")
            self.connection.rollback()

    def update(self, table, data, condition):
        try:
            set_statement = sql.SQL(', ').join(
                sql.Composed([sql.Identifier(k), sql.SQL(" = "), sql.Placeholder(k)]) for k in data.keys()
            )
            update_statement = sql.SQL('UPDATE {table} SET {set_values} WHERE {condition}').format(
                table=sql.Identifier(table),
                set_values=set_statement,
                condition=sql.SQL(condition)
            )
            self.cursor.execute(update_statement, data)
            self.connection.commit()
            print("Update successful")
        except Exception as e:
            print(f"Error updating data: {e}")
            self.connection.rollback()

    def select(self, table, columns='*', condition=None):
        try:
            if condition:
                query = sql.SQL('SELECT {fields} FROM {table} WHERE {condition}').format(
                    fields=sql.SQL(',').join(map(sql.Identifier, columns)),
                    table=sql.Identifier(table),
                    condition=sql.SQL(condition)
                )
            else:
                query = sql.SQL('SELECT {fields} FROM {table}').format(
                    fields=sql.SQL(',').join(map(sql.Identifier, columns)),
                    table=sql.Identifier(table)
                )
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except Exception as e:
            print(f"Error executing select query: {e}")
            return None

"""
# Example usage:
if __name__ == "__main__":
    db = PostgresDB(dbname='your_db', user='your_user', password='your_pass', host='your_host', port='your_port')
    db.connect()

    # Insert example
    db.insert('your_table', {'column1': 'value1', 'column2': 'value2'})

    # Update example
    db.update('your_table', {'column1': 'new_value1'}, "column2 = 'value2'")

    # Delete example
    db.delete('your_table', "column1 = 'new_value1'")

    # Select example
    results = db.select('your_table', ['column1', 'column2'], "column2 = 'value2'")
    print(results)
"""