import psycopg2
from psycopg2 import pool, sql
import pymongo

class MongoDBToPostgres:
    def __init__(self, mongo_uri, mongo_db, mongo_collection, pg_dbname, pg_user, pg_password, pg_host='localhost', pg_port=5432, minconn=1, maxconn=10):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection
        self.pg_dbname = pg_dbname
        self.pg_user = pg_user
        self.pg_password = pg_password
        self.pg_host = pg_host
        self.pg_port = pg_port
        self.minconn = minconn
        self.maxconn = maxconn
        self.mongo_client = None
        self.pg_pool = None

    def connect_mongo(self):
        try:
            self.mongo_client = pymongo.MongoClient(self.mongo_uri)
            self.mongo_db = self.mongo_client[self.mongo_db]
            self.mongo_collection = self.mongo_db[self.mongo_collection]
            print("Connected to MongoDB")
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")

    def connect_postgres(self):
        try:
            self.pg_pool = psycopg2.pool.SimpleConnectionPool(
                self.minconn, 
                self.maxconn,
                dbname=self.pg_dbname,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
            )
            if self.pg_pool:
                print("Connection pool created successfully")
        except Exception as e:
            print(f"Error creating connection pool: {e}")

    def close_connections(self):
        if self.mongo_client:
            self.mongo_client.close()
            print("MongoDB connection closed")
        if self.pg_pool:
            self.pg_pool.closeall()
            print("PostgreSQL connection pool closed")

    def transfer_data(self, pg_table):
        conn = None
        try:
            conn = self.pg_pool.getconn()
            cursor = conn.cursor()
            mongo_docs = self.mongo_collection.find()

            for doc in mongo_docs:
                doc_id = doc.pop('_id', None)  # Remove MongoDB specific ID
                columns = doc.keys()
                values = [doc[column] for column in columns]
                insert_statement = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING id"
                ).format(
                    table=sql.Identifier(pg_table),
                    fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
                    values=sql.SQL(', ').join(sql.Placeholder() * len(values))
                )
                cursor.execute(insert_statement, values)

            conn.commit()
            cursor.close()
            print("Data transferred successfully")
        except Exception as e:
            if conn:
                conn.rollback()
            print(f"Error transferring data: {e}")
        finally:
            if conn:
                self.pg_pool.putconn(conn)

# Example usage
if __name__ == "__main__":
    mongo_uri = "mongodb://localhost:27017/"
    mongo_db = "your_mongo_db"
    mongo_collection = "your_mongo_collection"
    pg_dbname = "your_pg_dbname"
    pg_user = "your_pg_user"
    pg_password = "your_pg_password"
    pg_host = "your_pg_host"
    pg_port = 5432
    pg_table = "stg.cv_employer_detail"  # Ensure this table exists in your PostgreSQL database

    transfer = MongoDBToPostgres(mongo_uri, mongo_db, mongo_collection, pg_dbname, pg_user, pg_password, pg_host, pg_port)
    transfer.connect_mongo()
    transfer.connect_postgres()
    transfer.transfer_data(pg_table)
    transfer.close_connections()
