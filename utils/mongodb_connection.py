from pymongo import MongoClient

class MongoDB:
    """
    Connect to a MongoDB database and perform CRUD (Create, Read, Update, Delete) operations, including truncation.
    """
    def __init__(self, dbname, collection_name, host='localhost', port=27017, username=None, password=None):
        """
        Initializes a MongoDB instance with connection parameters.

        Args:
            dbname (str): Name of the database.
            collection_name (str): Name of the initial collection.
            host (str): Host address for the MongoDB instance (default is 'localhost').
            port (int): Port number for MongoDB (default is 27017).
            username (str, optional): Username for database authentication.
            password (str, optional): Password for database authentication.
        """
        self.dbname = dbname
        self.collection_name = collection_name
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None
        self.db = None
        self.collection = None
        self.maxPoolSize = 10
        self.minPoolSize = 5
        self.connectTimeoutMS = 30000
        self.socketTimeoutMS = 30000

    def connect(self):
        """
        Establishes a connection to the MongoDB server and sets the database and collection.
        """
        try:
            if self.username and self.password:
                self.client = MongoClient(self.host, self.port, username=self.username, password=self.password,
                                          maxPoolSize=self.maxPoolSize, minPoolSize=self.minPoolSize,
                                          connectTimeoutMS=self.connectTimeoutMS, socketTimeoutMS=self.socketTimeoutMS)
            else:
                self.client = MongoClient(self.host, self.port, maxPoolSize=self.maxPoolSize, minPoolSize=self.minPoolSize,
                                          connectTimeoutMS=self.connectTimeoutMS, socketTimeoutMS=self.socketTimeoutMS)
            self.db = self.client[self.dbname]
            self.collection = self.db[self.collection_name]
            print("Connection successful")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        """
        Closes the connection to the MongoDB server.
        """
        if self.client:
            self.client.close()
        print("Connection closed")

    def set_collection(self, collection_name):
        """
        Sets the collection for future operations.

        Args:
            collection_name (str): The name of the new collection to use.
        """
        try:
            self.collection_name = collection_name
            self.collection = self.db[collection_name]
            print(f"Collection set to: {collection_name}")
        except Exception as e:
            print(f"Error setting collection: {e}")
            self.collection = None

    def insert_one(self, data):
        """
        Inserts a single document into the current collection.

        Args:
            data (dict): A dictionary representing the document to be inserted.
        """
        if self.collection is not None:
            try:
                result = self.collection.insert_one(data)
                print(f"Insert successful, ID: {result.inserted_id}")
            except Exception as e:
                print(f"Error inserting data: {e}")
        else:
            print("No collection found to insert.")

    def insert_many(self, data):
        """
        Inserts multiple documents into the current collection.

        Args:
            data (list): A list of dictionaries, each representing a document to be inserted.
        """
        if self.collection is not None:
            try:
                result = self.collection.insert_many(data)
                print(f"Insert successful, IDs: {result.inserted_ids}")
            except Exception as e:
                print(f"Error inserting data: {e}")
        else:
            print("No collection found to insert.")

    def delete_one(self, query):
        """
        Deletes a single document that matches the given query.

        Args:
            query (dict): The filter query to match documents to delete.
        """
        if self.collection is not None:
            try:
                result = self.collection.delete_one(query)
                print(f"Delete successful, Deleted count: {result.deleted_count}")
            except Exception as e:
                print(f"Error deleting data: {e}")
        else:
            print("No collection found to delete.")

    def delete_many(self, query):
        """
        Deletes multiple documents that match the given query.

        Args:
            query (dict): The filter query to match documents to delete.
        """
        if self.collection is not None:
            try:
                result = self.collection.delete_many(query)
                print(f"Delete successful, Deleted count: {result.deleted_count}")
            except Exception as e:
                print(f"Error deleting data: {e}")
        else:
            print("No collection found to delete.")

    def update_one(self, query, new_values):
        """
        Updates a single document that matches the given query.

        Args:
            query (dict): The filter query to match documents to update.
            new_values (dict): The values to update in the matched document.
        """
        if self.collection is not None:
            try:
                result = self.collection.update_one(query, {"$set": new_values})
                print(f"Update successful, Matched count: {result.matched_count}, Modified count: {result.modified_count}")
            except Exception as e:
                print(f"Error updating data: {e}")
        else:
            print("No collection found to update.")

    def update_many(self, query, new_values):
        """
        Updates multiple documents that match the given query.

        Args:
            query (dict): The filter query to match documents to update.
            new_values (dict): The values to update in the matched documents.
        """
        if self.collection is not None:
            try:
                result = self.collection.update_many(query, {"$set": new_values})
                print(f"Update successful, Matched count: {result.matched_count}, Modified count: {result.modified_count}")
            except Exception as e:
                print(f"Error updating data: {e}")
        else:
            print("No collection found to update.")

    def select(self, filter=None, projection=None):
        """
        Selects documents from the current collection.

        Args:
            filter (dict, optional): The filter query to match documents to select (default is all).
            projection (dict, optional): The fields to include or exclude (default is all).

        Returns:
            list: A list of matched documents.
        """
        if self.collection is not None:
            try:
                if filter is None:
                    filter = {}
                if projection is None:
                    projection = {}
                results = self.collection.find(filter, projection)
                return list(results)
            except Exception as e:
                print(f"Error selecting data: {e}")
                return []
        else:
            print("No collection found to select.")

    def truncate(self):
        """
        Truncates the current collection by deleting all documents.
        """
        if self.collection is not None:
            try:
                result = self.collection.delete_many({})
                print(f"Successfully truncated collection. Deleted {result.deleted_count} documents.")
            except Exception as e:
                print(f"Error truncating collection: {e}")
        else:
            print("No collection found to truncate.")

    def delete_duplicates_with_condition(self, key_fields, condition):
        """
        Deletes duplicate documents in the current collection based on key fields and a given condition.

        Args:
            key_fields (list): List of fields used to identify duplicates.
            condition (dict): Condition to filter documents for consideration of deletion.

        Example usage for operators:
            $gte >=, $gt >, $lt <, $lte <=
        """
        try:
            pipeline = [
                {"$match": condition},
                {"$group": {
                    "_id": {field: f"${field}" for field in key_fields},
                    "count": {"$sum": 1},
                    "ids": {"$push": "$_id"}
                }},
                {"$match": {
                    "count": {"$gt": 1}
                }}
            ]

            duplicates = list(self.collection.aggregate(pipeline))

            for doc in duplicates:
                ids = doc['ids']
                keep_id = ids.pop(0)  # Keep the first document and delete the rest
                self.collection.delete_many({"_id": {"$in": ids}})

            print(f"Deleted {len(duplicates)} duplicate groups based on the condition.")
        except Exception as e:
            print(f"Error deleting duplicates: {e}")

# Example usage
if __name__ == "__main__":
    db = MongoDB(dbname='your_db', collection_name='your_collection', host='localhost', port=27017, username='your_user', password='your_pass')
    db.connect()

    # Set Collection
    db.set_collection('new_collection_name')

    # Insert One
    db.insert_one({'field1': 'value1', 'field2': 'value2'})

    # Insert Many
    db.insert_many([
        {'field1': 'value1', 'field2': 'value2'},
        {'field1': 'value3', 'field2': 'value4'}
    ])

    # Delete One
    db.delete_one({'field1': 'value1'})

    # Delete Many
    db.delete_many({'field1': 'value1'})

    # Update One
    db.update_one({'field1': 'value3'}, {'field2': 'updated_value'})

    # Update Many
    db.update_many({'field1': 'value3'}, {'field2': 'updated_value_all'})

    # Select
    results = db.select({'field2': 'value2'})
    print(results)

    # Truncate Collection
    db.truncate()

    # Delete Duplicates with Condition
    db.delete_duplicates_with_condition(key_fields=['field1'], condition={"field2": {"$exists": True}})

    # Close Connection
    db.close()
