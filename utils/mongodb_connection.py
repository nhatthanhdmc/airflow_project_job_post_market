from pymongo import MongoClient

class MongoDB:
    """
        Connect to a MongoDB database and perform CRUD (Create, Read, Update, Delete) operations, including truncation
    """
    def __init__(self, dbname, collection_name, host='localhost', port=27017, username=None, password=None):
        try:
            self.dbname = dbname
            self.collection_name = collection_name
            self.host = host
            self.port = port
            self.username = username
            self.password = password
            self.client = None
            self.db = None
            self.collection = None
            self.maxPoolSize=10
            self.minPoolSize=5
            self.connectTimeoutMS=30000
            self.socketTimeoutMS=30000
        except Exception as e:
            print(f"Error connecting to MongoDB: {e}")

    def connect(self):
        try:
            if self.username and self.password:
                self.client = MongoClient(self.host, self.port, username=self.username, password=self.password
                                          , maxPoolSize=self.maxPoolSize, minPoolSize=self.minPoolSize
                                          , connectTimeoutMS=self.connectTimeoutMS, socketTimeoutMS=self.socketTimeoutMS
                                          )
            else:
                self.client = MongoClient(self.host, self.port
                                          , maxPoolSize=self.maxPoolSize, minPoolSize=self.minPoolSize
                                          , connectTimeoutMS=self.connectTimeoutMS, socketTimeoutMS=self.socketTimeoutMS)
            self.db = self.client[self.dbname]
            self.collection = self.db[self.collection_name]
            print("Connection successful")
        except Exception as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        if self.client:
            self.client.close()
        print("Connection closed")

    def set_collection(self, collection_name):
        """
        Set a new collection for the MongoDB instance
        """
        try:
            self.collection_name = collection_name
            self.collection = self.db[collection_name]
            print(f"Collection set to: {collection_name}")
        except Exception as e:
            print(f"Error setting collection: {e}")
            self.collection = None
                
    def insert_one(self, data):
        if self.collection is not None:
            try:
                result = self.collection.insert_one(data)
                print(f"Insert successful, ID: {result.inserted_id}")
            except Exception as e:
                print(f"Error inserting data: {e}")
        else:
            print("No collection found to insert.")  
            
    def insert_many(self, data):
        if self.collection is not None:
            try:
                result = self.collection.insert_many(data)
                print(f"Insert successful, ID: {result.inserted_ids}")
            except Exception as e:
                print(f"Error inserting data: {e}")
        else:
            print("No collection found to insert.")       

    def delete_one(self, query):
        if self.collection is not None:
            try:
                result = self.collection.delete_one(query)
                print(f"Delete successful, Deleted count: {result.deleted_count}")
            except Exception as e:
                print(f"Error deleting data: {e}")
        else:
            print("No collection found to delete.")
            
    def delete_many(self, query):
        if self.collection is not None:
            try:
                result = self.collection.delete_many(query)
                print(f"Delete successful, Deleted count: {result.deleted_count}")
            except Exception as e:
                print(f"Error deleting data: {e}")
        else:
            print("No collection found to delete.")   

    def update_one(self, query, new_values):
        if self.collection is not None:
            try:
                result = self.collection.update_one(query, {"$set": new_values})
                print(f"Update successful, Matched count: {result.matched_count}, Modified count: {result.modified_count}")
            except Exception as e:
                print(f"Error updating data: {e}")
        else:
            print("No collection found to update.")
            
    def update_many(self, query, new_values):
        if self.collection is not None:
            try:
                result = self.collection.update_many(query, {"$set": new_values})
                print(f"Update successful, Matched count: {result.matched_count}, Modified count: {result.modified_count}")
            except Exception as e:
                print(f"Error updating data: {e}")
        else:
            print("No collection found to update.")
            
    def select(self, filter=None, projection=None):
        if self.collection is not None:
            try:
                if filter is None:
                    filter = {}
                if projection is None:
                    projection = {}
                print(filter)
                print(projection)
                results = self.collection.find(filter, projection)
                return list(results)
            except Exception as e:
                print(f"Error selecting data: {e}")
                return []
        else:
            print("No collection found to select.")        

    def truncate(self):
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
        Delete duplicates in the collection based on key fields and an additional condition.
        :param key_fields: List of fields to identify duplicates.
        :param condition: Dictionary representing the additional condition for filtering documents.
        $gte >=
        $gt >
        $lt <
        $lte <=
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
                # Keep the first document and delete the rest
                keep_id = ids.pop(0)
                self.collection.delete_many({"_id": {"$in": ids}})

            print(f"Deleted {len(duplicates)} duplicate groups based on the condition.")
        except Exception as e:
            print(f"Error deleting duplicates: {e}")
"""
# Example usage:
if __name__ == "__main__":
    db = MongoDB(dbname='your_db', collection_name='your_collection', host='your_host', port=your_port, username='your_user', password='your_pass')
    db.connect()

    # Insert example
    db.insert({'field1': 'value1', 'field2': 'value2'})

    # Update example
    db.update({'field1': 'value1'}, {'field2': 'new_value2'})

    # Delete example
    db.delete({'field1': 'value1'})

    # Select example
    results = db.select({'field2': 'value2'})
    print(results)

    # Truncate example
    db.truncate()

    db.close()
    
"""