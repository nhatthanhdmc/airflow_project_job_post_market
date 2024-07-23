from pymongo import MongoClient 

from utils.mongodb_connection import MongoDB

from utils import config as cfg
mongo_conn = cfg.mongodb['CRAWLING']
mongodb = MongoDB(  dbname = mongo_conn['dbname'], 
                        collection_name = mongo_conn['cv_job_post_detail'],
                        host = mongo_conn['host'], 
                        port = mongo_conn['port'], 
                        username = mongo_conn['username'], 
                        password = mongo_conn['password']
                    )
mongodb.connect()    
def update_updated_date_with_created_date(collection_name):
    try:
        # Define the filter (empty filter matches all documents)
        filter = {}

        # Define the update operation using an aggregation pipeline
        update_operation = [{
            '$set': {
                'updated_date': '$created_date'
            }
        }]

        # Perform the update
        result = mongodb.update_many(filter, update_operation)

        print("Update completed successfully.")

    except Exception as e:
        print(f"Error updating data: {e}")

if __name__ == "__main__":
    # Example usage
    update_updated_date_with_created_date('your_collection_name')
