import sqlite3
import pymongo

client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client["imdb"]
collection = db.movies
sqlite_db_path = "database.db"

pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'replace', 'delete']}}}]
cursor = collection.watch(pipeline=pipeline, full_document='updateLookup')

sqlite_conn = sqlite3.connect(sqlite_db_path)
sqlite_cursor = sqlite_conn.cursor()

try:
    for change in cursor:
        operation_type = change['operationType']
        document = change['fullDocument']

        if operation_type == 'insert' or operation_type == 'replace':
            values = (
                document['mid'],
                document.get('titleType', ''),
                document.get('originalTitle', ''),
                document.get('isAdult', 0),
                document.get('startYear', 0),
                document.get('endYear', None),
                document.get('runtimeMinutes', 0),
                )

            sqlite_cursor.execute('''
                INSERT OR REPLACE INTO movies (mid, titleType, originalTitle, isAdult, startYear, endYear, runtimeMinutes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', values)

        elif operation_type == 'delete':
            sqlite_cursor.execute('''
                DELETE FROM movies WHERE mid=?
            ''', (str(document['mid']),))

        sqlite_conn.commit()

except Exception as e:
    print(f"error: {e}")

finally:
    cursor.close()
    sqlite_conn.close()