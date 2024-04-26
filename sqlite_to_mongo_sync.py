from ctypes import *
import sqlite3
import pymongo

SQLITE_DELETE =  9
SQLITE_INSERT = 18
SQLITE_UPDATE = 23

client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["imdb"]

sql_db_path = "database.db"

objectIdList = "tt8174510"
idName = "mid = "
pendingList = []
pendingListUpdate = []
query = "INSERT INTO movies (mid, primaryTitle, startYear) VALUES ('tt9999999', 'Example Movie', 2021)"

def create_connection(database_path):
    try:
        conn = sqlite3.connect(database_path)
        return conn
    except sqlite3.Error as e:
        print(e)
    return None

def callback(operation, db_name, table_name):
    if operation == SQLITE_DELETE:
        for objectId in objectIdList.split(db_name):
            conn = create_connection(sql_db_path)
            if conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                table_name = "%s" % (table_name)
                table_name = table_name[2:-1]
                query = "SELECT * FROM " + table_name + " WHERE " + idName + "'" + objectId + "'" 
                cursor.execute(query)
                row = cursor.fetchone()
                if row:
                    d = dict(zip(row.keys(), row))
                    print(d)
                    collection =database[table_name]
                    collection.delete_one(d)
                conn.close()

    elif operation == SQLITE_INSERT:
        for objectId in objectIdList.split(','):
            pendingList.append((objectId, table_name))

    elif operation == SQLITE_UPDATE:
        for objectId in objectIdList.split(','):
            pendingListUpdate.append((objectId, table_name))

def insert_pending():
    for objectId, table_name in pendingList:
        conn = create_connection(sql_db_path)
        if conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            table_name = "%s" % (table_name)
            table_name = table_name[2:-1]
            query = "SELECT * FROM " + table_name + " WHERE " + idName + "'" + objectId + "'" 
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                d = dict(zip(row.keys(), row))
                print(d)
                collection = database[table_name]
                collection.insert_one(d)
            conn.close()

def insert_pending_update():
    for objectId, table_name in pendingListUpdate:
        conn = create_connection(sql_db_path)
        if conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            table_name = "%s" % (table_name)
            table_name = table_name[2:-1]
            query = "SELECT * FROM " + table_name + " WHERE " + idName + "'" + objectId + "'" 
            cursor.execute(query)
            row = cursor.fetchone()
            if row:
                d = dict(zip(row.keys(), row))
                print(d)
                collection = database[table_name]
                collection.update_one(d)
            conn.close()

c_callback = CFUNCTYPE(c_void_p, c_void_p, c_int, c_char_p, c_char_p, c_int64)(callback)

dll = CDLL('./sqlite3.dll')
db = c_void_p()

dll.sqlite3_open(b'database.db', byref(db))
dll.sqlite3_update_hook(db, c_callback, None)

err = c_char_p()
dll.sqlite3_exec(db, query, None, None, byref(err))
if err:
    print(err.value)

insert_pending()
insert_pending_update()

dll.sqlite3_close(db)
