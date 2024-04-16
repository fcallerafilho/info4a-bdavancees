from pymongo import MongoClient
import sqlite3

sqlite_db_path = 'database.db'
mongo_db_uri = 'mongodb://localhost:27017/'
mongo_db_name = 'imdb'

def importCharacters():
    mongo_collection_name = 'characters'
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, pid, name FROM characters")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'pid': row[1],
            'name': row[2]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importDirectors():
    mongo_collection_name = 'directors'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, pid FROM directors")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'pid': row[1]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importEpisodes():
    mongo_collection_name = 'episodes'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, parentMid, seasonNumber, episodeNumber FROM episodes")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'parentMid': row[1],
            'seasonNumber': row[2],
            'episodeNumber': row[3]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importGenres():
    mongo_collection_name = 'genres'
    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, genre FROM genres")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'genre': row[1]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importKnownformovies():
    mongo_collection_name = 'knownformovies'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT pid, mid FROM knownformovies")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'pid': row[0],
            'mid': row[1]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()


def importMovies():
    mongo_collection_name = 'movies'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    sqlite_cursor.execute("SELECT * FROM movies")
    rows = sqlite_cursor.fetchall()

    columns = ['mid', 'titleType', 'primaryTitle', 'originalTitle', 'isAdult', 'startYear', 'endYear', 'runtimeMinutes']

    for row in rows:
        document = {columns[i]: row[i] for i in range(len(columns))}
        document['isAdult'] = bool(document['isAdult'])
        document['startYear'] = int(document['startYear']) if document['startYear'] else None
        document['endYear'] = int(document['endYear']) if document['endYear'] else None
        document['runtimeMinutes'] = int(document['runtimeMinutes']) if document['runtimeMinutes'] else None

    mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()
    mongo_client.close()

def importPersons():
    mongo_collection_name = 'persons'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT pid, primaryName, birthYear, deathYear FROM persons")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'pid': row[0],
            'primaryName': row[1],
            'birthYear': row[2] if row[2] else None,
            'deathYear': row[3] if row[3] else None
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importPrincipals():
    mongo_collection_name = 'principals'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, ordering, pid, category, job FROM principals")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'ordering': row[1],
            'pid': row[2],
            'category': row[3],
            'job': row[4] if row[4] else None
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importProfessions():
    mongo_collection_name = 'professions'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT pid, jobName FROM professions")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'pid': row[0],
            'jobName': row[1]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importRatings():
    mongo_collection_name = 'ratings'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, averageRating, numVotes FROM ratings")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'averageRating': float(row[1]),
            'numVotes': int(row[2])
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importTitles():
    mongo_collection_name = 'titles'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, ordering, title, region, language, types, attributes, isOriginalTitle FROM titles")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'ordering': row[1],
            'title': row[2],
            'region': row[3],
            'language': row[4],
            'types': row[5],
            'attributes': row[6],
            'isOriginalTitle': bool(row[7])
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def importWriters():
    mongo_collection_name = 'writers'

    sqlite_conn = sqlite3.connect(sqlite_db_path)
    sqlite_cursor = sqlite_conn.cursor()

    sqlite_cursor.execute("SELECT mid, pid FROM writers")
    rows = sqlite_cursor.fetchall()

    mongo_client = MongoClient(mongo_db_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    mongo_collection.drop()

    for row in rows:
        document = {
            'mid': row[0],
            'pid': row[1]
        }
        mongo_collection.insert_one(document)

    sqlite_cursor.close()
    sqlite_conn.close()

    mongo_client.close()

def main():
    importCharacters()
    importDirectors()
    importEpisodes()
    importGenres()
    importKnownformovies()
    importMovies()
    importPersons()
    importPrincipals()
    importProfessions()
    importRatings()
    importTitles()
    importWriters()

main()