from pymongo import MongoClient
import pymongo
import time

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['imdb']

characters_collection = mongo_db['characters']
# directors_collection = mongo_db['directors']
episodes_collection = mongo_db['episodes']
genres_collection = mongo_db['genres']
# knownformovies_collection = mongo_db['knownformovies']
movies_collection = mongo_db['movies']
persons_collection = mongo_db['persons']
# principals_collection = mongo_db['principals']
# professions_collection = mongo_db['professions']
ratings_collection = mongo_db['ratings']
titles_collection = mongo_db['titles']
# writers_collection = mongo_db['writers']

aggregated_films_collection = mongo_db['aggregated_films']
aggregated_films_collection.drop()

batch_size = 100
cursor = movies_collection.find({}) 
batch = []

def ensure_indexes():
    mongo_db['movies'].create_index([('mid', pymongo.ASCENDING)], unique=True)    
    mongo_db['characters'].create_index([('mid', pymongo.ASCENDING)])
    mongo_db['genres'].create_index([('mid', pymongo.ASCENDING)])    
    mongo_db['persons'].create_index([('pid', pymongo.ASCENDING)])    
    mongo_db['ratings'].create_index([('mid', pymongo.ASCENDING)])
    mongo_db['titles'].create_index([('mid', pymongo.ASCENDING)])
    print("Indexes created successfully.")

def process_batch(batch):
    for movie in batch:
        characters_list = list(mongo_db['characters'].find({'mid': movie['mid']}))
        characters_pid_list = [character['pid'] for character in characters_list]
        aggregated_film = {
            'movie': movie,
            'characters': characters_list,
            'genres': list(mongo_db['genres'].find({'mid': movie['mid']})),
            'persons': list(mongo_db['persons'].find({'pid': {"$in": characters_pid_list}})),
            'ratings': list(mongo_db['ratings'].find({'mid': movie['mid']})),
            'titles': list(mongo_db['titles'].find({'mid': movie['mid']}))
        }
        aggregated_films_collection.insert_one(aggregated_film)
    print(f'Inserted {len(batch)} aggregated films')

ensure_indexes()

time_begin = time.time()

for movie in cursor:
    batch.append(movie)
    if len(batch) >= batch_size:
        process_batch(batch)
        batch = []

if batch:
    process_batch(batch)

time_end = time.time()
print(f"Execution time: {time_end - time_begin:.3f} seconds")

mongo_client.close()