from pymongo import MongoClient

mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['imdb']

characters_collection = mongo_db['characters']
directors_collection = mongo_db['directors']
episodes_collection = mongo_db['episodes']
genres_collection = mongo_db['genres']
knownformovies_collection = mongo_db['knownformovies']
movies_collection = mongo_db['movies']
persons_collection = mongo_db['persons']
principals_collection = mongo_db['principals']
professions_collection = mongo_db['professions']
ratings_collection = mongo_db['ratings']
titles_collection = mongo_db['titles']
writers_collection = mongo_db['writers']

aggregated_films_collection = mongo_db['aggregated_films']
aggregated_films_collection.drop()

persons_with_movies_collection = mongo_db['persons_with_movies']
persons_with_movies_collection.drop()

for person in persons_collection.find():
    movies_cursor = principals_collection.find({'pid': person['pid']}, {'_id': 0, 'mid': 1})
    movie_ids = [movie['mid'] for movie in movies_cursor]  # Extract just the movie IDs

    person_with_movies = person.copy()  
    person_with_movies['movie_ids'] = movie_ids

    persons_with_movies_collection.insert_one(person_with_movies)
    print('a')

for movie in movies_collection.find():
    aggregated_film = {
        'movie': movie,
        'characters': list(characters_collection.find({'mid': movie['mid']})),
        #'directors': list(directors_collection.find({'mid': movie['mid']})),
        #'episodes': list(episodes_collection.find({'parentMid': movie['mid']})),
        #'genres': list(genres_collection.find({'mid': movie['mid']})),
        #'knownformovies': list(knownformovies_collection.find({'mid': movie['mid']})),
        'persons': list(persons_with_movies_collection.find({'mid': movie['mid']})),
        #'principals': list(principals_collection.find({'mid': movie['mid']})),
        'ratings': list(directors_collection.find({'mid': movie['mid']})),
        'titles': list(titles_collection.find({'mid': movie['mid']})),
        #'writers': list(titles_collection.find({'mid': movie['mid']}))
    }

    aggregated_films_collection.insert_one(aggregated_film)
    print('b')

mongo_client.close()
