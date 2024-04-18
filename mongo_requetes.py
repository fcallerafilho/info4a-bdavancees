from pymongo import MongoClient

def jeanRenoMovies():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']

    jean_reno = db.persons.find_one({'primaryName': 'Jean Reno'})
    jean_reno_pid = jean_reno['pid']

    mids_cursor = db.principals.find({'pid': jean_reno_pid}, {'mid': 1, '_id': 0})
    movie_ids = [doc['mid'] for doc in mids_cursor]

    films = db.movies.find({'mid': {'$in': movie_ids}}, {'primaryTitle': 1, '_id': 0})

    for film in films:
        print(film)

    client.close()

jeanRenoMovies()
