from pymongo import MongoClient

def jeanRenoMovies27019():
    client = MongoClient('mongodb://localhost:27019/')
    db = client['imdb']

    jean_reno = db.persons.find_one({'primaryName': 'Jean Reno'})
    print(jean_reno)
    
def jeanRenoMovies27020():
    client = MongoClient('mongodb://localhost:27019/')
    db = client['imdb']

    jean_reno = db.persons.find_one({'primaryName': 'Jean Reno'})
    print(jean_reno)


jeanRenoMovies27019()
jeanRenoMovies27020()
