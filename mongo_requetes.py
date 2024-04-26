from pymongo import MongoClient
import time

def jeanRenoMovies():
    timeBegin = time.time()

    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']

    jean_reno = db.persons.find_one({'primaryName': 'Jean Reno'})
    jean_reno_pid = jean_reno['pid']

    mids_cursor = db.principals.find({'pid': jean_reno_pid}, {'mid': 1, '_id': 0})
    movie_ids = [doc['mid'] for doc in mids_cursor]

    films = db.movies.find({'mid': {'$in': movie_ids}}, {'primaryTitle': 1, '_id': 0})

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for film in films:
        print(film)
    print("Execution time: {:f}".format(totalTime))

    client.close()

def troisMeilleursFilmsHorreur2000():
    timeBegin = time.time()

    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']

    horror_movie_ids = db.genres.find(
        {'genre': 'Horror'},
        {'_id': 0, 'mid': 1}
    )

    horror_movie_ids_list = [movie['mid'] for movie in horror_movie_ids]

    movies_2000s = db.movies.find(
        {
            'mid': {'$in': horror_movie_ids_list},
            'startYear': {'$gte': 2000, '$lt': 2010}
        },
        {'_id': 0, 'mid': 1}
    )

    movie_ids_2000s_list = [movie['mid'] for movie in movies_2000s]

    top_rated_horror_movies = db.ratings.find(
        {'mid': {'$in': movie_ids_2000s_list}},
        {'_id': 0, 'mid': 1, 'averageRating': 1}
    ).sort('averageRating', -1).limit(3)

    top_rated_movies_list = list(top_rated_horror_movies)

    top_movies = []
    for movie_rating in top_rated_movies_list:
        movie_details = db.movies.find_one(
            {'mid': movie_rating['mid']},
            {'_id': 0, 'primaryTitle': 1}
        )
        movie_details['averageRating'] = movie_rating['averageRating']
        top_movies.append(movie_details)

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for movie in top_movies:
        print(movie)
    print("Execution time: {:f}".format(totalTime))

    client.close()

def scenaristesFilmsJamaisJouesEspagne():
    timeBegin = time.time()
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb'] 

    movies_in_spain = db.titles.find({"region" : "ES"})
    movie_ids_in_spain = [movie['mid'] for movie in movies_in_spain]

    writers_for_movies_in_es = db.writers.find({"mid": {"$in": movie_ids_in_spain}})
    
    pids_in_spain = [writer['pid'] for writer in writers_for_movies_in_es]
    writers_for_movies_not_es = db.writers.find({"pid": {"$nin": pids_in_spain}})

    writer_pids = set(writer['pid'] for writer in writers_for_movies_not_es) 
    
    scenaristes = db.persons.find({"pid": {"$in": list(writer_pids)}})

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    # contrôle de l'affichage
    for i, scenariste in enumerate(scenaristes):
        if i < 10:
            print(scenariste)
        else:
            break

    print("Execution time: {:f}".format(totalTime))

    client.close()

def acteursPlusDeRolesDansUnFilm():
    timeBegin = time.time()
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb'] 

    characters = db.characters.find()
    persons = {person['pid']: person['primaryName'] for person in db.persons.find()}

    role_counts = {}

    for character in characters:
        pid = character['pid']
        mid = character['mid']
        role_name = character['name']
        key = (pid, mid)

        if key not in role_counts:
            role_counts[key] = {
                'name': persons.get(pid, 'Unknown'),
                'roles': set()
            }
        
        role_counts[key]['roles'].add(role_name)

    final_results = [{'primaryName': info['name'], 'totalRoles': len(info['roles'])} for key, info in role_counts.items()]

    final_results.sort(key=lambda x: x['totalRoles'], reverse=True)
    top_5_actors = final_results[:5]

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for actor in top_5_actors:
        print(actor)

    print("Execution time: {:f}".format(totalTime))

    client.close()

def fetch_distinct_pids(filter):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']
    return [document['pid'] for document in db.principals.find(filter)]

def carrierePropulseeParAvatar():
    timeBegin = time.time()
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']

    avatar_actors = fetch_distinct_pids({'mid': {'$in': [m['mid'] for m in db.movies.find({'primaryTitle': 'Avatar'})]}})

    # Actors known before 2009 with more than 200,000 votes
    mb2009_ids = [movie['mid'] for movie in db.movies.find({'startYear': {'$lt': 2009}})]
    mb2009etvotes_ids = [rating['mid'] for rating in db.ratings.find({'numVotes': {'$gt': 200000}, 'mid': {'$in': mb2009_ids}})]
    high_vote_actors_before_2009 = [document['pid'] for document in db.principals.find({'mid': {'$in': mb2009etvotes_ids}})]

    # Actors known after 2009 with more than 200,000 votes
    mb2009_ids = [movie['mid'] for movie in db.movies.find({'startYear': {'$gt': 2009}})]
    mb2009etvotes_ids = [rating['mid'] for rating in db.ratings.find({'numVotes': {'$gt': 200000}, 'mid': {'$in': mb2009_ids}})]
    high_vote_actors_after_2009 = [document['pid'] for document in db.principals.find({'mid': {'$in': mb2009etvotes_ids}})]

    before_high_vote_actors_set = set(high_vote_actors_before_2009)
    high_vote_actors_set = set(high_vote_actors_after_2009)

    # set intersection 
    not_known_before_2009_known_after = list(high_vote_actors_set - before_high_vote_actors_set)

    # Intersection of all sets
    avatar_actors_set = set(avatar_actors)
    not_known_before_2009_known_after_set = set(not_known_before_2009_known_after)

    # Compute the intersection of the three sets
    qualified_actors = avatar_actors_set & not_known_before_2009_known_after_set

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    qualified_actors = list(qualified_actors)

    for actor in qualified_actors:
        actor = db.persons.find_one({'pid': actor})
        print(actor)

    print("Execution time: {:f}".format(totalTime))

    client.close()

def jeanRenoSurDonnesStructurees():
    timeBegin = time.time()
    client = MongoClient('mongodb://localhost:27017/')
    db = client['imdb']

    jean_reno = db.aggregated_films.find({'persons.primaryName': 'Jean Reno'}, {'movie.primaryTitle': 1})

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for film in jean_reno:
        print(film['movie'])

    print("Execution time: {:f}".format(totalTime))

    client.close()

def main():
    print("\nTemps d'execution sur MongoDB:")

    print("\nFilmes de Jean Reno:")
    jeanRenoMovies()

    print("\nTrois meilleurs filmes d'erreurs de 2000 jusqu'a 2009:")
    troisMeilleursFilmsHorreur2000()

    print("\nScénaristes qui n'ont jamais écrit un filme qui a été joué en espagne:")
    scenaristesFilmsJamaisJouesEspagne()

    print("\nActeurs qui ont joué dans plusieurs rôles au même filme:")
    acteursPlusDeRolesDansUnFilm()

    print("\nPersonnes avec carrière propulsée par Avatar:")
    carrierePropulseeParAvatar()

    print("\nJean Reno sur données structurées:")
    jeanRenoSurDonnesStructurees()

main()