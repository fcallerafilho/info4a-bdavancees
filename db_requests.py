import sqlite3
import time

sqlite_db_path = 'database.db'

def jeanRenoMovies():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT m.originalTitle
    FROM movies m
    JOIN principals p ON m.mid = p.mid
    JOIN persons a ON p.pid = a.pid
    WHERE a.primaryName = 'Jean Reno'
    """

    # execute the query
    cursor.execute(query)

    # fetch all results
    films = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for film in films:
        print(film[0])
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

def troisMeilleursFilmsHorreur2000():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT m.originalTitle
    FROM movies m
    JOIN genres g ON m.mid = g.mid
    JOIN ratings r ON m.mid = r.mid
    WHERE g.genre = 'Horror' AND m.startYear BETWEEN 2000 AND 2009
    GROUP BY m.originalTitle
    ORDER BY averageRating DESC
    LIMIT 3
    """

    cursor.execute(query)

    films = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for film in films:
        print(film[0])
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

def scenaristesFilmsJamaisJouesEspagne(): # longue et difficile a optimiser
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT COUNT (w.pid)
    FROM writers w
    WHERE NOT EXISTS (
        SELECT 1
        FROM titles t
        WHERE w.mid = t.mid
        AND t.region = "ES"
    )
    """

    cursor.execute(query)

    scenaristes = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for scenariste in scenaristes:
        print(scenariste[0])
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

# Quels acteurs ont joué le plus de rôles différents dans un même film ?
def acteursPlusDeRolesDansUnFilm():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT DISTINCT p.primaryName, COUNT(*) as count
    FROM persons p
    JOIN principals pr ON p.pid = pr.pid
    GROUP BY pr.mid
    ORDER BY count DESC
    """

    cursor.execute(query)

    acteurs = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for i, acteur in enumerate(acteurs):
        if i < 5:
            print(acteur[0])
        else: # coupe de l'affichage 
            print("...") 
            break 

    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

# Quelles personnes ont vu leur carrière propulsée par Avatar (quel que soit leur métier), et alors
# qu’elles n’étaient pas connues avant (ayant seulement participé à des films avec moins de 200000
# votes avant avatar), elles sont apparues par la suite dans un film à succès (nombre de votes >
# 200000) ? Notez que les films ne sont pas ordonnés par date de sortie, et donc on utilisera l’année
# de sortie du film (strictement avant / après celle d’Avatar).

def carrierePropulseeParAvatar():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    WITH AvatarPersons AS (
        SELECT DISTINCT p.pid
        FROM principals AS p
        JOIN movies AS m ON p.mid = m.mid
        WHERE m.primaryTitle = 'Avatar' AND m.startYear = 2009
    ),
    BeforeAvatar AS (
        SELECT p.pid, COUNT(m.mid) AS MovieCount
        FROM principals AS p
        JOIN movies AS m ON p.mid = m.mid
        JOIN ratings AS r ON m.mid = r.mid
        WHERE m.startYear < 2009 AND r.numVotes < 200000
        GROUP BY p.pid
    ),
    AfterAvatarSuccess AS (
        SELECT p.pid, COUNT(m.mid) AS MovieCount
        FROM principals AS p
        JOIN movies AS m ON p.mid = m.mid
        JOIN ratings AS r ON m.mid = r.mid
        WHERE m.startYear > 2009 AND r.numVotes > 200000
        GROUP BY p.pid
    ),
    EligiblePersons AS (
        SELECT ap.pid
        FROM AvatarPersons AS ap
        JOIN AfterAvatarSuccess AS aas ON ap.pid = aas.pid
        LEFT JOIN BeforeAvatar AS ba ON ap.pid = ba.pid
        WHERE ba.pid IS NULL OR ba.MovieCount = 0
    )
    SELECT p.pid, p.primaryName, p.birthYear
    FROM EligiblePersons AS ep
    JOIN persons AS p ON ep.pid = p.pid;
    """

    cursor.execute(query)

    personnes = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin 

    for personne in personnes:
        print(personne[1])
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

def creeIndexes():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Jean Reno movies
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_persons_primaryName ON persons(primaryName)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_principals_pid_mid ON principals(pid, mid)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_mid ON movies(mid)')

    # Trois meilleurs filmes horreur de 2000 jusqu'a 2009
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_startYear ON movies(startYear);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_genres_genre ON genres(genre);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_averageRating ON ratings(averageRating);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_mid ON movies(mid);')

    # Scénaristes qui n'ont jamais écrit un filme qui a été joué en espagne
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_writers_mid ON writers(mid);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_titles_region_mid ON titles(region, mid);')

    # Acteurs qui ont joué dans plusieurs rôles au même filme
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_principals_mid ON principals(mid);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_persons_pid ON persons(pid);')

    # Personnes avec carrière propulsée par Avatar
    cursor.execute('CREATE INDEX idx_movies_primaryTitle_startYear ON movies(primaryTitle, startYear);')
    cursor.execute('CREATE INDEX idx_ratings_numVotes_mid ON ratings(numVotes, mid);')

    conn.commit()
    conn.close()

import sqlite3

def supprimeIndexes(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    index_names = [
        'idx_persons_primaryName',
        'idx_principals_pid_mid',
        'idx_movies_mid',
        'idx_movies_startYear',
        'idx_genres_genre',
        'idx_ratings_averageRating',
        'idx_movies_mid',
        'idx_writers_mid',
        'idx_titles_region_mid',
        'idx_principals_mid',
        'idx_persons_pid',
        'idx_movies_primaryTitle_startYear',
        'idx_ratings_numVotes_mid'
    ]

    for index_name in index_names:
        try:
            cursor.execute(f'DROP INDEX IF EXISTS {index_name}')
            print(f"Index {index_name} deleted successfully.")
        except sqlite3.OperationalError as e:
            print(f"Error deleting index {index_name}: {e}")

    conn.commit()
    conn.close()

def requetesSansIndexes():
    supprimeIndexes('database.db')

    print("\nTemps d'execution sans indexes:")

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

def requetesAvecIndexes():
    creeIndexes()

    print("\nTemps d'execution avec indexes:")

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

def main():
    requetesSansIndexes()
    requetesAvecIndexes()
    
# execute main
main()

# sur sqlite3 dans le terminal, SCAN et SEARCH sont les deux types d'opérations qui sont utilisées pour les requêtes SQL.
# SCAN est utilisé pour les requêtes qui scannent l'intégralité de la table, tandis que SEARCH est utilisé pour les requêtes qui utilisent un index.
# le but est de ne pas utiliser SCAN, mais SEARCH pour optimiser les requêtes.
# Pour la troisieme requete, on n'a pas arrive n'utiliser que des SEARCH
# EXPLAIN QUERY PLAN SELECT ...