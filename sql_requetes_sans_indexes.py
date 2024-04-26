import sqlite3
import time

sqlite_db_path = 'database.db'

def jeanRenoMovies():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT DISTINCT primaryTitle FROM movies
    JOIN characters ON movies.mid = characters.mid
    JOIN persons ON characters.pid = persons.pid
    WHERE primaryName='Jean Reno'
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

def troisMeilleursFilmsHorreur2000():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT m.primaryTitle
    FROM movies m
    JOIN genres g ON m.mid = g.mid
    JOIN ratings r ON m.mid = r.mid
    WHERE g.genre = 'Horror' AND m.startYear BETWEEN 2000 AND 2009
    GROUP BY m.primaryTitle
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
    SELECT DISTINCT COUNT(primaryName) FROM persons
    JOIN writers ON writers.pid = persons.pid
    JOIN titles ON titles.mid = writers.mid
    WHERE titles.mid NOT in(
        SELECT mid
        FROM titles
        WHERE region='ES'
    )
    """

    cursor.execute(query)

    scenaristes = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    print(scenaristes[0])
            
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

# Quels acteurs ont joué le plus de rôles différents dans un même film ?
def acteursPlusDeRolesDansUnFilm():
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    timeBegin = time.time()

    query = """
    SELECT DISTINCT persons.primaryName, COUNT (DISTINCT characters.name) AS totalRoles
    FROM characters
    JOIN persons ON characters.pid = persons.pid
    GROUP BY characters.pid, characters.mid
    ORDER BY totalRoles DESC
    LIMIT 5;
    """

    cursor.execute(query)

    acteurs = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin

    for acteur in acteurs:
        print(acteur[0], acteur[1])

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
    WITH AvatarActors AS (
        SELECT DISTINCT pr.pid
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        WHERE m.primaryTitle = 'Avatar'
    ),
    NotKnownBefore2009 AS (
        -- Select distinct PIDs not appearing in any high-vote movies before 2009
        SELECT DISTINCT pr.pid
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE m.startYear < 2009 AND r.numVotes < 200000
        EXCEPT
        -- Exclude PIDs who appeared in high-vote movies before 2009
        SELECT DISTINCT pr.pid
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE m.startYear < 2009 AND r.numVotes > 200000
    ),
    KnownAfter2009 AS (
        SELECT DISTINCT pr.pid
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE m.startYear > 2009 AND r.numVotes > 200000
    ),
    QualifiedActors AS (
        SELECT pid FROM AvatarActors
        INTERSECT
        SELECT pid FROM NotKnownBefore2009
        INTERSECT
        SELECT pid FROM KnownAfter2009
    )
    SELECT p.primaryName
    FROM QualifiedActors
    JOIN persons p ON QualifiedActors.pid = p.pid;
    """

    cursor.execute(query)

    personnes = cursor.fetchall()

    timeEnd = time.time()
    totalTime = timeEnd - timeBegin 

    for personne in personnes:
        print(personne[0])
    print("Execution time: {:f}".format(totalTime))

    cursor.close()
    conn.close()

def supprimeIndexes(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    index_names = [
        'idx_movies_mid',
        'idx_characters_mid',
        'idx_characters_pid',
        'idx_persons_pid',
        'idx_persons_primaryName',
        'idx_genres_mid',
        'idx_ratings_mid',
        'idx_movies_startYear',
        'idx_writers_pid'
        'idx_writers_mid',
        'idx_titles_mid',
        'idx_title_region'
        'idx_characters_pid_mid',
        'idx_principals_pid_mid',
        'idx_movies_primaryTitle',
        'idx_ratings_numVotes',
    ]

    for index_name in index_names:
        try:
            cursor.execute(f'DROP INDEX IF EXISTS {index_name}')
            print(f"Index {index_name} deleted successfully.")
        except sqlite3.OperationalError as e:
            print(f"Error deleting index {index_name}: {e}")

    # pour supprimer tous les indexes qui peuvent exister à cause des changements sur les requêtes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' AND sql IS NOT NULL;")
    indexes = cursor.fetchall()

    # Drop each index
    for index in indexes:
        index_name = index[0]  # Extract the index name
        drop_statement = f"DROP INDEX IF EXISTS {index_name};"
        cursor.execute(drop_statement)
        print(f"Dropped index: {index_name}")

    conn.commit()
    conn.close()

def requetes():
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

def main():
    supprimeIndexes('database.db')
    requetes()
    
main()

# sur sqlite3 dans le terminal, SCAN et SEARCH sont les deux types d'opérations qui sont utilisées pour les requêtes SQL.
# SCAN est utilisé pour les requêtes qui scannent l'intégralité de la table, tandis que SEARCH est utilisé pour les requêtes qui utilisent un index.
# le but est de ne pas utiliser SCAN, mais SEARCH pour optimiser les requêtes.
# Pour la troisieme requete, on n'a pas arrive n'utiliser que des SEARCH
# EXPLAIN QUERY PLAN SELECT ...