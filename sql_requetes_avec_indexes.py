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
        SELECT DISTINCT pr.pid
        FROM principals pr
        JOIN movies m ON pr.mid = m.mid
        JOIN ratings r ON m.mid = r.mid
        WHERE m.startYear < 2009 AND r.numVotes < 200000
        EXCEPT
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


def creeIndexes():
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    index_queries = [
        # indexes pour 1ere requete
        "CREATE INDEX IF NOT EXISTS idx_movies_mid ON movies(mid);",
        "CREATE INDEX IF NOT EXISTS idx_characters_mid ON characters(mid);",
        "CREATE INDEX IF NOT EXISTS idx_characters_pid ON characters(pid);",
        "CREATE INDEX IF NOT EXISTS idx_persons_pid ON persons(pid);",
        "CREATE INDEX IF NOT EXISTS idx_persons_primaryName ON persons(primaryName);",
        # indexes pour 2eme requete
        "CREATE INDEX IF NOT EXISTS idx_genres_mid ON genres(mid);",
        "CREATE INDEX IF NOT EXISTS idx_ratings_mid ON ratings(mid);",
        "CREATE INDEX IF NOT EXISTS idx_movies_startYear ON movies(startYear);",
        # indexes pour 3eme requete
        "CREATE INDEX IF NOT EXISTS idx_writers_pid ON writers(pid);",
        "CREATE INDEX IF NOT EXISTS idx_writers_mid ON writers(mid);",
        "CREATE INDEX IF NOT EXISTS idx_titles_mid ON titles(mid);",
        "CREATE INDEX IF NOT EXISTS idx_titles_region ON titles(region);",
        # indexe pour 4eme requete
        "CREATE INDEX IF NOT EXISTS idx_characters_pid_mid ON characters(pid, mid);",
        # indexes pour 5eme requete
        "CREATE INDEX IF NOT EXISTS idx_principals_pid_mid ON principals(pid, mid);",
        "CREATE INDEX IF NOT EXISTS idx_movies_primaryTitle ON movies(primaryTitle);",
        "CREATE INDEX IF NOT EXISTS idx_ratings_numVotes ON ratings(numVotes);"
    ]

    try:
        for query in index_queries:
            cursor.execute(query)
        conn.commit()
        print("Indexes created successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred while creating indexes: {e}")
    finally:
        cursor.close()
        conn.close()


def requetes():
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
    creeIndexes()
    requetes()

main()
