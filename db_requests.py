import sqlite3

# path to our SQLite database
sqlite_db_path = 'database.db'

# connect to the SQLite database
conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()

def jeanRenoMovies():
    # replace the table and column names according to your database schema
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

    # print the movie titles
    for film in films:
        print(film[0])

    # close the cursor and connection
    cursor.close()
    conn.close()

def troisMeilleursFilmsHorreur2000():
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

    for film in films:
        print(film[0])

    cursor.close()
    conn.close()

def scenaristesFilmsJamaisJouesEspagne():
    query = """
    SELECT p.primaryName
    FROM persons p
    WHERE p.pid NOT IN (
        SELECT p.pid
        FROM persons p
        JOIN principals pr ON p.pid = pr.pid
        JOIN movies m ON pr.mid = m.mid
        JOIN genres g ON m.mid = g.mid
        WHERE g.genres LIKE '%Drama%' AND m.country = 'Spain'
    )
    """

    cursor.execute(query)

    films = cursor.fetchall()

    for film in films:
        print(film[0])

    cursor.close()
    conn.close()

def main():
    scenaristesFilmsJamaisJouesEspagne()

main()
