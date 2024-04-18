import csv
import sqlite3

db_size = 'tiny'

def configCharacters():
    csv_file_path = 'imdb-' + db_size + '/characters.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS characters')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS characters (
            mid TEXT,
            pid TEXT,
            name TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO characters (mid, pid, name)
                VALUES (?, ?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configDirectors():
    csv_file_path = 'imdb-' + db_size + '/directors.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS directors')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS directors (
            mid TEXT,
            pid TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO directors (mid, pid)
                VALUES (?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configEpisodes():
    csv_file_path = 'imdb-' + db_size + '/episodes.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS episodes')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS episodes (
            mid TEXT,
            parentMid TEXT,
            seasonNumber NUMBER,
            episodeNumber NUMBER
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO episodes (mid, parentMid, seasonNumber, episodeNumber)
                VALUES (?, ?, ?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configGenres():
    csv_file_path = 'imdb-' + db_size + '/genres.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS genres')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS genres (
            mid TEXT,
            genre TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO genres (mid, genre)
                VALUES (?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configKnownformovies():
    csv_file_path = 'imdb-' + db_size + '/knownformovies.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS knownformovies')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS knownformovies (
            pid TEXT,
            mid TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO knownformovies (pid, mid)
                VALUES (?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configMovies():
    csv_file_path = 'imdb-' + db_size + '/movies.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS movies')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS movies (
            mid TEXT,
            titleType TEXT,
            primaryTitle TEXT,
            originalTitle TEXT,
            isAdult INTEGER,
            startYear INTEGER,
            endYear INTEGER,
            runtimeMinutes INTEGER
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO movies (mid, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configPersons():
    csv_file_path = 'imdb-' + db_size + '/persons.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS persons')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS persons (
            pid TEXT,
            primaryName TEXT,
            birthYear INTEGER,
            deathYear INTEGER
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO persons (pid, primaryName, birthYear, deathYear)
                VALUES (?, ?, ?, ?)
            ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def configPrincipals():
    csv_file_path = 'imdb-' + db_size + '/principals.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS principals')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS principals (
            mid TEXT,
            ordering INTEGER,
            pid TEXT,
            category TEXT,
            job TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO principals (mid, ordering, pid, category, job)
                VALUES (?, ?, ?, ?, ?)
            ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configProfessions():
    csv_file_path = 'imdb-' + db_size + '/professions.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS professions')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS professions (
            pid TEXT,
            jobName TEXT
        )
    ''')

    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO professions (pid, jobName)
                VALUES (?, ?)
            ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def configRatings():
    csv_file_path = 'imdb-' + db_size + '/ratings.csv'

    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS ratings')

    # Create a table for the characters data
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS ratings (
            mid TEXT,
            averageRating REAL,
            numVotes INTEGER
        )
    ''')

    # Open the CSV file and insert the data
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO ratings (mid, averageRating, numVotes)
                VALUES (?, ?, ?)
            ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def configTitles():
    csv_file_path = 'imdb-' + db_size + '/titles.csv'

    sqlite_db_path = 'database.db'

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Drop the table if it already exists
    cursor.execute('DROP TABLE IF EXISTS titles')

    # Create a table for the characters data
    # Adjust the data types according to the contents of your CSV
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS titles (
            mid TEXT,
            ordering INTEGER,
            title TEXT,
            region TEXT,
            language TEXT,
            types TEXT,
            attributes TEXT,
            isOriginalTitle INTEGER
        )
    ''')

    # Open the CSV file and insert the data
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO titles (mid, ordering, title, region, language, types, attributes, isOriginalTitle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def configWriters():
    csv_file_path = 'imdb-' + db_size + '/writers.csv'

    sqlite_db_path = 'database.db'

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Drop the table if it already exists
    cursor.execute('DROP TABLE IF EXISTS writers')

    # Create a table for the characters data
    # Adjust the data types according to the contents of your CSV
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS writers (
            mid TEXT,
            pid TEXT
        )
    ''')

    # Open the CSV file and insert the data
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header row
        for row in csv_reader:
            cursor.execute('''
                INSERT INTO writers (mid, pid)
                VALUES (?, ?)
            ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def main():
    configCharacters()
    configDirectors()
    #configEpisodes()
    configGenres()
    configKnownformovies()
    configMovies()
    configPersons()
    configPrincipals()
    configProfessions()
    configRatings()
    configTitles()
    configWriters()
    

main()