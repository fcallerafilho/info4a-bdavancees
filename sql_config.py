import csv
import sqlite3
import time

db_size = 'medium'

def configCharacters():
    csv_file_path = 'imdb-' + db_size + '/characters.csv'
    sql_file_path = 'sqls/characters.sql'
    sqlite_db_path = 'database.db'
    
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS characters')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO characters (mid, pid, name)
            VALUES (?, ?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configDirectors():
    csv_file_path = 'imdb-' + db_size + '/directors.csv'
    sql_file_path = 'sqls/directors.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS directors')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  
        for row in csv_reader:
            unique_rows.add(tuple(row))  
            
    for row in unique_rows:
        cursor.execute('''
            INSERT INTO directors (mid, pid)
            VALUES (?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configEpisodes():
    csv_file_path = 'imdb-' + db_size + '/episodes.csv'
    sql_file_path = 'sqls/episodes.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS episodes')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO episodes (mid, parentMid, seasonNumber, episodeNumber)
            VALUES (?, ?, ?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configGenres():
    csv_file_path = 'imdb-' + db_size + '/genres.csv'
    sql_file_path = 'sqls/genres.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS genres')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO genres (mid, genre)
            VALUES (?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configKnownformovies():
    csv_file_path = 'imdb-' + db_size + '/knownformovies.csv'
    sql_file_path = 'sqls/knownformovies.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS knownformovies')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO knownformovies (pid, mid)
            VALUES (?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configMovies():
    csv_file_path = 'imdb-' + db_size + '/movies.csv'
    sql_file_path = 'sqls/movies.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS movies')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO movies (mid, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configPersons():
    csv_file_path = 'imdb-' + db_size + '/persons.csv'
    sql_file_path = 'sqls/persons.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS persons')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None) 
        for row in csv_reader:
            unique_rows.add(tuple(row))

    for row in unique_rows:
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
    sql_file_path = 'sqls/principals.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS principals')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header
        for row in csv_reader:
            unique_rows.add(tuple(row))  # Add each row as a tuple to the set, which automatically removes duplicates

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO principals (mid, ordering, pid, category, job)
            VALUES (?, ?, ?, ?, ?)
        ''', row)

    conn.commit()
    cursor.close()
    conn.close()

def configProfessions():
    csv_file_path = 'imdb-' + db_size + '/professions.csv'
    sql_file_path = 'sqls/professions.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS professions')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header
        for row in csv_reader:
            unique_rows.add(tuple(row))  # Add each row as a tuple to the set, which automatically removes duplicates

    for row in unique_rows:
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
    sql_file_path = 'sqls/ratings.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS ratings')

    # Create a table for the characters data
    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header
        for row in csv_reader:
            unique_rows.add(tuple(row))  # Add each row as a tuple to the set, which automatically removes duplicates

    for row in unique_rows:
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
    sql_file_path = 'sqls/titles.sql'
    sqlite_db_path = 'database.db'

    # Connect to the SQLite database
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    # Drop the table if it already exists
    cursor.execute('DROP TABLE IF EXISTS titles')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header
        for row in csv_reader:
            unique_rows.add(tuple(row))  # Add each row as a tuple to the set, which automatically removes duplicates

    for row in unique_rows:
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
    sql_file_path = 'sqls/writers.sql'
    sqlite_db_path = 'database.db'

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()

    cursor.execute('DROP TABLE IF EXISTS writers')

    with open(sql_file_path, 'r') as sql_file:
        sql_script = sql_file.read()
        cursor.executescript(sql_script)

    unique_rows = set()
    with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader, None)  # Skip the header
        for row in csv_reader:
            unique_rows.add(tuple(row))  # Add each row as a tuple to the set, which automatically removes duplicates

    for row in unique_rows:
        cursor.execute('''
            INSERT INTO writers (mid, pid)
            VALUES (?, ?)
        ''', row)

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()

def main():
    timeBegin = time.time()
    configCharacters()
    print("Characters done")
    configDirectors()
    print("Directors done")
    configEpisodes()
    print("Episodes done")
    configGenres()
    print("Genres done")
    configKnownformovies()
    print("Knownformovies done")
    configMovies()
    print("Movies done")
    configPersons()
    print("Persons done")
    configPrincipals()
    print("Principals done")
    configProfessions()
    print("Professions done")
    configRatings()
    print("Ratings done")
    configTitles()
    print("Titles done")
    configWriters()
    print("Writers done")
    timeEnd = time.time()
    totalTime = timeEnd - timeBegin
    print("Execution time: {:f}".format(totalTime))

main()