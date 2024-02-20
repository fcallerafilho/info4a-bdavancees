import csv
import sqlite3

# Path to your CSV file
csv_file_path = 'imdb-full/episodes.csv'

# Path to your SQLite database
sqlite_db_path = 'database.db'

# Connect to the SQLite database
conn = sqlite3.connect(sqlite_db_path)
cursor = conn.cursor()

# Drop the table if it already exists
cursor.execute('DROP TABLE IF EXISTS directors')

# Create a table for the characters data
# Adjust the data types according to the contents of your CSV
cursor.execute('''
    CREATE TABLE IF NOT EXISTS directors (
        mid TEXT,
        parentMid TEXT,
        seasonNumber NUMBER,
        episodeNumber NUMBER
    )
''')

# Open the CSV file and insert the data
with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
    csv_reader = csv.reader(csv_file)
    next(csv_reader, None)  # Skip the header row
    for row in csv_reader:
        cursor.execute('''
            INSERT INTO directors (mid, parentMid, seasonNumber, episodeNumber)
            VALUES (?, ?, ?, ?)
        ''', row)

# Commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()
