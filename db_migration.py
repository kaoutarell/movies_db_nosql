import psycopg2 # type: ignore
import csv
from dotenv import load_dotenv # type: ignore
import os

# Load environment variables from .env file
load_dotenv()

# Connect to PostgreSQL
conn = psycopg2.connect(
    dbname=os.getenv("DATABASE_NAME"),
    user=os.getenv("DATABASE_USER"),
    password=os.getenv("DATABASE_PASSWORD"),
    host=os.getenv("DATABASE_HOST"),
    port=os.getenv("DATABASE_PORT")
)
cur = conn.cursor()

# 1. Export Movies 
movies_query = """
SELECT 
    m.title,
    m.plot,
    cr.rating AS content_rating,
    r.viewer_rating,
    m.release_year,
    g.name AS genre,
    l.name AS language,
    a.title AS aka,
    split_part(act.name, ' ', 1) AS actor_first_name,  -- First name of the actor
    split_part(act.name, ' ', 2) AS actor_last_name,   -- Last name of the actor
    k.name AS keyword,
    c.name AS country   -- Adding the country name
FROM movie m
LEFT JOIN movie_content_rating mcr ON m.movie_id = mcr.movie_id
LEFT JOIN content_rating cr ON mcr.content_rating_id = cr.content_rating_id
LEFT JOIN movie_review mr ON m.movie_id = mr.movie_id
LEFT JOIN review r ON mr.review_id = r.review_id
LEFT JOIN movie_genre mg ON m.movie_id = mg.movie_id
LEFT JOIN genre g ON mg.genre_id = g.genre_id
LEFT JOIN movie_language ml ON m.movie_id = ml.movie_id
LEFT JOIN language_ l ON ml.language_id = l.language_id
LEFT JOIN movie_aka ma ON m.movie_id = ma.movie_id
LEFT JOIN aka a ON ma.aka_id = a.aka_id
LEFT JOIN movie_actor ma2 ON m.movie_id = ma2.movie_id
LEFT JOIN actor act ON ma2.actor_id = act.actor_id
LEFT JOIN movie_keyword mk ON m.movie_id = mk.movie_id
LEFT JOIN keyword k ON mk.keyword_id = k.keyword_id
LEFT JOIN movie_country mc ON m.movie_id = mc.movie_id  -- Join movie_country table
LEFT JOIN country c ON mc.country_id = c.country_id;    -- Join country table
"""

# fetch results
cur.execute(movies_query)
movies_rows = cur.fetchall()

# Write to CSV file
with open('movies.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow([
        'title', 'plot', 'content_rating', 'viewer_rating', 'release_year', 
        'genre', 'language', 'aka', 'actor_first_name', 'actor_last_name', 'keyword', 'country'
    ])
    for row in movies_rows:
        writer.writerow(row)

print("Movies data exported successfully to movies.csv.")

# 2. Export Actors (first and last name only))
actors_query = """
SELECT 
    split_part(name, ' ', 1) AS first_name,  -- First name
    split_part(name, ' ', 2) AS last_name    -- Last name
FROM actor;
"""

# fetch results
cur.execute(actors_query)
actors_rows = cur.fetchall()

# Write to a CSV file
with open('actors.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['first_name', 'last_name'])
    for row in actors_rows:
        writer.writerow(row)

print("Actors data exported successfully to actors.csv.")

# 3. Export Keywords
keywords_query = """
SELECT name AS keyword
FROM keyword;
"""

# fetch results
cur.execute(keywords_query)
keywords_rows = cur.fetchall()

# Write to a CSV file
with open('keywords.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['keyword'])
    for row in keywords_rows:
        writer.writerow(row)

print("Keywords data exported successfully to keywords.csv.")

# 4. Export Languages
languages_query = """
SELECT DISTINCT name AS language
FROM language_;
"""

# fetch results
cur.execute(languages_query)
languages_rows = cur.fetchall()

# Write to a CSV file
with open('languages.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['language'])
    for row in languages_rows:
        writer.writerow(row)

print("Languages data exported successfully to languages.csv.")

# 5. Export Genres
genres_query = """
SELECT name AS genre
FROM genre;
"""

# fetch results
cur.execute(genres_query)
genres_rows = cur.fetchall()

# Write to a CSV file
with open('genres.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['genre'])
    for row in genres_rows:
        writer.writerow(row)

print("Genres data exported successfully to genres.csv.")

# Repeat similar queries for actors, countries, and keywords
cur.close()
conn.close()
