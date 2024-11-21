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

# Extract Movies
cur.execute("""
    SELECT m.title, m.plot, cr.rating AS content_rating, 
           r.viewer_rating, m.release_year, 
           ARRAY_AGG(g.name) AS genres,
           ARRAY_AGG(l.name) AS languages,
           ARRAY_AGG(a.title) AS akas,
           wm.watchmode_id AS watchmode_id
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
    LEFT JOIN movie_watchmode mw ON m.movie_id = mw.movie_id
    LEFT JOIN watchmode wm ON mw.watchmode_id = wm.watchmode_id
    GROUP BY m.movie_id, cr.rating, r.viewer_rating, wm.watchmode_id;
""")
movies = cur.fetchall()

with open("movies.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["title", "plot", "content_rating", "viewer_rating", "release_year", "genres", "languages", "akas", "watchmode_id"])
    writer.writerows(movies)

# Extract Actors
cur.execute("""
    SELECT a.name AS actor_name, 
           ARRAY_AGG(m.title) AS movies
    FROM actor a
    LEFT JOIN movie_actor ma ON a.actor_id = ma.actor_id
    LEFT JOIN movie m ON ma.movie_id = m.movie_id
    GROUP BY a.actor_id;
""")
actors = cur.fetchall()

with open("actors.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["actor_name", "movies"])
    writer.writerows(actors)

# Extract Countries
cur.execute("""
    SELECT c.name AS country_name, c.country_code, 
           ARRAY_AGG(m.title) AS movies
    FROM country c
    LEFT JOIN movie_country mc ON c.country_id = mc.country_id
    LEFT JOIN movie m ON mc.movie_id = m.movie_id
    GROUP BY c.country_id;
""")
countries = cur.fetchall()

with open("countries.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["country_name", "country_code", "movies"])
    writer.writerows(countries)

# Extract Keywords
cur.execute("""
    SELECT k.name AS keyword, 
           ARRAY_AGG(m.title) AS movies
    FROM keyword k
    LEFT JOIN movie_keyword mk ON k.keyword_id = mk.keyword_id
    LEFT JOIN movie m ON mk.movie_id = m.movie_id
    GROUP BY k.keyword_id;
""")
keywords = cur.fetchall()

with open("keywords.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["keyword", "movies"])
    writer.writerows(keywords)

cur.close()
conn.close()
