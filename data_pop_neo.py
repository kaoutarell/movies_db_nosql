from neo4j import GraphDatabase  # type: ignore
from dotenv import load_dotenv  # type: ignore
import os

# Load environment variables
load_dotenv()

# Connect to Neo4j
uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")

# Connect to the database
try:
    driver = GraphDatabase.driver(uri, auth=(username, password))
    with driver.session() as session:
        result = session.run("RETURN 'Connection successful!' AS message")
        for record in result:
            print(record["message"])
    print("Connected to Neo4j!")
except Exception as e:
    print(f"Error: {e}")
finally:
    if 'driver' in locals() and driver is not None:
        driver.close()


# Function to create nodes and relationships from CSV data
def create_nodes_and_relationships(session):
    # 1. Movies data and Movie nodes
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1PSkijnWnG--CIBeWG9VS56z6RhDaYhg6' AS row
    WITH row
    MERGE (m:Movie {title: row.title})
    ON CREATE SET m.plot = row.plot, 
                m.content_rating = row.content_rating, 
                m.viewer_rating = toFloat(row.viewer_rating),
                m.release_year = toInteger(row.release_year),
                m.watchmode_id = CASE WHEN row.watchmode_id IS NOT NULL AND row.watchmode_id <> '' THEN row.watchmode_id ELSE NULL END
    ON MATCH SET m.plot = row.plot, 
                m.content_rating = row.content_rating, 
                m.viewer_rating = toFloat(row.viewer_rating),
                m.release_year = toInteger(row.release_year),
                m.watchmode_id = CASE WHEN row.watchmode_id IS NOT NULL AND row.watchmode_id <> '' THEN row.watchmode_id ELSE m.watchmode_id END
    RETURN row.title;
    """)

    # 2. Genres data and Genre nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=18orT5EblD6Abyu67u69LP0fr8nSY3X6o' AS row
    MERGE (g:Genre {name: row.genre})
    WITH g, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_GENRE]->(g);
    """)

    # 3. Actors data and Actor nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1906saR4S2dsfWprVgxMzSbhk6FaE-Ius' AS row
    WITH row
    WHERE row.first_name IS NOT NULL AND row.last_name IS NOT NULL
    // Clean up the 'movies' column (remove brackets, quotes, and split the movie titles by comma)
    WITH row, trim(replace(replace(row.movies, '[', ''), ']', '')) AS cleaned_movies
    // Remove any extra spaces or quotes around movie titles
    WITH row, split(replace(cleaned_movies, '"', ''), ',') AS movie_titles
    UNWIND movie_titles AS movie_title
    WITH row, trim(movie_title) AS clean_movie_title
    MATCH (m:Movie {title: clean_movie_title})
    MERGE (a:Actor {first_name: row.first_name, last_name: row.last_name})
    MERGE (a)-[:HAS_ACTOR]->(m);  // Create relationship between actor and movie
    """)




    # 4. Countries data and Country nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1TWDZyExrigDz_KAAt2DZo96jfzyIFv0X' AS row
    MERGE (c:Country {name: row.country_name})
    ON CREATE SET c.country_code = row.country_code
    WITH c, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_COUNTRY]->(c);
    """)

    # 5. Keywords data and Keyword nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1z7UXWUw9q841BmSmmomLUUBhGH8_8NtG' AS row
    MERGE (k:Keyword {name: row.keyword})
    WITH k, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_KEYWORD]->(k);
    """)

    print("Nodes and relationships created successfully!")


if __name__ == "__main__":
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session() as session:
            create_nodes_and_relationships(session)
    except Exception as e:
        print(f"Error during data population: {e}")
    finally:
        if 'driver' in locals() and driver is not None:
            driver.close()
