from neo4j import GraphDatabase  # type: ignore
from dotenv import load_dotenv  # type: ignore
import os

# Load environment variables 
load_dotenv()

# Connect to Neo4j Aura 
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


# Function to create nodes and relationships
def create_nodes_and_relationships(session):
    # 1. Movies data and Movie nodes
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1PSkijnWnG--CIBeWG9VS56z6RhDaYhg6' AS row
    CREATE (m:Movie {
        title: row.title,
        plot: row.plot,
        content_rating: row.content_rating,
        viewer_rating: toFloat(row.viewer_rating),
        release_year: toInteger(row.release_year),
        watchmode_id: row.watchmode_id
    });
    """)

    # 2. Genres data and Genre nodes, then relate them to Movies --> required 
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=18orT5EblD6Abyu67u69LP0fr8nSY3X6o' AS row
    MERGE (g:Genre {name: row.genre})
    WITH g, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_GENRE]->(g);
    """)

    # 3. Languages data and Language nodes, then relate them to Movies --> required 
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1k9FkaDbr-2hITiimIHVk0W1lrMqNIUp-' AS row
    WITH row WHERE row.language IS NOT NULL AND row.language <> ''
    MERGE (l:Language {name: row.language})
    WITH l, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_LANGUAGE]->(l);
    """)

    # 4. Actors data and Actor nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1oreKJWYH9y_3iSV7eT-AerwI725kg8U9' AS row
    MERGE (a:Actor {name: row.actor_name})
    WITH a, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (a)-[:ACTED_IN]->(m);
    """)

    # 5. Countries data and Country nodes, then relate them to Movies
    session.run("""
    LOAD CSV WITH HEADERS FROM 'https://drive.google.com/uc?export=download&id=1TWDZyExrigDz_KAAt2DZo96jfzyIFv0X' AS row
    MERGE (c:Country {name: row.country_name, country_code: row.country_code})
    WITH c, row
    UNWIND split(row.movies, ',') AS movie_title
    MATCH (m:Movie {title: movie_title})
    MERGE (m)-[:HAS_COUNTRY]->(c);
    """)

    # 6. Keywords data and Keyword nodes, then relate them to Movies
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
