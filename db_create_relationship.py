import csv
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os
import requests
import uuid  # For unique IDs

# Load environment variables
load_dotenv()

# Connect to Neo4j Aura
uri = os.getenv("NEO4J_URI")
username = os.getenv("NEO4J_USERNAME")
password = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(uri, auth=(username, password))

def fetch_csv_from_url(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error if the request fails
    return response.text.splitlines()  # Split content into lines for CSV reader

def generate_id(row):
    """Generate a unique ID for movies based on title and release year."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{row['title']}-{row['release_year']}"))

def bulk_insert_data(driver):
    with driver.session() as session:
        # Create a unique constraint on Movie IDs
        session.run("""
        CREATE CONSTRAINT FOR (m:Movie)
        REQUIRE m.id IS UNIQUE
        """)
        print("Unique constraint on Movie(id) created successfully.")

        # Bulk Insert Movies
        movies_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1do4SeWxGxoh9S8_iP-NhSk7cQNT2oANs')
        reader = csv.DictReader(movies_csv)
        for row in reader:
            movie_id = generate_id(row)  # Generate a unique ID for the movie
            
            # Create Movie node with AKA titles
            session.run("""
            MERGE (m:Movie {id: $id})
            SET m.title = $title, 
                m.plot = $plot, 
                m.content_rating = $content_rating, 
                m.viewer_rating = $viewer_rating, 
                m.release_year = $release_year
            """, id=movie_id, title=row['title'], plot=row['plot'], 
            content_rating=row['content_rating'], viewer_rating=float(row['viewer_rating']), 
            release_year=int(row['release_year']))
            
            # Add Genres
            for genre in row['genre'].split(','):
                session.run("""
                MERGE (g:Genre {name: $genre})
                MERGE (m)-[:HAS_GENRE]->(g)
                """, genre=genre.strip(), id=movie_id)
            
            # Add Languages
            for language in row['language'].split(','):
                session.run("""
                MERGE (l:Language {name: $language})
                MERGE (m)-[:HAS_LANGUAGE]->(l)
                """, language=language.strip(), id=movie_id)
            
            # Add AKAs
            for aka in row['aka'].split(','):
                session.run("""
                MERGE (a:Aka {title: $aka})
                MERGE (m)-[:HAS_AKA]->(a)
                """, aka=aka.strip(), id=movie_id)
            
            # Link Actors
            actor_first_name = row['actor_first_name']
            actor_last_name = row['actor_last_name']
            session.run("""
            MATCH (m:Movie {id: $id})
            MERGE (a:Actor {first_name: $actor_first_name, last_name: $actor_last_name})
            MERGE (m)-[:HAS_ACTOR]->(a)
            """, id=movie_id, actor_first_name=actor_first_name.strip(), actor_last_name=actor_last_name.strip())
            
            # Link Keywords
            for keyword in row['keyword'].split(','):
                session.run("""
                MERGE (k:Keyword {name: $keyword})
                MERGE (m)-[:HAS_KEYWORD]->(k)
                """, keyword=keyword.strip(), id=movie_id)
            
            # Link Countries
            for country in row['country'].split(','):
                session.run("""
                MERGE (c:Country {name: $country_name})
                MERGE (m)-[:HAS_COUNTRY]->(c)
                """, country_name=country.strip(), id=movie_id)
        
        print("Movies and relationships inserted successfully.")
        
        # Bulk Insert Actors
        actors_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1JEkHF-RV56H0bxDRk_sS32GHxtvFIqKm')
        reader = csv.DictReader(actors_csv)
        for row in reader:
            session.run("""
            MERGE (a:Actor {first_name: $first_name, last_name: $last_name})
            """, first_name=row['first_name'], last_name=row['last_name'])
        
        print("Actors inserted successfully.")
        
        # Bulk Insert Keywords
        keywords_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1ffaKrzBg9E8LRLE_VHmZdFUf_jRrNrHP')
        reader = csv.DictReader(keywords_csv)
        for row in reader:
            session.run("""
            MERGE (k:Keyword {name: $keyword})
            """, keyword=row['keyword'])
        
        print("Keywords inserted successfully.")
        
        # Bulk Insert Genres
        genres_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1g3_jVilsktn7NbtABPgnW92N5fhRg-H0')
        reader = csv.DictReader(genres_csv)
        for row in reader:
            session.run("""
            MERGE (g:Genre {name: $genre})
            """, genre=row['genre'])
        
        print("Genres inserted successfully.")
        
        # Bulk Insert Languages
        languages_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1yHYJ04yuoVrEaznDLG9Gs-Gh395L4hVC')
        reader = csv.DictReader(languages_csv)
        for row in reader:
            session.run("""
            MERGE (l:Language {name: $language})
            """, language=row['language'])
        
        print("Languages inserted successfully.")
        
        # Bulk Insert Countries
        countries_csv = fetch_csv_from_url('https://drive.google.com/uc?export=download&id=1Kh4J80kfh9bKD0au3gFKIOSkKpN-3Tye')
        reader = csv.DictReader(countries_csv)
        for row in reader:
            session.run("""
            MERGE (c:Country {name: $country_name})
            """, country_name=row['country'])
        
        print("Countries inserted successfully.")

# Execute the bulk insert
bulk_insert_data(driver)

# Close the Neo4j driver connection
driver.close()
