
# A. Find all movies that are played by a sample actor
=======================
MATCH (a:Actor)
WHERE a.first_name CONTAINS "Leonardo" AND a.last_name CONTAINS "DiCaprio"
OPTIONAL MATCH (m:Movie)-[:HAS_ACTOR]->(a)
RETURN a.first_name, a.last_name, m.title AS movie_title, m.release_year, m.plot

Results:
╒════════════╤═══════════╤═══════════════════════╤══════════════╤══════════════════════════════════════════════════════════════════════╕
│a.first_name│a.last_name│movie_title            │m.release_year│m.plot                                                                │
╞════════════╪═══════════╪═══════════════════════╪══════════════╪══════════════════════════════════════════════════════════════════════╡
│"Leonardo"  │"DiCaprio" │"Fury Road: Reborn"    │2025          │"A new generation of rebels in a post-apocalyptic wasteland fight to o│
│            │           │                       │              │verthrow a cruel warlord and his army in a furious battle for freedom.│
│            │           │                       │              │"                                                                     │
├────────────┼───────────┼───────────────────────┼──────────────┼──────────────────────────────────────────────────────────────────────┤
│"Leonardo"  │"DiCaprio" │"Revenge of the Fallen"│2025          │"A former assassin returns to exact revenge on those who betrayed him,│
│            │           │                       │              │ but soon discovers a web of conspiracy involving high-ranking officia│
│            │           │                       │              │ls."                                                                  │
└────────────┴───────────┴───────────────────────┴──────────────┴──────────────────────────────────────────────────────────────────────┘

# B. Find all the number of movies with and without a watch-movie info
====================
MATCH (m:Movie)
WITH COUNT(CASE WHEN m.watchmode_id IS NOT NULL THEN 1 END) AS with_watchmode,
     COUNT(CASE WHEN m.watchmode_id IS NULL THEN 1 END) AS without_watchmode
RETURN with_watchmode, without_watchmode

Results:
╒══════════════╤═════════════════╕
│with_watchmode│without_watchmode│
╞══════════════╪═════════════════╡
│65            │86               │
└──────────────┴─────────────────┘


# C. Find all movies that are released after the year 2023 and has a viewer rating
of at least 5.
=======================
MATCH (m:Movie)
WHERE m.release_year > 2023 AND m.viewer_rating >= 5
RETURN m.title, m.release_year, m.viewer_rating

Results:
╒════════════════════════════════════════╤══════════════╤═══════════════╕
│m.title                                 │m.release_year│m.viewer_rating│
╞════════════════════════════════════════╪══════════════╪═══════════════╡
│"Venom: The Last Dance"                 │2024          │7.2            │
├────────────────────────────────────────┼──────────────┼───────────────┤
│"Smile 2"                               │2024          │7.5            │
├────────────────────────────────────────┼──────────────┼───────────────┤
│"The Wild Robot"                        │2024          │7.4            │
├────────────────────────────────────────┼──────────────┼───────────────┤
│"Terrifier 3"                           │2024          │6.5            │
├────────────────────────────────────────┼──────────────┼───────────────┤
│"Gladiator II"                          │2024          │7.8            │
...

#D. Find all movies with two countries of your choice. Make sure your query returns
more than one movie. List movies that may be associated with either of the countries
(not necessarily both).
============================
MATCH (m:Movie)-[:HAS_COUNTRY]->(c:Country)
WHERE c.name IN ['USA', 'Canada']
RETURN m.title, m.release_year, m.viewer_rating

Results:

╒═══════════════════════╤══════════════╤═══════════════╕
│m.title                │m.release_year│m.viewer_rating│
╞═══════════════════════╪══════════════╪═══════════════╡
│"Venom: The Last Dance"│2024          │7.2            │
├───────────────────────┼──────────────┼───────────────┤
│"Moana 2"              │2024          │7.2            │
├───────────────────────┼──────────────┼───────────────┤
│"Despicable Me 4"      │2024          │7.5            │
├───────────────────────┼──────────────┼───────────────┤
│"Inception"            │2010          │8.8            │
├───────────────────────┼──────────────┼───────────────┤
│"The Wild Robot"       │2024          │7.4            │
├───────────────────────┼──────────────┼───────────────┤
│"The Dark Knight"      │2008          │9.0            │
└───────────────────────┴──────────────┴───────────────┘

#E. Find top 2 movies with largest number of keywords.
=================
MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
WITH m, COUNT(k) AS keyword_count
ORDER BY keyword_count DESC
LIMIT 2
RETURN m.title, keyword_count

Results:

╒══════════════════╤═════════════╕
│m.title           │keyword_count│
╞══════════════════╪═════════════╡
│"Gladiator II"    │5            │
├──────────────────┼─────────────┤
│"Transformers One"│5            │
└──────────────────┴─────────────┘


#F. Find top 5 movies (ordered by rating) in a language of your choice.
===================
MATCH (m:Movie)-[:HAS_LANGUAGE]->(l:Language {name: 'English'})
WITH m, COALESCE(m.viewer_rating, 0) AS Rating
RETURN m.title AS Movie, Rating
ORDER BY Rating DESC
LIMIT 5

Results:

╒═════════════════╤══════╕
│Movie            │Rating│
╞═════════════════╪══════╡
│"The Dark Knight"│9.0   │
├─────────────────┼──────┤
│"Inception"      │8.8   │
├─────────────────┼──────┤
│"The Matrix"     │8.7   │
├─────────────────┼──────┤
│"Interstellar"   │8.6   │
├─────────────────┼──────┤
│"Parasite"       │8.5   │
└─────────────────┴──────┘

#G. Build full text search index to query movie plots.
====================
-> CREATE FULLTEXT INDEX movie_plot_index FOR (m:Movie) ON EACH [m.plot];

#H. Write a full text search query and search for some sample text of your choice.
=====================
-> CALL db.index.fulltext.queryNodes("movie_plot_index", "robot AND island") YIELD node, score
RETURN node.title AS Movie, node.plot AS Plot, score
ORDER BY score DESC;

Results:
╒════════════════╤══════════════════════════════════════════════════════════════════════╤══════════════════╕
│Movie           │Plot                                                                  │score             │
╞════════════════╪══════════════════════════════════════════════════════════════════════╪══════════════════╡
│"The Wild Robot"│"After a shipwreck, an intelligent robot called Roz is stranded on an │3.3482823371887207│
│                │uninhabited island. To survive the harsh environment, Roz bonds with t│                  │
│                │he island's animals and cares for an orphaned baby goose."            │                  │
└────────────────┴──────────────────────────────────────────────────────────────────────┴──────────────────┘
(the score is a relevance measure that indicates how closely the text in a node (in this case, the movie plot) matches the search terms provided)



# Additional queries to test the Graph DB 
-> counting nodes
MATCH (n)
RETURN count(n) AS totalNodes;

-> all the "has_language' relationships :
MATCH (m:Movie)-[r:HAS_LANGUAGE]->(l:Language)
RETURN m.title AS Movie, l.name AS Language, r

--> delete a relationships
MATCH ()-[r:RELATIONSHIP_NAME]->()
DELETE r
