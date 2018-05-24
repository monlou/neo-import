from neo4j.v1 import GraphDatabase

# Connect to Neo4j
uri = "bolt://localhost:7687/db"
driver = GraphDatabase.driver(uri, encryption=(False))

# Document Files
fish_brief = "file:///CSVs/Fishes-brief.csv"
fish_file = "file:///CSVs/Fishes-Shortened.csv"

fish_indexes = ["CREATE INDEX ON :Fishes(scientificName);",
                    "CREATE INDEX ON :Fishes(recordID);",
                    "CREATE INDEX ON :Fishes(decimalLatitude);",
                    "CREATE INDEX ON :Fishes(decimalLongitude);",
                    "CREATE INDEX ON :Fishes(eventDate);",
                    "CREATE INDEX ON :Fishes(family);"]

# Upload CSV files to Graph Database
def upload_csv(file):
    # Create transaction string
    load_csv = "USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM '"
    fish_db_setup = """' AS row 
                   CREATE (n:Fishes) 
                   SET n = row, 
                   n.recordID = row.recordID,
                   n.scientificName = row.scientificName, 
                   n.decimalLatitude = row.decimalLatitude, 
                   n.decimalLongitude = row.decimalLongitude, 
                   n.coordinateUncertaintyInMeters = row.coordinateUncertaintyInMeters, 
                   n.eventDate = row.eventDate, 
                   n.basisOfRecord = row.basisOfRecord, 
                   n.family = row.family """
    fish_csv = load_csv + file + fish_db_setup
    # Setup indexes for creating relationships
    # Run Cypher query
    with driver.session() as session:
        with session.begin_transaction() as upload:
            upload.run(fish_csv)

# Add relationships between related fish
def relationships(edge):
    create_relation = """MATCH (n:Fishes),(m:Fishes)
                        WHERE n.family = m.family
                        AND NOT n.scientificName = m.scientificName
                        CREATE (n)-[:Related]->(m)"""
    create_location = """MATCH (n:Fishes),(m:Fishes)
                        WHERE abs(tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) < 0.1
                        AND n <> m
                        CREATE (n)-[:SightedNearby]->(m)"""
    create_time = """MATCH (n:Fishes),(m:Fishes)
                        WHERE abs(tofloat(left(n.eventDate,4)) - tofloat(left(m.eventDate,4))) < 1 
                        AND n <> m
                        CREATE (n)-[:SameYear]->(m)"""

    if edge == 'relation':
        return create_relation
    elif edge == 'location':
        return create_location
    elif edge == 'time':
        return create_time

upload_csv(fish_brief)