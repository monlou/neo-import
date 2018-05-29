from neo4j.v1 import GraphDatabase

# Connect to Neo4j
uri = "bolt://localhost:7687/db"
driver = GraphDatabase.driver(uri, encryption=(False))

# Document Files
file = "file:///CSVs/Fishes-Shortened.csv"

# Upload CSV files to Graph Database
def createNodes():
    # Create transaction string
    periodic_commit = "USING PERIODIC COMMIT 10000"
    load_csv = "LOAD CSV WITH HEADERS FROM '"
    family_setup = """' AS row
                        CREATE (n:Families)
                        SET n = row,
                        n.name = row.family"""
    fish_setup = """' AS row
                       CREATE (n:Fishes)
                       SET n = row,
                       n.recordID = row.recordID,
                       n.scientificName = row.scientificName,
                       n.decimalLatitude = row.decimalLatitude,
                       n.decimalLongitude = row.decimalLongitude,
                       n.coordinateUncertaintyInMeters = row.coordinateUncertaintyInMeters,
                       n.eventDate = row.eventDate,
                       n.family = row.family"""
    full_setup = """' AS row
                   CREATE (n:Fishes)
                   SET n = row,
                   n.recordID = row.recordID,
                   n.scientificName = row.scientificName,
                   n.decimalLatitude = row.decimalLatitude,
                   n.decimalLongitude = row.decimalLongitude,
                   n.coordinateUncertaintyInMeters = row.coordinateUncertaintyInMeters,
                   n.eventDate = row.eventDate,
                   n.species = row.species,
                   n.family = row.family,
                   n.kingdom = row.kingdom,
                   n.sex = row.sex,
                   n.lifeStage = row.lifeStage,
                   n.institution = row.institution"""
    fish_csv = load_csv + file + fish_setup
    family_csv = load_csv + file + family_setup
    # Run Cypher query
    with driver.session() as session:
        with session.begin_transaction() as upload:
            upload.run(fish_csv)
            upload.run(family_csv)

def removeDuplicates():
    merge_nodes = """MATCH (n:Families)
                WITH n.family AS family, COLLECT(n) AS nodelist, COUNT(*) AS count
                WHERE count > 1
                CALL apoc.refactor.mergeNodes(nodelist) YIELD node
                RETURN node"""
    remove_data = """"MATCH (n:Families) REMOVE n.scientificName, n.coordinateUncertaintyInMeters,
                    n.decimalLatitude, n.decimalLongitude, n.eventDate, n.basisOfRecord, n.name, n.RecordID"""
    with driver.session() as session:
        with session.begin_transaction() as merge:
            merge.run(merge_nodes)
            merge.run(remove_data)

# Add relationships between related fish
def createRelationships():
    create_relation = """MATCH (n:Fishes),(m:Families)
                        WHERE n.family = m.family
                        CREATE (n)-[r:Related 
                            {family: n.family ,
                            fish: n.scientificName}]->(m)"""
    create_location = """MATCH (n:Fishes),(m:Fishes)
                        WHERE abs(tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) < 0.1
                        AND n <> m
                        CREATE (n)-[:SightedNearby]->(m)"""
    create_time = """MATCH (n:Fishes),(m:Fishes)
                        WHERE abs(tofloat(left(n.eventDate,4)) - tofloat(left(m.eventDate,4))) < 1
                        AND n <> m
                        CREATE (n)-[:SameYear]->(m)"""
    create_nearby = """MATCH (n:Fishes),(m:Fishes)
                        WHERE sqrt((tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) 
                            * (tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) 
                            + (tofloat(n.decimalLongitude) - tofloat(m.decimalLongitude)) 
                            * (tofloat(n.decimalLongitude) - tofloat(m.decimalLongitude))) <= 0.5
                        AND abs(toFloat(substring(n.eventDate, 0, 4)) * 12 + toFloat(substring(n.eventDate, 5, 2)) 
                            - toFloat(substring(m.eventDate, 0, 4)) * 12 + toFloat(substring(m.eventDate, 5, 2))) <= 1
                        CREATE (n)-[r:Nearby 
                            {distance: sqrt((tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) 
                                * (tofloat(n.decimalLatitude) - tofloat(m.decimalLatitude)) 
                                + (tofloat(n.decimalLongitude) - tofloat(m.decimalLongitude)) 
                                * (tofloat(n.decimalLongitude) - tofloat(m.decimalLongitude))),
                            timeDiff: abs(toFloat(substring(n.eventDate, 0, 4)) * 12 + toFloat(substring(n.eventDate, 5, 2)) 
                                - toFloat(substring(m.eventDate, 0, 4)) * 12 + toFloat(substring(m.eventDate, 5, 2)))}]->(m)"""
    # Run Cypher query
    with driver.session() as session:
        with session.begin_transaction() as upload:
            upload.run(create_relation)
            #upload.run(create_location)
            #upload.run(create_time)
            upload.run(create_nearby)

#createNodes()
mergeDuplicates()
createRelationships()
