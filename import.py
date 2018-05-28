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
            #upload.run(fish_csv)
            upload.run(family_csv)

def mergeDuplicates():
    merge_nodes = """MATCH (n:Families)
                WITH n.family AS family, COLLECT(n) AS nodelist, COUNT(*) AS count
                WHERE count > 1
                CALL apoc.refactor.mergeNodes(nodelist) YIELD node
                RETURN node"""
    with driver.session() as session:
        with session.begin_transaction() as merge:
            merge.run(merge_nodes)

#createNodes()
mergeDuplicates()