from neo4j.v1 import GraphDatabase

# Connect to Neo4j
uri = "bolt://localhost:7687/db"
driver = GraphDatabase.driver(uri, encryption=(False))

# Document Files
fish_brief = "file:///CSVs/Fishes-brief.csv"
fish_file = "file:///CSVs/Fishes-Shortened.csv"
reserves_file = "file:///CSVs/marine_reserves.csv"

# Upload CSV files to Graph Database
def upload_csv():
    # Create transaction string
    load_csv = "LOAD CSV WITH HEADERS FROM '"
    reserve_db_setup = """' AS row 
                   CREATE (n:Reserves) 
                   SET n = row, 
                   n.PA_ID = row.paID,
                   n.NAME = row.name,
                   n.TYPE = row.type,
                   n.GAZ_DATE = row.gazDate,
                   n.LATEST_GAZ = row.latestGaz,
                   n.AUTHORITY = row.authority,
                   n.GOVERNANCE = row.governance,
                   n.ENVIRON = row.environment,
                   n.X_COORD = row.xCoord,
                   n.Y_COORD = row.yCoord,
                   n.MGT_PLAN = row.management,
                   n.RES_NUMBER = row.resNumber,
                   n.ZONE_TYPE = row.zoneType,
                   n.SHAPE_AREA = row.shapeArea,
                   n.SHAPE_LEN = row.shapeLength"""
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
    fish_csv = load_csv + fish_file + fish_db_setup
    reserves_csv = load_csv + reserves_file + reserve_db_setup

    # Setup indexes for creating relationships
    # Run Cypher query
    with driver.session() as session:
        with session.begin_transaction() as upload:
            upload.run(fish_csv)
            upload.run(reserves_csv)

def indexes():
    fish_indexes = ["CREATE INDEX ON :Fishes(scientificName);",
                    "CREATE INDEX ON :Fishes(recordID);",
                    "CREATE INDEX ON :Fishes(decimalLatitude);",
                    "CREATE INDEX ON :Fishes(decimalLongitude);",
                    "CREATE INDEX ON :Fishes(eventDate);",
                    "CREATE INDEX ON :Fishes(family);"]
    reserve_indexes = ["CREATE INDEX ON :Reserves(name);",
                    "CREATE INDEX ON :Reserves(type);",
                    "CREATE INDEX ON :Reserves(xCoord);",
                    "CREATE INDEX ON :Reserves(yCoord);",
                    "CREATE INDEX ON :Reserves(shapeArea);"]
    with driver.session() as session:
        with session.begin_transaction() as index:
            for i in fish_indexes:
                index.run(i)
            for j in reserve_indexes:
                index.run(j)

# Add relationships between related fish
def relationships():
    create_same = """MATCH (n1:Fishes),(n2:Fishes)
                        WHERE n1.scientificName = n2.scientificName 
                        AND NOT n1.recordID = n2.recordID
                        CREATE (n1)-[:Same]->(n2)"""
    create_relation = """MATCH (n1:Fishes),(n2:Fishes)
                        WHERE n1.family = n2.family
                        AND NOT n1.scientificName = n2.scientificName
                        CREATE (n1)-[:Related]->(n2)"""
    create_location = """MATCH (f:Fishes),(r:Reserves)
                        WHERE f.decimalLongitude <= r.xCoord
                        AND f.decimalLatitude >= r.yCoord
                        CREATE (f)-[:Sighted]->(r)"""
    with driver.session() as session:
        with session.begin_transaction() as relationship:
            relationship.run(create_same)
            relationship.run(create_relation)
            relationship.run(create_location)

upload_csv()
indexes()
relationships()