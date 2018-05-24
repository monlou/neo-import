from neo4j.v1 import GraphDatabase
import csv

# Connect to Neo4j
uri = "bolt://localhost:7687/db"
driver = GraphDatabase.driver(uri, encryption=(False))

# Document Files
fish_brief = "file:///CSVs/Fishes-brief.csv"
fish_file = "file:///CSVs/Fishes-Shortened.csv"
mammals_file = "file:///CSVs/Mammals.csv"
conservation_file = "file:///CSVs/Conservation.csv"

# Convert Shapefiles to CSV
#with open('/home/ricckli/Desktop/example.tsv', 'rb') as csvfile:
#    reader = csv.reader(csvfile, delimiter='\t') #my example uses the tab as delimiter
    #for line in reader:
        #print '; '.join(line)

# Upload CSV files to Graph Database
def upload_csv():
    # Create transaction string
    load_csv = "LOAD CSV WITH HEADERS FROM '"
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
    # Setup indexes for creating relationships
    # Run Cypher query
    with driver.session() as session:
        with session.begin_transaction() as upload:
            upload.run(fish_csv)

def indexes():
    fish_indexes = ["CREATE INDEX ON :Fishes(scientificName);",
                    "CREATE INDEX ON :Fishes(recordID);",
                    "CREATE INDEX ON :Fishes(decimalLatitude);",
                    "CREATE INDEX ON :Fishes(decimalLongitude);",
                    "CREATE INDEX ON :Fishes(eventDate);",
                    "CREATE INDEX ON :Fishes(family);"]
    with driver.session() as session:
        with session.begin_transaction() as index:
            for i in fish_indexes:
                index.run(i)

# Add relationships between related fish
def relationships():
    print("Building relationships")
    create_relation = """MATCH (n:Fishes),(m:Fishes)
                        WHERE n.family = m.family
                        AND NOT n.scientificName = m.scientificName
                        CREATE (n)-[:Related]->(m)"""
    create_location = """MATCH (n:Fishes),(m:Fishes)
                        WHERE n.decimalLongitude <= toFloat(m.decimalLongitude) +1.0
                        AND n.decimalLongitude >= toFloat(m.decimalLatitude) -1.0
                        AND n.decimalLatitude <= toFloat(m.decimalLatitude) +1.0
                        AND n.decimalLatitude >= toFloat(m.decimalLatitude) -1.0
                        CREATE (n)-[:Sighted]->(m)"""
    with driver.session() as session:
        with session.begin_transaction() as relationship:
            relationship.run(create_relation)
            relationship.run(create_location)

def nodes():
    begin_node = 'CREATE (a:Family { Name : "'
    end_node = '" })'
    families = ['Galaxiidae', 'Pseudaphritidae', 'Percichthyidae', 'Eleotridae', 'Monacanthidae',
                'Odacidae']
    create_relation = """MATCH (n:Fishes),(f:Familiy)
                            WHERE n.family = f.Name
                            CREATE (n)-[:Fam]->(f)"""
    with driver.session() as session:
        with session.begin_transaction() as nodes:
            for i in families:
                nodes.run(begin_node + i + end_node)
            nodes.run(create_relation)

upload_csv()
#indexes()
relationships()
#nodes()
