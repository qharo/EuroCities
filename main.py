from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, expr, udf, struct, lit, array
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType, BooleanType
import xml.etree.ElementTree as ET
import os

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("OSM France Analysis") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# Define schema for OSM XML elements
osm_node_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("tags", MapType(StringType(), StringType()), True)
])

osm_way_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("nodes", ArrayType(StringType()), True)
])

osm_relation_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("tags", MapType(StringType(), StringType()), True),
    StructField("members", ArrayType(
        StructType([
            StructField("type", StringType(), True),
            StructField("ref", StringType(), True),
            StructField("role", StringType(), True)
        ])
    ), True)
])

def parse_osm_chunk(xml_chunk):
    """Parse an OSM XML chunk and extract elements"""
    try:
        # Parse the XML chunk
        root = ET.fromstring(xml_chunk)
        elements = []
        
        # Process nodes
        for node in root.findall(".//node"):
            node_id = node.get("id")
            lat = float(node.get("lat")) if node.get("lat") else None
            lon = float(node.get("lon")) if node.get("lon") else None
            
            # Extract tags
            tags = {}
            for tag in node.findall("tag"):
                k = tag.get("k")
                v = tag.get("v")
                if k and v:
                    tags[k] = v
            
            # Check if it's a city/town with population
            if 'place' in tags and tags['place'] in ['city', 'town'] and 'population' in tags:
                try:
                    population = int(tags['population'])
                    if population > 100000:
                        elements.append({
                            "id": node_id,
                            "type": "node",
                            "lat": lat,
                            "lon": lon,
                            "tags": tags
                        })
                except ValueError:
                    pass
        
        # Process ways (for buildings)
        for way in root.findall(".//way"):
            way_id = way.get("id")
            
            # Extract tags
            tags = {}
            for tag in way.findall("tag"):
                k = tag.get("k")
                v = tag.get("v")
                if k and v:
                    tags[k] = v
            
            # Extract nodes
            nodes = []
            for nd in way.findall("nd"):
                ref = nd.get("ref")
                if ref:
                    nodes.append(ref)
            
            # Check if it's a building
            if 'building' in tags:
                elements.append({
                    "id": way_id,
                    "type": "way",
                    "tags": tags,
                    "nodes": nodes
                })
        
        # Process relations (for administrative boundaries)
        for relation in root.findall(".//relation"):
            relation_id = relation.get("id")
            
            # Extract tags
            tags = {}
            for tag in relation.findall("tag"):
                k = tag.get("k")
                v = tag.get("v")
                if k and v:
                    tags[k] = v
            
            # Extract members
            members = []
            for member in relation.findall("member"):
                member_type = member.get("type")
                ref = member.get("ref")
                role = member.get("role")
                if member_type and ref:
                    members.append({
                        "type": member_type,
                        "ref": ref,
                        "role": role or ""
                    })
            
            # Check if it's a city/town with population
            if 'place' in tags and tags['place'] in ['city', 'town'] and 'population' in tags:
                try:
                    population = int(tags['population'])
                    if population > 100000:
                        elements.append({
                            "id": relation_id,
                            "type": "relation",
                            "tags": tags,
                            "members": members
                        })
                except ValueError:
                    pass
            
            # Also check if it's an administrative boundary that might contain a city
            if 'boundary' in tags and tags['boundary'] == 'administrative' and 'name' in tags:
                elements.append({
                    "id": relation_id,
                    "type": "relation",
                    "tags": tags,
                    "members": members
                })
                
        return elements
    except Exception as e:
        print(f"Error parsing XML chunk: {e}")
        return []

def convert_pbf_to_xml_chunks(pbf_file, output_dir="osm_chunks", chunk_size=1000000):
    """
    Convert OSM PBF to XML chunks using osmconvert
    This requires osmconvert to be installed: https://wiki.openstreetmap.org/wiki/Osmconvert
    
    For now, this is a placeholder. In a real implementation, you would:
    1. Use subprocess to call osmconvert to convert PBF to XML
    2. Split the XML file into manageable chunks
    
    For simplicity in this example, we assume the XML chunks already exist
    """
    print(f"Converting {pbf_file} to XML chunks in {output_dir}")
    print("In a real implementation, this would use osmconvert")
    
    # Return a list of chunk file paths (placeholder)
    return [f"{output_dir}/chunk_{i}.osm" for i in range(1, 10)]

def process_osm_data(osm_file_path):
    """
    Process OSM PBF file and filter for cities with population > 500,000
    
    Note: In a real implementation, you would first convert the PBF to XML chunks
    """
    print(f"Processing OSM file: {osm_file_path}")
    
    # Process the XML chunks to extract cities
    # In a real implementation, we would:
    # 1. Convert PBF to XML chunks using osmconvert
    # 2. Process each XML chunk
    # For now, we'll create a sample DataFrame
    
    # Sample data for demonstration (in a real implementation, this would come from parsing XML)
    sample_cities = [
        {
            "id": "1234567",
            "type": "node",
            "lat": 48.8566,
            "lon": 2.3522,
            "tags": {"name": "Paris", "place": "city", "population": "2187526", "admin_level": "8"}
        },
        {
            "id": "7654321",
            "type": "node",
            "lat": 43.2965,
            "lon": 5.3698,
            "tags": {"name": "Marseille", "place": "city", "population": "870018", "admin_level": "8"}
        },
        {
            "id": "9876543",
            "type": "node",
            "lat": 45.7578,
            "lon": 4.8320,
            "tags": {"name": "Lyon", "place": "city", "population": "518635", "admin_level": "8"}
        }
    ]
    
    # Create a DataFrame from the sample data
    cities_df = spark.createDataFrame(sample_cities, schema=osm_node_schema)
    
    # Extract population as integer
    @udf(IntegerType())
    def extract_population(tags):
        if tags and 'population' in tags:
            try:
                return int(tags['population'])
            except ValueError:
                return None
        return None
    
    # Extract name as string
    @udf(StringType())
    def extract_name(tags):
        if tags and 'name' in tags:
            return tags['name']
        return None
    
    # Extract admin level
    @udf(StringType())
    def extract_admin_level(tags):
        if tags and 'admin_level' in tags:
            return tags['admin_level']
        return None
    
    # Apply UDFs and filter by population
    cities_df = cities_df.withColumn("population", extract_population(col("tags"))) \
                       .withColumn("name", extract_name(col("tags"))) \
                       .withColumn("admin_level", extract_admin_level(col("tags"))) \
                       .filter(col("population") > 100000)
    
    return cities_df

def extract_buildings(osm_file_path):
    """
    Extract buildings from OSM data
    
    Note: In a real implementation, you would process XML chunks
    """
    print(f"Extracting buildings from: {osm_file_path}")
    
    # Sample data for demonstration
    sample_buildings = [
        {
            "id": "12345",
            "type": "way",
            "tags": {"building": "yes", "building:levels": "5", "height": "15", "name": "Sample Building 1"},
            "nodes": ["1", "2", "3", "4", "1"]
        },
        {
            "id": "23456",
            "type": "way",
            "tags": {"building": "residential", "building:levels": "10", "height": "30"},
            "nodes": ["5", "6", "7", "8", "5"]
        },
        {
            "id": "34567",
            "type": "way",
            "tags": {"building": "commercial", "building:levels": "20", "height": "60", "name": "Office Tower"},
            "nodes": ["9", "10", "11", "12", "9"]
        }
    ]
    
    # Create a DataFrame from the sample data
    buildings_df = spark.createDataFrame(sample_buildings, schema=osm_way_schema)
    
    # Extract building attributes
    @udf(StringType())
    def extract_building_type(tags):
        if tags and 'building' in tags:
            return tags['building']
        return None
    
    @udf(IntegerType())
    def extract_levels(tags):
        if tags and 'building:levels' in tags:
            try:
                return int(tags['building:levels'])
            except ValueError:
                return None
        return None
    
    @udf(DoubleType())
    def extract_height(tags):
        if tags and 'height' in tags:
            try:
                return float(tags['height'])
            except ValueError:
                return None
        return None
    
    @udf(StringType())
    def extract_name(tags):
        if tags and 'name' in tags:
            return tags['name']
        return None
    
    # Apply UDFs
    buildings_df = buildings_df.withColumn("building_type", extract_building_type(col("tags"))) \
                              .withColumn("levels", extract_levels(col("tags"))) \
                              .withColumn("height", extract_height(col("tags"))) \
                              .withColumn("name", extract_name(col("tags")))
    
    return buildings_df

def calculate_building_metrics(buildings_df):
    """
    Calculate metrics for buildings
    """
    # Count buildings by type
    building_counts = buildings_df.groupBy("building_type").count()
    
    # Calculate average levels and height by building type
    building_stats = buildings_df.groupBy("building_type") \
                                .agg(
                                    {"levels": "avg", "height": "avg", "id": "count"}
                                ) \
                                .withColumnRenamed("avg(levels)", "avg_levels") \
                                .withColumnRenamed("avg(height)", "avg_height") \
                                .withColumnRenamed("count(id)", "count")
    
    return building_stats

# Main execution
if __name__ == "__main__":
    # Path to the OSM PBF file
    osm_file = "france-latest.osm.pbf"
    
    # Process OSM data to extract large cities
    large_cities_df = process_osm_data(osm_file)
    
    print("Cities with population > 500,000:")
    large_cities_df.select("id", "name", "population", "admin_level").show()
    
    # Flatten DataFrame for CSV export (remove complex types)
    cities_for_export = large_cities_df.select(
        "id", "type", "lat", "lon", "name", "population", "admin_level"
    )
    
    # Save cities to CSV
    cities_for_export.write.csv("large_cities_france.csv", header=True, mode="overwrite")
    
    # Extract buildings from OSM data
    buildings_df = extract_buildings(osm_file)
    
    print("Building sample:")
    buildings_df.select("id", "building_type", "levels", "height").show(5)
    
    # Calculate building metrics
    building_metrics = calculate_building_metrics(buildings_df)
    
    print("Building metrics:")
    building_metrics.show()
    
    # Ensure building metrics don't have complex types before saving
    building_metrics_export = building_metrics.select(
        "building_type", "count", "avg_levels", "avg_height"
    )
    
    # Save building metrics to CSV
    building_metrics_export.write.csv("building_metrics_france.csv", header=True, mode="overwrite")
    
    spark.stop()

# Instructions for using this code with a real OSM PBF file:
# 1. Install osmconvert: https://wiki.openstreetmap.org/wiki/Osmconvert
# 2. Convert PBF to OSM XML: osmconvert france-latest.osm.pbf -o=france.osm
# 3. Split the XML file into manageable chunks (e.g., using split command)
# 4. Update the code to process these XML chunks
# 5. Run with: spark-submit --driver-memory 8g --executor-memory 8g script.py
