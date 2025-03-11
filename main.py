import osmnx as ox
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, sqrt, pow, when, udf
from pyspark.sql.types import IntegerType, FloatType, StringType, StructType, StructField
import time
import json
import logging
import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Spark session
spark = SparkSession.builder \
    .appName("Urban Accessibility") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# spark.sparkContext.setLogLevel("INFO")

def fetch_city_data(city_name):
    """Fetch buildings, transport, and green space data from OpenStreetMap."""
    # Fetch buildings with additional attributes including year
    try:
        buildings = ox.features_from_place(city_name, tags={"building": True}).to_crs(2154)
        print(f"Successfully fetched {len(buildings)} buildings")
        
        # Debug information about the data structure
        print("Building data columns:", buildings.columns.tolist())
        if len(buildings) > 0:
            sample_row = buildings.iloc[0]
            print("Sample building data structure:")
            for col in buildings.columns:
                print(f"  {col}: {type(sample_row[col])}")
    except Exception as e:
        print(f"Error fetching buildings: {str(e)}")
        buildings = pd.DataFrame()
    
    # Fetch transport nodes
    transport_modes = {
        "bus": {"highway": "bus_stop"},
        "train": {"railway": "station"},
        "tram": {"railway": "tram_stop"},
        "subway": {"railway": "subway_entrance"},
    }
    transport_data = []
    
    for mode, tags in transport_modes.items():
        try:
            logger.info(f"Fetching {mode} stops")
            gdf = ox.features_from_place(city_name, tags)
            if not gdf.empty:
                gdf = gdf.to_crs(2154).assign(transport_mode=mode)
                transport_data.append(gdf)
                print(f"Found {len(gdf)} {mode} stops")
            else:
                print(f"No {mode} stops found")
        except Exception as e:
            print(f"Failed to fetch {mode} stops: {str(e)}")
    
    transport_stops = pd.concat(transport_data) if transport_data else pd.DataFrame()
    
    # Fetch green spaces
    try:
        green_spaces = ox.features_from_place(city_name, tags={"landuse": ["park", "forest", "recreation_ground", "meadow"]})
        green_spaces = pd.concat([green_spaces, 
                               ox.features_from_place(city_name, tags={"leisure": ["park", "garden"]})]) if not green_spaces.empty else \
                      ox.features_from_place(city_name, tags={"leisure": ["park", "garden"]})
        green_spaces = green_spaces.to_crs(2154)
        print(f"Found {len(green_spaces)} green spaces")
    except Exception as e:
        print(f"Failed to fetch green spaces: {str(e)}")
        green_spaces = pd.DataFrame()
    
    # Fetch street network
    try:
        streets = ox.graph_from_place(city_name)
        streets_gdf = ox.graph_to_gdfs(streets, nodes=False, edges=True)
        streets_gdf = streets_gdf.to_crs(2154)
        print(f"Found {len(streets_gdf)} street segments")
    except Exception as e:
        print(f"Failed to fetch streets: {str(e)}")
        streets_gdf = pd.DataFrame()
    
    return buildings, transport_stops, green_spaces, streets_gdf

def parse_year(year_str):
    """Parse year from OSM tags, handling various formats."""
    if pd.isna(year_str):
        return None
    
    try:
        # Try to parse as integer
        year = int(year_str)
        # Basic validation (buildings unlikely before 1000 CE, and not in the future)
        current_year = datetime.datetime.now().year
        if 1000 <= year <= current_year:
            return year
    except (ValueError, TypeError):
        # Try to extract year from more complex formats
        try:
            # Look for 4-digit patterns that could be years
            import re
            year_match = re.search(r'\b(1\d{3}|20\d{2})\b', str(year_str))
            if year_match:
                year = int(year_match.group(1))
                current_year = datetime.datetime.now().year
                if 1000 <= year <= current_year:
                    return year
        except:
            pass
    
    return None

def prepare_dataframes(buildings, transport_stops, green_spaces, streets):
    """Extract and prepare data for analysis."""
    # Extract year built information
    years = []
    
    if len(buildings) == 0:
        print("No building data available")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    # Try multiple approaches to extract year information
    try:
        for i, building in buildings.iterrows():
            year = None
            
            # Approach 1: Check if building has a tags attribute that's a dictionary
            if hasattr(building, 'tags') and isinstance(building.tags, dict):
                for tag in ['start_date', 'year_built', 'year', 'construction:date', 'built']:
                    if tag in building.tags and building.tags[tag]:
                        year = parse_year(building.tags[tag])
                        if year:
                            break
            
            # Approach 2: Check if tags is a column in the dataframe
            elif 'tags' in buildings.columns:
                tags = building['tags']
                # Handle different formats of tags
                if isinstance(tags, dict):
                    tag_dict = tags
                elif isinstance(tags, str):
                    try:
                        tag_dict = eval(tags)  # Convert string to dict if possible
                    except:
                        tag_dict = {}
                else:
                    tag_dict = {}
                
                for tag in ['start_date', 'year_built', 'year', 'construction:date', 'built']:
                    if tag in tag_dict and tag_dict[tag]:
                        year = parse_year(tag_dict[tag])
                        if year:
                            break
            
            # Approach 3: Check if year-related columns exist directly
            else:
                for tag in ['start_date', 'year_built', 'year', 'construction:date', 'built']:
                    if tag in buildings.columns and pd.notna(building[tag]):
                        year = parse_year(building[tag])
                        if year:
                            break
            
            years.append(year)
    except Exception as e:
        print(f"Error extracting year data: {str(e)}")
        years = [None] * len(buildings)
    
    # Calculate building areas
    building_areas = buildings.geometry.area
    
    # Prepare buildings dataframe
    buildings_simple = pd.DataFrame({
        'building_id': [f"building_{i}" for i in range(len(buildings))],
        'bldg_x': buildings.geometry.centroid.x,
        'bldg_y': buildings.geometry.centroid.y,
        'building_area': building_areas,
        'year_built': years
    }).dropna(subset=['bldg_x', 'bldg_y'])  # Only drop rows with missing coordinates
    
    # Prepare transport dataframe
    transport_simple = pd.DataFrame()
    if not transport_stops.empty:
        transport_simple = pd.DataFrame({
            'stop_id': [f"stop_{i}" for i in range(len(transport_stops))],
            'transport_mode': transport_stops['transport_mode'],
            'stop_x': transport_stops.geometry.centroid.x,
            'stop_y': transport_stops.geometry.centroid.y
        }).dropna(subset=['stop_x', 'stop_y'])
    
    # Prepare green spaces dataframe
    green_simple = pd.DataFrame()
    if not green_spaces.empty:
        green_simple = pd.DataFrame({
            'green_id': [f"green_{i}" for i in range(len(green_spaces))],
            'green_area': green_spaces.geometry.area,
            'green_x': green_spaces.geometry.centroid.x,
            'green_y': green_spaces.geometry.centroid.y
        }).dropna(subset=['green_x', 'green_y'])
    
    # Prepare streets dataframe
    streets_simple = pd.DataFrame()
    if not streets.empty:
        streets_simple = pd.DataFrame({
            'street_id': [f"street_{i}" for i in range(len(streets))],
            'street_length': streets.geometry.length,
            'street_width': streets.apply(lambda x: float(x.get('width', 5)) if x.get('width') and str(x.get('width')).replace('.', '', 1).isdigit() else 5, axis=1)
        })
    
    return buildings_simple, transport_simple, green_simple, streets_simple

def calculate_accessibility(buildings_df, transport_df, max_distance=5000):
    """Calculate transport accessibility metrics."""
    if transport_df.count() == 0:
        logger.warning("No transport stops found. Creating dummy results.")
        return buildings_df.withColumn("nearest_distance", F.lit(float('inf'))) \
                          .withColumn("nearby_modes", F.lit(0))
    
    joined_df = buildings_df.crossJoin(transport_df) \
        .withColumn("distance", 
                    sqrt(pow(buildings_df.bldg_x - transport_df.stop_x, 2) + 
                         pow(buildings_df.bldg_y - transport_df.stop_y, 2)))
    
    filtered_df = joined_df.filter(joined_df.distance <= max_distance)
    
    min_distances = filtered_df.groupBy("building_id", "bldg_x", "bldg_y", "building_area", "year_built") \
        .agg(F.min("distance").alias("nearest_distance"))
    
    modes_df = filtered_df.filter(filtered_df.distance <= 100) \
        .groupBy("building_id") \
        .agg(F.countDistinct("transport_mode").alias("nearby_modes"))
    
    # Join and handle missing values, but use 0 instead of None for year_built
    result = min_distances.join(modes_df, "building_id", "left_outer") \
        .fillna(0, subset=["nearby_modes"])
    
    return result

def calculate_green_accessibility(buildings_df, green_df, max_distance=1000):
    """Calculate green space accessibility metrics."""
    if green_df.count() == 0:
        logger.warning("No green spaces found. Creating dummy results.")
        return buildings_df.withColumn("nearest_green_distance", F.lit(float('inf'))) \
                          .withColumn("nearby_green_area", F.lit(0))
    
    joined_df = buildings_df.crossJoin(green_df) \
        .withColumn("green_distance", 
                    sqrt(pow(buildings_df.bldg_x - green_df.green_x, 2) + 
                         pow(buildings_df.bldg_y - green_df.green_y, 2)))
    
    filtered_df = joined_df.filter(joined_df.green_distance <= max_distance)
    
    min_distances = filtered_df.groupBy("building_id") \
        .agg(F.min("green_distance").alias("nearest_green_distance"))
    
    nearby_green = filtered_df.filter(filtered_df.green_distance <= 500) \
        .groupBy("building_id") \
        .agg(F.sum("green_area").alias("nearby_green_area"))
    
    result = min_distances.join(nearby_green, "building_id", "left_outer") \
        .fillna(0, subset=["nearby_green_area"])
    
    return result

def calculate_building_age_distribution(buildings_df):
    """Calculate building age distribution."""
    # Check if year_built column exists
    has_year_column = "year_built" in buildings_df.columns
    
    if not has_year_column:
        logger.warning("No year_built column found in the data.")
        schema = StructType([
            StructField("period", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("percentage", FloatType(), True)
        ])
        return spark.createDataFrame([], schema)
    
    # Filter out buildings with no year data
    buildings_with_year = buildings_df.filter(col("year_built").isNotNull() & (~F.isnan(col("year_built"))))
    
    # If no buildings have year data, return empty dataframe
    if buildings_with_year.count() == 0:
        logger.warning("No buildings with valid year data found.")
        schema = StructType([
            StructField("period", StringType(), True),
            StructField("count", IntegerType(), True),
            StructField("percentage", FloatType(), True)
        ])
        return spark.createDataFrame([], schema)
    
    # Define age periods
    current_year = datetime.datetime.now().year
    
    @udf(returnType=StringType())
    def get_age_period(year):
        if year is None or pd.isna(year) or year == 0:
            return "Unknown"
        
        try:
            year_int = int(year)
            if year_int < 1800:
                return "Pre-1800"
            elif year_int < 1850:
                return "1800-1849"
            elif year_int < 1900:
                return "1850-1899"
            elif year_int < 1945:
                return "1900-1944"
            elif year_int < 1970:
                return "1945-1969"
            elif year_int < 1990:
                return "1970-1989"
            elif year_int < 2000:
                return "1990-1999"
            elif year_int < 2010:
                return "2000-2009"
            else:
                return "2010-present"
        except (ValueError, TypeError):
            return "Unknown"
    
    # Calculate distribution
    age_distribution = buildings_df.withColumn("age_period", get_age_period(col("year_built")))
    
    # Count buildings by period
    period_counts = age_distribution.groupBy("age_period").count()
    
    # Calculate percentages
    total_buildings = buildings_df.count()
    distribution = period_counts.withColumn(
        "percentage", 
        (col("count") / total_buildings * 100).cast(FloatType())
    ).orderBy("age_period")
    
    return distribution

def calculate_city_metrics(buildings_df, transport_df, green_df, streets_df):
    """Calculate overall city-level metrics."""
    city_metrics = {}
    
    # Total building count and average area
    total_buildings = buildings_df.count()
    avg_building_area = buildings_df.agg(F.avg("building_area")).collect()[0][0]
    city_metrics["total_buildings"] = total_buildings
    city_metrics["avg_building_area"] = float(avg_building_area) if avg_building_area else 0
    
    # Transport metrics
    if transport_df.count() > 0:
        avg_distance = buildings_df.agg(F.avg("nearest_distance")).collect()[0][0]
        city_metrics["avg_transport_distance"] = float(avg_distance) if avg_distance else float('inf')
        
        # Transport mode distribution
        mode_counts = transport_df.groupBy("transport_mode").count()
        city_metrics["transport_mode_distribution"] = {row["transport_mode"]: row["count"] for row in mode_counts.collect()}
        
        # Transit accessibility percentage (buildings within 600m of transit)
        buildings_near_transit = buildings_df.filter(col("nearby_modes") > 0).count()
        city_metrics["transit_accessibility_pct"] = (buildings_near_transit / total_buildings * 100) if total_buildings > 0 else 0
    else:
        city_metrics["avg_transport_distance"] = float('inf')
        city_metrics["transport_mode_distribution"] = {}
        city_metrics["transit_accessibility_pct"] = 0
    
    # Green space metrics
    if green_df.count() > 0 and "nearby_green_area" in buildings_df.columns:
        total_green_area = green_df.agg(F.sum("green_area")).collect()[0][0]
        city_area = (buildings_df.agg(F.max("bldg_x") - F.min("bldg_x")).collect()[0][0] * 
                    buildings_df.agg(F.max("bldg_y") - F.min("bldg_y")).collect()[0][0])
        
        city_metrics["total_green_area"] = float(total_green_area) if total_green_area else 0
        city_metrics["green_space_ratio"] = float(total_green_area / city_area) if total_green_area and city_area else 0
        
        # Green accessibility percentage (buildings within 500m of green space)
        buildings_near_green = buildings_df.filter(col("nearby_green_area") > 0).count()
        city_metrics["green_accessibility_pct"] = (buildings_near_green / total_buildings * 100) if total_buildings > 0 else 0
    else:
        city_metrics["total_green_area"] = 0
        city_metrics["green_space_ratio"] = 0
        city_metrics["green_accessibility_pct"] = 0
    
    # Building-to-street ratio if street data available
    if streets_df.count() > 0:
        total_street_area = streets_df.agg(F.sum(col("street_length") * col("street_width"))).collect()[0][0]
        total_building_area = buildings_df.agg(F.sum("building_area")).collect()[0][0]
        
        city_metrics["building_to_street_ratio"] = float(total_building_area / total_street_area) if total_building_area and total_street_area else 0
    else:
        city_metrics["building_to_street_ratio"] = 0
    
    # Building age metrics if available
    if "year_built" in buildings_df.columns:
        buildings_with_year = buildings_df.filter(col("year_built").isNotNull() & (col("year_built") > 0)).count()
        if buildings_with_year > 0:
            avg_year = buildings_df.filter(col("year_built") > 0).agg(F.avg("year_built")).collect()[0][0]
            median_year = buildings_df.filter(col("year_built") > 0).approxQuantile("year_built", [0.5], 0.05)[0]
            oldest_year = buildings_df.filter(col("year_built") > 0).agg(F.min("year_built")).collect()[0][0]
            newest_year = buildings_df.filter(col("year_built") > 0).agg(F.max("year_built")).collect()[0][0]
            
            city_metrics["buildings_with_year_data_pct"] = (buildings_with_year / total_buildings * 100) if total_buildings > 0 else 0
            city_metrics["avg_building_year"] = float(avg_year) if avg_year else None
            city_metrics["median_building_year"] = float(median_year) if median_year else None
            city_metrics["oldest_building_year"] = float(oldest_year) if oldest_year else None
            city_metrics["newest_building_year"] = float(newest_year) if newest_year else None
        else:
            city_metrics["buildings_with_year_data_pct"] = 0
            city_metrics["avg_building_year"] = None
            city_metrics["median_building_year"] = None
            city_metrics["oldest_building_year"] = None
            city_metrics["newest_building_year"] = None
    
    return city_metrics

def create_geojson(results_df):
    """Create GeoJSON from building results with all available metrics."""
    # Get all available columns
    all_columns = results_df.columns
    
    # Define which columns to include in properties
    property_columns = [col for col in all_columns if col not in ['building_id', 'bldg_x', 'bldg_y']]
    
    # Create a dynamically built struct for properties
    struct_fields = []
    
    # Add building_id as id
    struct_fields.append(col("building_id").alias("id"))
    
    # Add all other columns with appropriate renaming
    for column in property_columns:
        # Apply special handling for specific columns
        if column == "nearest_distance":
            struct_fields.append(col(column).alias("transit_distance"))
        elif column == "nearby_modes":
            struct_fields.append(col(column).alias("transit_modes"))
        elif column == "nearest_green_distance":
            struct_fields.append(
                F.when(col(column).isNotNull(), col(column)).otherwise(lit(float('inf'))).alias("green_distance")
            )
        else:
            # Keep original name for other columns
            struct_fields.append(col(column))
    
    # Select all metrics for each building
    features_df = results_df.select(
        lit("Feature").alias("type"),
        F.struct(
            lit("Point").alias("type"),
            F.array(col("bldg_x"), col("bldg_y")).alias("coordinates")
        ).alias("geometry"),
        F.struct(*struct_fields).alias("properties")
    )
    
    features = features_df.collect()
    geojson = {"type": "FeatureCollection", "features": [row.asDict(recursive=True) for row in features]}
    
    return geojson

import math
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, float):
            if math.isnan(obj):
                return None  # Convert NaN to null in JSON
            if math.isinf(obj):
                return None  # Convert infinity to null as well
        return super().default(obj)

def main():
    city_name = "Vienna, Austria"
    output_prefix = city_name.split(',')[0].lower()
    
    try:
        # Data preparation
        logger.info(f"Fetching data for {city_name}")
        buildings, transport_stops, green_spaces, streets = fetch_city_data(city_name)
        
        # Check if we have building data
        if len(buildings) == 0:
            logger.error("No building data was retrieved. Exiting.")
            return
            
        buildings_simple, transport_simple, green_simple, streets_simple = prepare_dataframes(
            buildings, transport_stops, green_spaces, streets
        )
        
        # Check if data preparation was successful
        if len(buildings_simple) == 0:
            logger.error("Building data preparation failed. Exiting.")
            return
            
        # Log the schema of the prepared dataframes
        print(f"Buildings dataframe columns: {buildings_simple.columns.tolist()}")
        print(f"Transport dataframe columns: {transport_simple.columns.tolist() if not transport_simple.empty else []}")
        
        # Create Spark dataframes with error handling
        try:
            buildings_df = spark.createDataFrame(buildings_simple).cache()
            
            transport_df = spark.createDataFrame(transport_simple) if not transport_simple.empty else \
                          spark.createDataFrame([], schema="stop_id string, transport_mode string, stop_x double, stop_y double")
            transport_df.cache()
            
            green_df = spark.createDataFrame(green_simple) if not green_simple.empty else \
                      spark.createDataFrame([], schema="green_id string, green_area double, green_x double, green_y double")
            green_df.cache()
            
            streets_df = spark.createDataFrame(streets_simple) if not streets_simple.empty else \
                        spark.createDataFrame([], schema="street_id string, street_length double, street_width double")
            streets_df.cache()
        except Exception as e:
            logger.error(f"Error creating Spark dataframes: {str(e)}")
            return
        
        # Calculate accessibility metrics
        logger.info("Calculating transport accessibility")
        transport_results = calculate_accessibility(buildings_df, transport_df)
        transport_results.cache()
        
        # Calculate green space accessibility
        logger.info("Calculating green space accessibility")
        green_results = calculate_green_accessibility(buildings_df, green_df)
        green_results.cache()
        
        # Join all results together
        results = transport_results.join(
            green_results, "building_id", "left_outer"
        )
        results.cache()
        
        # Calculate city-level metrics
        logger.info("Calculating city-level metrics")
        city_metrics = calculate_city_metrics(results, transport_df, green_df, streets_df)
        
        # Calculate building age distribution for city metrics - with error handling
        logger.info("Calculating building age distribution")
        try:
            age_distribution = calculate_building_age_distribution(results)
            age_distribution_data = age_distribution.collect()
            # Add age distribution to city metrics
            city_metrics["building_age_distribution"] = [row.asDict() for row in age_distribution_data]
        except Exception as e:
            logger.warning(f"Error calculating building age distribution: {str(e)}")
            city_metrics["building_age_distribution"] = []
        
        # Create GeoJSON output (building-level data)
        logger.info("Creating GeoJSON output")
        geojson = create_geojson(results)
        
        # Save output files
        logger.info("Saving output files")
        
        # 1. Main GeoJSON file with building-level metrics
        with open(f"{output_prefix}.geojson", "w") as f:
            json.dump(geojson, f)
            print(f"Saved building-level metrics to {output_prefix}.geojson")
        
        # 2. City-level metrics JSON file
        with open(f"{output_prefix}_city_metrics.json", "w") as f:
            json.dump(city_metrics, f, cls=CustomJSONEncoder)
            print(f"Saved city-level metrics to {output_prefix}_city_metrics.json")
        
        # Print summary of outputs
        print(f"\nAnalysis complete for {city_name}")
        print(f"Analyzed {results.count()} buildings")
        print(f"Found {transport_df.count()} transit stops and {green_df.count()} green spaces")
        
        # Print a few key city metrics
        print("\nKey City Metrics:")
        metrics_to_show = [
            "avg_transport_distance", 
            "transit_accessibility_pct",
            "green_accessibility_pct", 
            "building_to_street_ratio"
        ]
        for metric in metrics_to_show:
            if metric in city_metrics:
                print(f"  {metric}: {city_metrics[metric]}")
        
        spark.stop()
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        spark.stop()

if __name__ == "__main__":
    main()