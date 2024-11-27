from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import json

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Intervention DataFrame") \
    .config("spark.sql.files.maxPartitionBytes", "256MB") \
    .config("spark.sql.files.maxRecordLength", "4096000") \
    .getOrCreate()

# Define schema for CSV file
schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("chamber", StringType(), True),
    StructField("type", StringType(), True),
    StructField("raw_text", StringType(), True),
    StructField("clean_text", StringType(), True),
    StructField("intervention_pairs", StringType(), True),
    StructField("interventions", StringType(), True)  # Read as StringType initially
])

# Load the CSV file
original_df = r"/home/asarria/ideology/data/session_texts.csv"
print("Starting Spark job...")
try:
    df = spark.read.csv(original_df, schema=schema, header=True,
                        multiLine=True, escape='"', maxCharsPerColumn=4096000)
    print("CSV file read successfully")
except Exception as e:
    print(f"Error reading CSV file: {e}")

# Collect rows for manual processing
rows = df.collect()

# Initialize a list to hold processed rows
processed_rows = []

for row in rows:
    try:
        # Parse the 'interventions' column as a JSON list of lists
        interventions = json.loads(row["interventions"].replace("'", "\""))
        
        # Add each sublist in 'interventions' as a row
        for intervention_list in interventions:
            intervention_str = ", ".join(intervention_list)  # Convert list of words to a comma-separated string
            processed_rows.append((row["id"], intervention_str))
    
    except (json.JSONDecodeError, TypeError) as e:
        print(f"Skipping problematic row with id {row['id']} due to parsing error: {e}")

# Create a DataFrame from the processed rows
intervention_df = spark.createDataFrame(processed_rows, ["session", "intervention"]) \
                       .withColumn("int_id", monotonically_increasing_id())

# Save the result to CSV
intervention_df.write.csv(r"data/interventions", header=True, mode="overwrite")
print("Number of rows in intervention DataFrame:", intervention_df.count())

# Stop the Spark session
spark.stop()