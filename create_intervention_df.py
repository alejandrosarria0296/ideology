from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Create Intervention DataFrame") \
    .getOrCreate()

def create_intervention_df(df):
    # Explode the 'interventions' array into individual rows
    exploded_df = df.withColumn("intervention", explode(col("interventions")))
    
    # Add a unique ID for each intervention by grouping and adding row numbers
    intervention_df = exploded_df.withColumn("id", col("interventions").cast("int"))
    
    return intervention_df.select("session", "id", "intervention")

if __name__ == "__main__":
    # Define schema for CSV file
    schema = StructType([
        StructField("session", StringType(), True),
        StructField("id", IntegerType(), True),
        StructField("interventions", ArrayType(StringType()), True)
    ])
    
    # Load the CSV file
    original_df = r"data/session_texts.csv"
    df = spark.read.csv(original_df, schema=schema, header=True)
    
    # Process interventions
    intervention_df = create_intervention_df(df)
    
    # Save the result to CSV
    intervention_df.write.csv(r"data/interventions.csv", header=True, mode="overwrite")

    # Stop the Spark session
    spark.stop()