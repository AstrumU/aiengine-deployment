# USAGE
# Start the server:
# 	python feature-space.py
# Submit a request via cURL:
# 	curl -X POST -F data=@campaign_entity.json 'http://localhost:5000/predict'

# import the necessary packages
from pyspark.sql import SparkSession
from flask import abort, jsonify, Flask, request, Response
import os


# initialize our Flask application and the AI model
app = Flask(__name__)
model = None

# Grap Spark Session which is the entry point for the cluster resources
def CreateSparkSession():
    spark = SparkSession \
        .builder \
        .appName("AI_translation_engine") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.some.config.option", "some-value")\
        .config("spark.executor.memory","8g") \
        .enableHiveSupport() \
        .getOrCreate()

    #set up an account key to azure blob container
    spark.conf.set(
	    "fs.azure.account.key.dsdatalakev3.blob.core.windows.net", \
	    "2xnju/y0WIPj1JUQ7KA1HvJTaLJo3HyFMMZ/62MTa91ju6G4GYCk2Ml5bfK/IiR9ZUFHiGNQZ+V16ByV5DrP7Q=="
    )
    #set up azure blob storage container path
    Setpath()   
    return spark

def Setpath():
    global blobpath
    blobpath = "wasbs://databricks-gold@dsdatalakev3.blob.core.windows.net" 

@app.route('/feature-space', methods=['POST'])



if __name__ == "__main__":
    spark = CreateSparkSession()
    readcsvpath = blobpath + "/Common_Data/airports.csv"
    
    student_profile = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    student_profile_engine.write.option("mergeSchema", "true").mode("overwrite").parquet(student_profile_engine_parquet)

    app.run(host='0.0.0.0', port=8080)


    df = spark.read.option("header", "true")\
          .option("delimiter", ",")\
          .option("inferSchema", "true")\
          .csv(readcsvpath)

    df.registerTempTable("airlines")
    result = spark.sql("""
          select state
          from airlines    
     """)

    #result.write.mode("overwrite").parquet("wasbs://databricks-gold@dsdatalakev3.blob.core.windows.net/Common_Data/airport-result.parquet")
    #sdf.show(sdf)
    print(df)
