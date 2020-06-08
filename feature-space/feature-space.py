# USAGE
# Start the server:
# 	python feature-space.py
# Submit a request via cURL:
# 	curl -X POST -F data=@campaign_entity.json 'http://localhost:5000/predict'

# import the necessary packages
from pyspark.sql import SparkSession
from flask import abort, jsonify, Flask, request, Response



# initialize our Flask application and the AI model
app = Flask(__name__)
model = None

#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@dsdatalakev3.blob.core.windows.net" 
student_profile_path = blobpath + "/AI_Engine/student_360_profile_engine_parquet/"
campaign_entity_json_path = blobpath + "/AI_Engine/student_360_profile_engine_parquet/"
campaign_feature_space_parquet_empty_path  =  blobpath + "/AI_Engine/campaign_feature_space_parquet_empty/'
campaign_feature_space_parquet_update_path =  blobpath + "dbfs:/mnt/databricks-gold/AI_Engine/campaign_feature_space_parquet_update/“

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
  
     return spark


@app.route('/featurespace', methods=['POST'])
def FeatureSpace():
    student_profile_id = spark.read.option("mergeSchema","true").parquet(student_profile_engine_parquet).select('AstrumU_UUID').distinct()
    campaign_entity_id = spark.read.format("json").load(campaign_entity_json_path1).select(col('id').alias('Campaign_ID')).distinct()
    campaign_feture_space_empty =  spark.read.option("mergeSchema","true").parquet(campaign_feature_space_parquet_empty_path)
    
    student_campaign_id = student_profile_id.crossJoin(campaign_entity_id).distinct()
    campaign_feature_space_update = campaign_feture_space_empty.join(student_campaign_id, "AstrumU_UUID", 'outer')
    campaign_feature_space_update.write.option("mergeSchema", "true").mode("overwrite").parquet(campaign_feature_space_parquet_update_path)   
       

if __name__ == "__main__":
    spark = CreateSparkSession()
    
    app.run(host='0.0.0.0', port=5000)
