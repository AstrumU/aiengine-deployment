# USAGE
# Start the server:
# 	python feature-space.py
# Submit a request via cURL:
# 	curl -X POST -F data=@campaign_entity.json 'http://localhost:5000/predict'

# import the necessary packages
from pyspark.sql import SparkSession
from flask import abort, jsonify, Flask, request, Response
import json


# initialize our Flask application and the AI model
app = Flask(__name__)

#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
student_profile_path = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
campaign_entity_delta_path = blobpath + "/Common_Data/Campaign_Entity/campaign_entity_delta/"
campaign_entity_json_path = blobpath + "/Common_Data/Campaign_Entity/campaign_entity.json"
campaign_feature_space_parquet_empty_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_empty/"
campaign_feature_space_update_path =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_update_table/"

# Grap Spark Session which is the entry point for the cluster resources
def CreateSparkSession():
    spark = SparkSession \
        .builder \
        .appName("AI_translation_engine") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.some.config.option", "some-value")\
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
        .enableHiveSupport() \
        .getOrCreate()

    #set up an account key to azure blob container
    spark.conf.set(
	    "fs.azure.account.key.testdsdatalakev3.blob.core.windows.net", \
	    "hzLmHtN7sk3rCyPUnWC6vVgwLz/K8bVBuZ0pq5cSlMIFp20Xve8TqBx8S3ji/J5KDLw7tBYp8bwoVKMweIC8GA=="
    )
  
    return spark


#@app.route('/featurespace', methods=['POST'])
def featurespace(raw_data):
    #read data
    raw_data = request.get_json()
    campaing_id_new = spark.read.json(sc.parallelize([raw_data])).select(col('id').alias('Campaign_ID')).distinct()
    student_profile_id = spark.read.format('delta').load(student_profile_path).select("AstrumU_UUID", "Program_ID").distinct()
    campaign_feture_space_empty =  spark.read.format('delta').load(campaign_feature_space_empty_path)
    #process data
    student_campaign_id = student_profile_id.crossJoin(campaing_id_new).distinct()
    campaign_feature_space_update = campaign_feture_space_empty.join(student_campaign_id, ["AstrumU_UUID", "Program_ID"],"outer")
    #save the result
    campaign_feature_space_update.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(campaign_feature_space_update_path)  
    status = f"Success. build campaign feature space"
    return status


if __name__ == "__main__":
    spark = CreateSparkSession()
   
    app.run(host='0.0.0.0', port=8000)
