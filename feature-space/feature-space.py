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
model = None

#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@dsdatalakev3.blob.core.windows.net" 
blobpath = "abfss://databricks-adfs-gold@dsdatalakev3.dfs.core.windows.net" 
student_profile_path = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
campaign_entity_delta_path = blobpath + "/Common_Data/Campaign_Entity/campaign_entity_delta/"
campaign_entity_json_path = blobpath + "/Common_Data/Campaign_Entity/public_campaign_entity.json"
campaign_feature_space_empty_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_empty/"
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
	    "fs.azure.account.key.dsdatalakev3.blob.core.windows.net", \
	    "2xnju/y0WIPj1JUQ7KA1HvJTaLJo3HyFMMZ/62MTa91ju6G4GYCk2Ml5bfK/IiR9ZUFHiGNQZ+V16ByV5DrP7Q=="
    )
  
    return spark


#@app.route('/featurespace', methods=['POST'])
def FeatureSpace():
    campaign_json = request.get_json()
    campaign_entity_ne = spark.read.json(sc.parallelize([campaign_json]))
    #read data
    student_profile_id = spark.read.format('delta').load(student_profile_path).select('AstrumU_UUID').distinct()
    campaign_entity_all = spark.read.format('delta').load(campaign_entity_delta_path)
    #campaign_entity_new = spark.read.format("json").load(campaign_entity_json_path)
    campaing_id_new = campaign_entity_new.select(col('id').alias('Campaign_ID')).distinct()
    campaign_feture_space_empty =  spark.read.format('delta').load(campaign_feature_space_empty_path)
    #process data
    student_campaign_id = student_profile_id.crossJoin(campaing_id_new).distinct()
    campaign_entity_all_update = campaign_entity_all.union(campaign_entity_new).distinct() 
    campaign_feature_space_update = campaign_feture_space_empty.join(student_campaign_id, "AstrumU_UUID", 'outer')
    #save the result
    campaign_entity_all_update.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(campaign_entity_delta_path)     
    campaign_feature_space_update.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(campaign_feature_space_update_path)   
       

if __name__ == "__main__":
    spark = CreateSparkSession()
    FeatureSpace()
    #student_profile_engine = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
    #student_profile_original = spark.read.format("delta").load(student_profile_engine)
    #print(student_profile_original.count())
    #FeatureSpace()

    app.run(host='0.0.0.0', port=5000)
