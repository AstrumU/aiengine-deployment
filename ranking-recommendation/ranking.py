# USAGE
# Start the server:
# 	python feature-space.py
# Submit a request via cURL:
# 	curl -X POST -F data=@campaign_entity.json 'http://localhost:5000/predict'

# import the necessary packages
from pyspark.sql import SparkSession
from flask import abort, jsonify, Flask, request, Response
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import *
from pyspark.sql.functions import desc
import json

# initialize our Flask application and the AI model
app = Flask(__name__)

#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
campaign_feature_space_table_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_table//"
campaign_rec_result_path =  blobpath + "/Campaign_Recommendation/Recommendation_Result/Campaign_Rec_Result-staging/"

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

@app.route('/rankingrec', methods=['POST'])
def rankingrec():
    #use the weight to assianment the attribute
    hard_skill_weight = "0.7"
    GPA_weight = "0.3"

    #load feature space table
    campaign_feature_space = spark.read.format("delta").load(campaign_feature_space_table_path)
    #for null values just give value 0
    campaign_feature_space_fillna = campaign_feature_space.fillna(0).dropDuplicates(['AstrumU_UUID'])
    #Compute evaluation score based on hard skill matching and gap matching result 
    campaign_feature_space_evaluation = campaign_feature_space_fillna.withColumn('Evaluation_Score', campaign_feature_space_fillna.Hard_Skill_Scoring*0.8+campaign_feature_space_fillna.GPA_Scoring*0.2)
    campaign_feature_space_evaluation = campaign_feature_space_evaluation.withColumn('Evaluation_Score', F.bround('Evaluation_Score',scale=4))

    #get the ranking number for each campaign id
    recommentdation_windowSpec = Window.partitionBy('Campaign_ID').orderBy(desc('Evaluation_Score'))
    campaign_feature_space_evaluation = campaign_feature_space_evaluation.withColumn("rank_number", row_number().over(recommentdation_windowSpec))
    campaign_rec_result = campaign_feature_space_evaluation.select('AstrumU_UUID','Campaign_ID', 'Program_ID', 'Evaluation_Score','Rank_Number', 'Student_Skills', 'Hard_Skill_ranking_list', 'Soft_Skill_ranking_list').distinct()
    campaign_rec_result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(campaign_rec_result_path)

if __name__ == "__main__":
    spark = CreateSparkSession()
    
    app.run(host='0.0.0.0', port=8003)
