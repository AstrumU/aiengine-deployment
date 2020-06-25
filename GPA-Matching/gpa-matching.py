# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from astrumu_ds_tools import Company_SearchObj
from flask import abort, jsonify, Flask, request, Response
import time
import json

# initialize our Flask application and the AI model
app = Flask(__name__)

#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
student_profile_path = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
campaign_feature_space_table_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_table/"
campaign_infor_path = blobpath + "/Common_Data/Campaign_Entity/campaign_entity_new_delta/"

position_taxonomy_path =  blobpath + "/LookupTables/AstrumU_DS_Tool/Position_Taxonomy/"
lookup_table_adjust_path = blobpath + "/LookupTables/ViewMapping/Position_L1_Company_L1_Adjust/"
lookup_table_default_path = blobpath + "/LookupTables/ViewMapping/Position_L1_Company_L1_Default/"

# Grap Spark Session which is the entry point for the cluster resources
def CreateSparkSession():
    spark = SparkSession \
        .builder \
        .appName("AI_translation_engine") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.some.config.option", "some-value")\
        .config("spark.debug.maxToStringFields", 1000)\
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
        .getOrCreate()
    
    #definaiton configation for delta lake file
    conf = spark.sparkContext._conf.setAll([('spark.delta.logStore.class','org.apache.spark.sql.delta.storage.AzureLogStore')])  
    spark.sparkContext._conf.getAll()

    #set up an account key to azure blob container
    spark.conf.set(
	    "fs.azure.account.key.testdsdatalakev3.blob.core.windows.net", \
	    "hzLmHtN7sk3rCyPUnWC6vVgwLz/K8bVBuZ0pq5cSlMIFp20Xve8TqBx8S3ji/J5KDLw7tBYp8bwoVKMweIC8GA=="
    )
  
    return spark

def join_update_columns(df1, df2, cond, col_update, how='left'):
  df_update_temp = df1.select(df1[col_update].alias("col_temp"), "AstrumU_UUID", "Campaign_ID") 
  df3 = df2.join(df_update_temp, cond, 'left')
  df3 = df3.withColumn(col_update, coalesce("col_temp", col_update)).drop("col_temp")        
  return df3


# COMMAND ----------
@app.route("/")
def hello():
    return "Hello from GPA Matching!"

# DBTITLE 1,Micro-service entry script
@app.route('/gpamatching', methods=['POST'])
def gpamatching():
  """
  raw_data: Campaign_ID, Program_ID
  Return: updated campaign_feature_space_table with GPA_Scoring
  """
  raw_data = request.get_json()
  last = time.time()
  status = "Unkown error"
  campaign_id, program_id = raw_data['id'], raw_data['Program_ID']
  
  """
  Step 1: Using Campaign_ID to get Company Industry and Job role category
  """
  company_name = campaign_infor.at[campaign_id, 'companyName']
  matched_companys, matching_score, company_norm = Company_SearchObj.get_matched_companys(company_name, topN=1)
  if matching_score < 0.5:
    return "Error: Company not matched"
  company_L1 = matched_companys.at[0, 'L1']
  #company_size = matched_companys.at[0, 'L4']
  job_role_id = campaign_infor.at[campaign_id, 'jobRoleId']
  if job_role_id is None:
    job_role_id = "15-1133.00"
  
  job_L1 = position_taxonomy.at[job_role_id, "L1"]
  
  """
  Step 2: Adjust GPA based on requested GPA or get default GPA if not requested
  """
  gpa_request = campaign_infor.at[campaign_id, 'gpa']
  if gpa_request is None or gpa_request == '{}':  # case 1: no GPA request
    adjusted_gpa = lookup_table_default.loc[job_L1].at[company_L1, "median"]
  else:  # case 2: company has GPA request
    adjusted_gpa = lookup_table_adjust.loc[job_L1].loc[company_L1].at[gpa_request, "median"]
  if adjusted_gpa == 'None':
    adjusted_gpa = 1000
  else:  
    adjusted_gpa = float(adjusted_gpa)                   
  last = time.time()
  
  """
  Step 3: Compute GPA_Scoring = exp(-|student_weighted_GPA - adjusted_GPA_request|)
  GPA_Scoring ranges 0~1. Closer to 1 means a better match.
  """
  student_profile =  student_profile_load\
    .filter(col("Current_Student") == True)\
    .groupby("Student_UUID", "Term_Year")\
    .agg(first("AstrumU_UUID").alias("AstrumU_UUID"),mean("Weighted_GPA").alias("Weighted_GPA"))\
    .select("AstrumU_UUID", "Weighted_GPA").fillna(1000)\
    .withColumn('GPA_Scoring', exp(-abs(col('Weighted_GPA') - adjusted_gpa)).cast("float"))\
    .withColumn('Campaign_ID', lit(campaign_id))  
  """
  Step 4: Update campaign_feature_space_table
  """
  cond_columns = ["AstrumU_UUID", "Campaign_ID"]    
  campaign_feature_space_update = join_update_columns(
    student_profile, campaign_feature_space_table, cond_columns, 'GPA_Scoring')
  campaign_feature_space_update.write.format("delta").mode("overwrite")\
    .save(campaign_feature_space_table_path)
  status = f"Success. Adjusted GPA = {adjusted_gpa:.2f}"
  return status

if __name__ == "__main__":
    spark = CreateSparkSession()
    # DBTITLE 1,Load Data
    student_profile_load = spark.read.format("delta").load(student_profile_path)
    print("student_profile_load.count()")
    #campaign_infor = spark.read.load(campaign_infor_path)\
    #  .select('id', 'companyName', json_tuple('parameters', 'gpa', 'jobRoleId').alias('gpa', 'jobRoleId'))\
    #  .toPandas().set_index("id")
    #campaign_infor.index = campaign_infor.index.astype(int)
    #campaign_feature_space_table = spark.read.load(campaign_feature_space_table_path)
    #position_taxonomy = spark.read.load(position_taxonomy_path).toPandas().drop_duplicates(subset=["L3"]).set_index("L3")
    #lookup_table_adjust = spark.read.load(lookup_table_adjust_path).toPandas().set_index(["position_L1", "company_L1", "Raw_GPA"])
    #lookup_table_default = spark.read.load(lookup_table_default_path).toPandas().set_index(["position_L1", "company_L1"])

    #app.run(host='0.0.0.0')

