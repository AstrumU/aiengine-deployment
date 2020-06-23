# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from astrumu_ds_tools import Location_SearchObj
import time
import json

# initialize our Flask application and the AI model
app = Flask(__name__)

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


#set up azure blob storage container path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
student_profile_path = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
campaign_feature_space_table_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_table//"
campaign_rec_result_path =  blobpath + "/Campaign_Recommendation/Recommendation_Result/Campaign_Rec_Result-staging/"
position_taxonomy_path =  blobpath + "/LookupTables/AstrumU_DS_Tool/Position_Taxonomy"
lookup_table_adjust_path = blobpath + "/LookupTables/ViewMapping/Position_L1_Company_L1_Adjust"
lookup_table_default_path = blobpath + "/LookupTables/ViewMapping/Position_L1_Company_L1_Default"

# DBTITLE 1,Load Data
campaign_feature_space_table = spark.read.load(campaign_feature_space_table_path)
position_taxonomy = spark.read.load(position_taxonomy_path)
lookup_table_adjust = spark.read.load(lookup_table_adjust_path)
lookup_table_default = spark.read.load(lookup_table_default_path)

# COMMAND ----------

# DBTITLE 1,Micro-service entry script
@app.route('/location-matching', methods=['POST'])
def run():
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
  company_name = raw_data['companyName']
  matched_companys, matching_score, company_norm = Company_SearchObj.get_matched_companys(company_name, topN=1)
  if matching_score < 0.5:
    return "Error: Company not matched"
  company_L1 = matched_companys.at[0, 'L1']
  #company_size = matched_companys.at[0, 'L4']
  job_role_id = raw_data['parameters']['jobRoleId']
  try:
    job_L1 = position_taxonomy.filter(position_taxonomy.L3 == job_role_id).collect()[0]['L1']
  except:
    pass
  else:
    job_role_id = "15-1133.00"
    job_L1 = position_taxonomy.filter(position_taxonomy.L3 == job_role_id).collect()[0]['L1']
  print(f"company_name = {company_name}, company_L1 = {company_L1}, job_role_id = {job_role_id}, job_L1 = {job_L1}")
  
  """
  Step 2: Adjust GPA based on requested GPA or get default GPA if not requested
  """
  gpa_request = raw_data['parameters']['gpa']
  if gpa_request is None or gpa_request == '{}':  # case 1: no GPA request
    adjusted_gpa = lookup_table_default.filter(col("position_L1") == job_L1) \
      .filter(col("company_L1") == company_L1).collect()[0]["median"]
  else:  # case 2: company has GPA request
    adjusted_gpa = lookup_table_adjust.filter(col("position_L1") == job_L1) \
      .filter(col("company_L1") == company_L1).filter(col("Raw_GPA") == gpa_request).collect()[0]["median"]
  if adjusted_gpa == 'None':
    adjusted_gpa = 1000
  else:  
    adjusted_gpa = float(adjusted_gpa)
  print(f"gpa_request = {gpa_request}, adjusted_gpa = {adjusted_gpa} ({time.time() - last:.2f}) sec")
  last = time.time()
  
  """
  Step 3: Compute GPA_Scoring = exp(-|student_weighted_GPA - adjusted_GPA_request|)
  GPA_Scoring ranges 0~1. Closer to 1 means a better match.
  """
  student_profile = spark.read.format("delta")\
    .load(student_profile_path)\
    .filter(col("Current_Student") == True)\
    .groupby("Student_UUID", "Term_Year")\
    .agg(first("AstrumU_UUID").alias("AstrumU_UUID"),mean("Weighted_GPA").alias("Weighted_GPA"))\
    .select("AstrumU_UUID", "Weighted_GPA").fillna(1000)\
    .withColumn('GPA_Scoring', exp(-abs(col('Weighted_GPA') - adjusted_gpa)).cast("float"))\
    .withColumn('Campaign_ID', lit(campaign_id))
#   display(student_profile.orderBy(desc("GPA_Scoring")))
  print(f"student_profile done ({time.time() - last:.2f} sec)")
  
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

    app.run(host='0.0.0.0', port=8003)

