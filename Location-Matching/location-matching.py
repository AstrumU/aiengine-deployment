# Databricks notebook source
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
        .config("spark.some.config.option", "some-value") \
        .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
        .enableHiveSupport() \
        .getOrCreate()

    # set up an account key to azure blob container
    spark.conf.set(
      "fs.azure.account.key.testdsdatalakev3.blob.core.windows.net", \
      "hzLmHtN7sk3rCyPUnWC6vVgwLz/K8bVBuZ0pq5cSlMIFp20Xve8TqBx8S3ji/J5KDLw7tBYp8bwoVKMweIC8GA=="
    )

    return spark


# set up azure blob storage container path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
student_profile_path = blobpath + "/Common_Data/Student_Profile/student_360_profile_campaign/"
campaign_feature_space_table_path = blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_table//"
campaign_rec_result_path = blobpath + "/Campaign_Recommendation/Recommendation_Result/Campaign_Rec_Result-staging/"
company_master_table_path = blobpath + "/Campaign_Recommendation/Location_Match/Company_Master_Table"
position_taxonomy_path = "/LookupTables/AstrumU_DS_Tool/Position_Taxonomy"
job_major_relation_path = "/Campaign_Recommendation/Location_Match/Job_Majors_Popular_Score"

# DBTITLE 1,Load Data
campaign_feature_space_table = spark.read.load(campaign_feature_space_table_path)
company_master_table = spark.read.load(company_master_table_path)
position_taxonomy = spark.read.load(position_taxonomy_path)
job_major_relation = spark.read.load(job_major_relation_path)

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

  student_profile = spark.read.format("delta") \
    .load(student_profile_path)\
    .filter(col("Current_Student") == True).select(
      "AstrumU_UUID", "Double_Major", "Is_In_State_Student",
      "Age_Entering_College", "Home_City_State", "University_City_State", 
      "University_MSA", "Home_MSA", "Major_L2")
  
  """
  Step 1: Using Campaign_ID to get Job category
  """
  job_role_id = raw_data['parameters']['jobRoleId']
  try:
    position_L2 = position_taxonomy.filter(
      position_taxonomy.L3 == job_role_id).collect()[0]['L2']
  except:
    pass
  else:
    job_role_id = "15-1133.00"
    position_L2 = position_taxonomy.filter(
      position_taxonomy.L3 == job_role_id).collect()[0]['L2']
  print(f"job_role_id = {job_role_id}, job_L2 = {position_L2}")
  student_profile = student_profile.withColumn("Position_L2", lit(position_L2))
  
  """
  Step 2: Get company industry & location
  """
  company_name = raw_data['companyName']
  matched_companys, matching_score, company_norm = \
    Company_SearchObj.get_matched_companys(company_name, topN=1)
  if matching_score < 0.5:
    return "Error: Company not matched"
  company_L1 = matched_companys.at[0, 'L1']
  company_location = company_master_table.filter(col("Name") == company_name) \
    .collect()[0]['Headquarters']
  matched_locations, matching_score, location_norm = Location_SearchObj \
    .get_matched_locations(company_location, topN=1)
  company_state = matched_locations.at[0, 'L1']
  company_msa = matched_locations.at[0, 'L2']
  print(f"company_name = {company_name}, company_L1 = {company_L1}")
  print(f"company_location = {company_location}, company_state = {company_state}, company_msa = {company_msa}")
  student_profile = student_profile.withColumn("Position_State", lit(company_state))
  student_profile = student_profile.withColumn("Position_MSA", lit(company_msa))
  
  """
  Step 3: Get location based features, and position-major-relations
  """
  student_profile = student_profile \
    .withColumn("Home_State", substring(col("Home_City_State"), -2, 2)) \
    .withColumn("University_State", substring(col("University_City_State"), -2, 2))
  student_profile = student_profile.withColumn(
    "First_Job_In_University_State",
    col("Position_State") == upper(col("University_State")))
  student_profile = student_profile.withColumn(
    "First_Job_In_University_MSA",
    col("Position_MSA") == col("University_MSA"))
  student_profile = student_profile.withColumn(
    "First_Job_In_Home_State",
    col("Home_State") == col("Position_State"))
  student_profile = student_profile.withColumn(
    "First_Job_In_Home_MSA",
    col("Home_MSA") == col("Position_MSA"))
  student_profile = student_profile.withColumn('Campaign_ID', lit(campaign_id))
  student_profile = student_profile.join(job_major_relation,
                                         ["Position_L2", "Major_L2"], "left")
  student_profile = student_profile.withColumn('Campaign_ID', lit(campaign_id))
#   display(student_profile)
  print(f"student_profile done ({time.time() - last:.2f} sec)")
  
  """
  Step 4: Update campaign_feature_space_table
  """
  campaign_feature_space_table = spark.read.load(campaign_feature_space_table_path)
  cond_columns = ["AstrumU_UUID", "Campaign_ID"]
  add_cols = ["Double_Major", "Is_In_State_Student", "Age_Entering_College", 
              "First_Job_In_University_State", "First_Job_In_University_MSA",
              "First_Job_In_Home_State", "First_Job_In_Home_MSA",
              "Position_Major_Popular_Score", "Major_Position_Popular_Score"]
  for add_col in add_cols:
    student_profile = student_profile.withColumn(
      add_col, student_profile[add_col].cast("double"))
    if add_col in campaign_feature_space_table.columns:
      campaign_feature_space_table = campaign_feature_space_table.drop(add_col)
  campaign_feature_space_update = campaign_feature_space_table.join(
    student_profile.select(cond_columns + add_cols), cond_columns, "left")
  
  campaign_feature_space_update.write.format("delta").mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(campaign_feature_space_table_path')
  status = f"Success."
  return status

if __name__ == "__main__":
    spark = CreateSparkSession()

    app.run(host='0.0.0.0', port=8004)

