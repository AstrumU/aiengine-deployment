# Databricks notebook source
import sys
import math
import pandas as pd
import numpy as np
import pickle as pkl
import json
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import *
from sklearn.metrics.pairwise import cosine_similarity
from pyspark.mllib.linalg import Vector, Vectors
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from scipy.spatial import distance
from functools import reduce
from pyspark.sql import DataFrame
import requests, timeit

# COMMAND ----------
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

#set up all data path
blobpath = "wasbs://databricks-gold@testdsdatalakev3.blob.core.windows.net" 
campaign_feature_space_path  =  blobpath + "/Campaign_Recommendation/Common_Data/campaign_feature_space_table//"
classes_path = blobpath + "/temp/classes.txt'
Student_Vectors_Campaign_path = blobpath + "/Campaign_Recommendation/Hard_Skill_Match/Student_Vectors_Campaign/'

# COMMAND ----------

@app.route('/hardskill', methods=['POST'])
def hardskill():
  raw_data = request.get_json()    
  #read and test the campaign entity infor
  campaign_id, program_id, description = raw_data['id'], raw_data['Program_ID'], raw_data['description']
  
  labels = pd.read_csv(classes_path, header=None)
  labels = [label[0] for label in labels.values]

  processed_labels = [label.replace(' ', '-').replace('&', '-') for label in labels]
  response = requests.post(
              url='http://104.43.131.200:80/api/v1/service/job-description-vectorizer/score', data=description.encode('utf-8'),
              headers={"Content-type": "application/json", 'Authorization': 'Bearer {}'.format('eYbTN14yVYFbCzj7uFLSYx71rSSbnexf')})
  values = [float(value) for value in response.text.strip('][').split(', ')]
  vector = {labels[i]: values[i] for i in range(len(values))} 

  processed_vector = {}
  for key in vector.keys():
    processed_vector[key.replace(' ', '-').replace('&', '-')] = vector[key]

  campaign_entity_json_binary = raw_data
  campaign_entity_json_binary['vector'] = processed_vector
  campaign_entity_json = json.dumps(campaign_entity_json_binary)

  campaign_entity_df = spark.read.json(sc.parallelize([campaign_entity_json]))
  
  def sigmoid(x):
    return 1 / (1 + math.exp(-x))
  
  campaign_feature_space_table = spark.read.format("delta").load(Student_Vectors_Campaign_path)
  last_year_student_window = Window.partitionBy('AstrumU_UUID').orderBy(campaign_feature_space_table.Term_Year.desc())
  campaign_feature_space_table = campaign_feature_space_table.withColumn('topn', F.row_number().over(last_year_student_window))
  student_course_existing = campaign_feature_space_table.where(campaign_feature_space_table.topn==1)
  student_profiles_with_programs = student_course_existing.where(F.col('Program_ID').isNotNull())
  campaign_feature_space_table_df = pd.DataFrame(columns=campaign_feature_space_table.columns)
  campaign_entity_original = campaign_entity_df
  campaign_entity_original_df = campaign_entity_original.toPandas()
  campaign = campaign_entity_original_df.iloc[0]
  campaign_dict = campaign['vector']
  campaign_id = campaign['id']
  student_profiles_with_programs_df = student_profiles_with_programs.toPandas()
  sample_student = student_profiles_with_programs_df.iloc[0]
  vector = sample_student['vector']
  mutual_processed_labels = [label for label in processed_labels if label in list(vector.keys())]
  count = 0
  processed_campaign_dict = {}
  for label in mutual_processed_labels:
    processed_campaign_dict[label] = campaign_dict[label]
    count += 1
    
  student_vector_dicts = []
  for i in range(len(student_profiles_with_programs_df)):
    student = student_profiles_with_programs_df.iloc[i]
    vector = student['vector']
    vector_values = [vector[skill] for skill in mutual_processed_labels]
    vector_gpa_weighted = student['vector_gpa_weighted']
    vector_values_gpa_weighted = [vector_gpa_weighted[skill] for skill in mutual_processed_labels]
    
    student_vector_dicts.append({'Student_UUID': str(student['Student_UUID']), 'AstrumU_UUID': str(student['AstrumU_UUID']), 'Program_ID': str(student['Program_ID']), 'Campaign_ID': str(campaign['id']),  'vector_old': vector_values, 'vector': vector_values_gpa_weighted, 'sigmoid_dict': {skill: float(vector_gpa_weighted[skill]) for skill in vector_gpa_weighted.keys()}, 'skill_dict': {skill: int(np.around(vector_gpa_weighted[skill])) for skill in vector_gpa_weighted.keys()}, 'matched_course_list': student['matched_course_list'], 'student_skill_list': student['student_skill_list']})

  partitionNum = 100
  student_vector_json = sc.parallelize(student_vector_dicts, partitionNum)
  student_vector_pyspark = sqlContext.read.json(student_vector_json)
  student_vector_pyspark = student_vector_pyspark.where(F.col('vector').isNotNull())

  num_students = student_vector_pyspark.count()
  print(num_students)

  campaign_vector = list(processed_campaign_dict.values())
  campaign_vector = [sigmoid(vector) for vector in campaign_vector]
  initial_time = timeit.default_timer()
  distance_udf = F.udf(lambda x: (1 + float(cosine_similarity(X=[x], Y=[campaign_vector], dense_output=True))) / 2.0)
  student_vector_pyspark = student_vector_pyspark.withColumn('Hard_Skill_Scoring', distance_udf(F.col('vector')))
  student_vector_pyspark.collect()
  time = timeit.default_timer() - initial_time
  print('Took ', time, ' seconds.')
  
  student_vector_pyspark_df = student_vector_pyspark.toPandas()
  campaign_skills = [skill for skill in processed_campaign_dict.keys() if sigmoid(processed_campaign_dict[skill]) >= 0.5]
  
  skill_dict_keys = list(student_vector_pyspark_df['sigmoid_dict'].iloc[0].keys())
  def match_skills(student_skill_dict):
    student_skills = [skill for skill in processed_campaign_dict.keys() if student_skill_dict[skill] >= 0.5]
    return [skill for skill in student_skills if skill in campaign_skills]

  def match_skills_new(student_skill_dict):
    student_skills = [skill for skill in processed_campaign_dict.keys() if student_skill_dict[skill] == 1]
    return [skill for skill in student_skills if skill in campaign_skills]

  def list_skills(student_skill_dict):
    student_skills = [skill for skill in skill_dict_keys if student_skill_dict[skill] == 1]
    return student_skills

  def check_empty_skill_list(Hard_Skill_ranking_list, Hard_Skill_Scoring):
    if len(Hard_Skill_ranking_list) == 0:
      return 0.0
    else:
      return float(Hard_Skill_Scoring)

  matching_udf = F.udf(match_skills, ArrayType(StringType()))
  matching_udf_new = F.udf(match_skills_new, ArrayType(StringType()))
  skill_udf = F.udf(list_skills, ArrayType(StringType()))
  check_empty_skill_list_udf = F.udf(check_empty_skill_list, FloatType())

  student_vector_pyspark_minimal = student_vector_pyspark.withColumn('Hard_Skill_ranking_list', matching_udf_new(F.col('student_skill_list')))
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.withColumn('Student_Skills', skill_udf(F.col('student_skill_list')))
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.withColumn('Hard_Skill_Scoring', check_empty_skill_list_udf(F.col('Hard_Skill_ranking_list'), F.col('Hard_Skill_Scoring'))).sort(F.col("Hard_Skill_Scoring").desc())
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.drop('vector')
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.drop('sigmoid_dict')
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.drop('skill_dict')
  student_vector_pyspark_minimal = student_vector_pyspark_minimal.drop('student_skill_list')
  #intergate the calculate result to pathway fature sapce
  def join_update_columns(df1, df2, cond, col_update, how='left'):
    df_update_temp = df1.select(df1[col_update].alias("col_temp"), "AstrumU_UUID", "Campaign_ID") 
    df3 = df2.join(df_update_temp, cond, 'left')
    df3 = df3.withColumn(col_update, F.coalesce("col_temp", col_update)).drop("col_temp")        
    return df3

  cond_columns = ["AstrumU_UUID", "Campaign_ID"]    
  campaign_feature_space_original = spark.read.format("delta").load(campaign_feature_space_path)

  #add campaign calculation result in feature space 
  campaign_feature_space_scoring = join_update_columns(student_vector_pyspark_minimal, campaign_feature_space_original, cond_columns, 'Hard_Skill_Scoring')
  campaign_feature_space_skills = join_update_columns(student_vector_pyspark_minimal, campaign_feature_space_scoring, cond_columns, 'Student_Skills')
  campaign_feature_space_hardskill_update = join_update_columns(student_vector_pyspark_minimal, campaign_feature_space_skills, cond_columns, 'Hard_Skill_ranking_list')

  campaign_feature_space_hardskill_update.write.format("delta").option('mergeSchema', 'true').option("overwriteSchema", "true").mode("overwrite").save(campaign_feature_space_path)
  

if __name__ == "__main__":
    spark = CreateSparkSession()
    
    app.run(host='0.0.0.0', port=8002)


