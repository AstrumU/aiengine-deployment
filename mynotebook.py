# Databricks notebook source
import argparse
#import config

parser = argparse.ArgumentParser(description='Process feture space.')
parser.add_argument('input', type=int, help='an compaing id from company input')
args = parser.parse_args()
campaign_ID = args.input

print(campaign_ID)
#definaiton prameter
#dbutils.widgets.text("Campaign_ID", "10", "CampaignEmptyID")
#print(dbutils.widgets.get("Campaign_ID"))
#Campaign_ID_inpurt = dbutils.widgets.get("Campaign_ID")
#newvalue = Campaign_ID_inpurt + '10'



# COMMAND ----------

#{"notebook_params":{"name":"john doe","age":"35"}}

# COMMAND ----------

#dbutils.notebook.exit("hello world")

# COMMAND ----------
