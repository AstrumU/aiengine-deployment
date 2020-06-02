# Databricks notebook source
# MAGIC %md # connect to 'databricks-bronze' container with read/write access right (only available to 'bronze' group)

# COMMAND ----------

from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)

# <auth-type>
auth_type_key = "fs.azure.account.auth.type"
auth_type_value = "OAuth"

# <provider-type>
provider_type_key = "fs.azure.account.oauth.provider.type"
provider_type_value = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"

# <secret-scope>
secret_scope_name = "storage_account_readwrite_access"
print("secret_scope_name: " + secret_scope_name)

# <client-id>
client_id_key = "fs.azure.account.oauth2.client.id"
client_id_value = dbutils.secrets.get(scope = secret_scope_name, key = "appId")

# <clinet-secret>
client_secret_key = "fs.azure.account.oauth2.client.secret"
client_secret_value = dbutils.secrets.get(scope = secret_scope_name, key = "password")

# <client-endpoint>
client_endpoint_key = "fs.azure.account.oauth2.client.endpoint"
client_endpoint_value = "https://login.microsoftonline.com/9b62c0fc-ad42-4789-b7e9-0d9e03e1e11f/oauth2/token"

# <configs>
configs = {
  auth_type_key:auth_type_value,
  provider_type_key:provider_type_value,
  client_id_key:client_id_value,
  client_secret_key:client_secret_value,
  client_endpoint_key:client_endpoint_value}
print(configs)

# <file_system>
file_system = "databricks-adfs-bronze"
print("file_system: " + file_system)

# <storage-account-name>
storage_account_name = "dsdatalakev3"
print("storage_account_name: " + storage_account_name)

# <file_system>@<storage-account-name>
file_system_storage_account_name = file_system + "@" + storage_account_name
print("file_system_storage_account_name: " + file_system_storage_account_name)

# file-path
file_path = "abfss://" + file_system_storage_account_name + ".dfs.core.windows.net"
print("file_path: " + file_path)

# <mount_point>
mount_point = "/mnt/databricks-bronze"
print("mount_point" + mount_point)

# COMMAND ----------

if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
  dbutils.fs.unmount(mount_point)

dbutils.fs.mount(
  source = file_path,
  mount_point = mount_point,
  extra_configs = configs)
print(mount_point+" is mounted")

# COMMAND ----------

# read
print("list of content in " + file_system)
content_list = dbutils.fs.ls(mount_point)
for content in content_list:
  print(content)

# write
new_folder = mount_point + '/test1'
print("create a new folder: " + new_folder)
dbutils.fs.mkdirs(new_folder)
content_list = dbutils.fs.ls(mount_point)
for content in content_list:
  print(content)

print("delete the new folder: " + new_folder)
dbutils.fs.rm(new_folder)
content_list = dbutils.fs.ls(mount_point)
for content in content_list:
  print(content)

# COMMAND ----------


