from pyspark.sql import SparkSession
import pgpy
import cardutil
import os
import logging
from pyspark.sql.types import *
from pyspark.sql.functions import *
from google.cloud import bigquery
from google.cloud import storage
from uuid import uuid4
from time import gmtime, strftime
import pandas as pd
logging.basicConfig(level=logging.INFO)

def get_secret(type_file):
  secret_key_file = "gcloud secrets versions access latest --secret={0}_file_key > {0}_file.key".format(type_file)
  secret_password = "gcloud secrets versions access latest --secret={0}_password_file > {0}_password_file.key".format(type_file)
  try:
    os.system(secret_key_file)
    logging.info('success download secret_key_file')
  except Exception as e:
    logging.error(f"failed when get secret key file {type_file}"+e)
    #print(f"failed when get secret key file {type_file}"+e)

  try:
    os.system(secret_password)
    logging.info('success download secret_password')
  except Exception as e:
    logging.error(f"failed when get secret password {type_file}"+e)
    #print(f"failed when get secret password {type_file}"+e)
    

def decrypt(type_file,emsg,namefile):

  private_key_file = "{0}_file.key".format(type_file)
  password_file = "{0}_password_file.key".format(type_file)
  try:
    password = open(password_file).read()
  except Exception as e:
    logging.error(f"failed get password of {type_file} \n"+str(e))
    #print(f"failed get password of {type_file} \n"+e)

  try:
    private_key, _ =pgpy.PGPKey.from_file(private_key_file)
    with private_key.unlock(password) as ukey:
      return (ukey.decrypt(emsg).message)
    logging.info("success decrypt "+namefile)
  except Exception as e:
    logging.error("failed when decrypt "+namefile+" \n"+str(e))
    #print(f"faile when decrypt {emsg} \n"+e)

def rejected(TYPE_PROCESS,NAME_FILES,REJECTED_MESSAGES,REJECTED_VALUE):

  fec_process = str(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  uniq_id = str(uuid4())
  reject_values = REJECTED_VALUE[:500].replace("<","").replace(">","").replace("[","").replace("]","")

  data = [[fec_process,TYPE_PROCESS,NAME_FILES,uniq_id,REJECTED_MESSAGES,reject_values]]
  df = pd.DataFrame(data, columns = ['DATE_PROCESS_FILE','TYPE_PROCESS','NAME_FILE','ID_ROW','REJECTED_MESSAGE','REJECTED_VALUE']) 
  df.to_csv("./rejected/"+NAME_FILES+".csv",index=False,sep='|')

def upload_rejected(spark,output):

  output_bq = output.split(".")[0]+"."+output.split(".")[1]
  timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
  os.system("hdfs dfs -put ./rejected/ /")
  try:
    df_rejected = spark.read.option("sep", "|").option("header", "true").option("encoding", "ISO-8859-1").csv("hdfs:///rejected/")
    #df_rejected.show(1000,truncate=False)
    #df_rejected.printSchema()
    df_rejected.write.format("bigquery").option("table", output_bq+".rejected").mode("append").save()
  except Exception as e:
    logging.error("failed when upload data rejected "+" \n"+str(e))
    df_rejected.coalesce(1).write.options(header='True', delimiter=';').csv("gs://mark-vii-conciliacion/rejected/"+timestamp_bucket+"/")
    


  
