from pyspark.sql import SparkSession
import logging
from time import gmtime, strftime
from  pyspark.sql.functions import input_file_name
from google.cloud import bigquery
from google.cloud import storage
import os
import sys
from pyspark.sql.functions import *
from uuid import uuid4

logging.basicConfig(level=logging.INFO)

class Ipm:
	def __init__(self,type_process, spark,list_file,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.list_file = list_file
		self.type_process = type_process

	def convert_ipm_to_csv(self):

		for name_file in self.list_file :
			try:
				name_csv = name_file.replace("_encrypted","")
				convert_csv = "mci_ipm_to_csv --out-encoding UTF-16 -o ./ipm_csv/{0}.csv ./decrypted/{1}".format(name_csv,name_file)
				os.system(convert_csv)
				logging.info(f"success when convert {name_file} to csv \n")
			except Exception as e:
				logging.error(f"failed when convert {name_file} to csv"+str(e)+"\n")

	def write_files_gcs(self):

		move_files = "gsutil -m cp ./ipm_csv/* {0}IPM/{1}/".format(self.output,self.timestamp_bucket)
		os.system(move_files)

	def write_files_bigquery(self):
		os.system("hdfs dfs -put ./ipm_csv/ /")
		df = self.spark.read.options(header='True',delimiter=',',encoding='UTF-16').csv("hdfs:///ipm_csv/")
		df.printSchema()
		df.show(10,truncate=False)
		os.system("/usr/share/google/get_metadata_value attributes/dataproc-cluster-name>cluster_name.txt")
		cluster_name = open("./cluster_name.txt","r").read()
		df1 = df.select([regexp_replace(col(c), "�", "").alias(c) for c in df.columns]).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_FILE",regexp_replace("NAME_FILE_ENTIRE","hdfs://"+cluster_name+"-m/ipm_csv/","")).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn('FECPROCES',to_timestamp( regexp_replace(concat(lit("20"),substring("NAME_FILE",25,14)),".T"," " ) ,"yyyyMMdd HHmmss" ))
		df2 = df1.selectExpr('DATE_PROCESS_FILE', 'NAME_FILE', 'ID_ROW', 'MTI', 'DE2', 'DE3', 'DE4', 'DE5', 'DE6', 'DE9', 'DE10', 'DE12', 'DE14', 'DE22', 'DE23', 'DE24', 'DE25', 'DE26', 'DE30', 'DE31', 'DE32', 'DE33', 'DE37', 'DE38', 'DE40', 'DE41', 'DE42', 'DE43_NAME', 'DE43_SUBURB', 'DE43_POSTCODE', 'DE48', 'DE49', 'DE50', 'DE51', 'DE54', 'DE62', 'DE63', 'DE71', 'DE72', 'DE73', 'DE93', 'DE94', 'DE95', 'DE100', 'DE123', 'DE124', 'DE125', 'DE127', 'PDS0002', 'PDS0003', 'PDS0014', 'PDS0015', 'PDS0023', 'PDS0025', 'PDS0044', 'PDS0052', 'PDS0080', 'PDS0105', 'PDS0122', 'PDS0137', 'PDS0146', 'PDS0148', 'PDS0158', 'PDS0159', 'PDS0164', 'PDS0165', 'PDS0170', 'PDS0173', 'PDS0176', 'PDS0177', 'PDS0188', 'PDS0191', 'PDS0198', 'PDS0300', 'PDS0301', 'PDS0306', 'PDS0372', 'PDS0374', 'PDS0378', 'PDS0380', 'PDS0381', 'PDS0384', 'PDS0390', 'PDS0391', 'PDS0392', 'PDS0393', 'PDS0394', 'PDS0395', 'PDS0396', 'PDS0400', 'PDS0401', 'PDS0402', 'PDS0502', 'PDS1000', 'PDS1002', 'ICC_DATA','FECPROCES')
		
		try:
			df2.write.format("bigquery") \
             .option("table", self.output) \
             .mode("overwrite") \
             .save()

		except Exception as e:
			logging.error(f"faile when write in bigquery :"+str(e)+"\n")

	def run(self):
		
		self.convert_ipm_to_csv()

		if (self.mode_deploy == "test") :
			self.write_files_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_files_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")

