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
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

logging.basicConfig(level=logging.INFO)

class Incident:

	def __init__(self,type_process, spark,list_file,mode_deploy,output):

		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.list_file = list_file
		self.type_process = type_process

	def convert_incident_to_csv(self):

		for name_file in self.list_file :
			
			get_total_lines="cat ./decrypted/{0}|wc -l".format(name_file)
			get_begin_line="grep -n 'SQL>' ./decrypted/{0}|head -1|cut -d ':' -f 1".format(name_file)
			total_lines=os.popen(get_total_lines).read().replace('\n','')
			begin_line=os.popen(get_begin_line).read().replace('\n','')
			first_line=int(begin_line)+3
			last_line=int(total_lines)-4
			generate_csv="cat ./decrypted/{0} | sed -n '{1},{2} p' > ./opd_incident_csv/{0}.csv".format(name_file,first_line,last_line)
			os.system(generate_csv)
			logging.info(f"success when convert {name_file} to csv \n")

	def write_files_csv_gcs(self):

		move_files = "gsutil -m cp ./opd_incident_csv/* {0}OPD/INCIDENT/{1}/".format(self.output,self.timestamp_bucket)
		os.system(move_files)

	def write_df_to_bigquery(self):

		os.system("hdfs dfs -put ./opd_incident_csv/ /")

		schema = StructType([\
					StructField("NUMDOC", StringType(), True),\
					StructField("CODE", StringType(), True),\
					StructField("NUMINC", StringType(), True),\
					StructField("NUMEXP", StringType(), True),\
					StructField("INDNORCOR", StringType(), True),\
					StructField("TIPOFAC", StringType(), True),\
					StructField("LAST_PAN", StringType(), True),\
					StructField("TIPFRAN", StringType(), True),\
					StructField("SECOPE", StringType(), True),\
					StructField("NUMREF", StringType(), True),\
					StructField("FECFAC", StringType(), True),\
					StructField("NUMAUT", StringType(), True),\
					StructField("NOMCOMRED", StringType(), True),\
					StructField("CODCOM", StringType(), True),\
					StructField("CODACT", StringType(), True),\
					StructField("INDDEBCRE", StringType(), True),\
					StructField("CLAMONDIV", StringType(), True),\
					StructField("CMBAPLI", StringType(), True),\
					StructField("IMPDIV", StringType(), True),\
					StructField("IMPLIQ", StringType(), True),\
					StructField("CLAMONLIQ", StringType(), True),\
					StructField("FECALTAINC", StringType(), True),\
					StructField("TIPOINC", StringType(), True),\
					StructField("INDERROR", StringType(), True),\
					StructField("FECSOLINC", StringType(), True),\
					StructField("CLAMON", StringType(), True),\
					StructField("IMPFAC", StringType(), True)])

		df = self.spark.read.option("sep", ";").option("header", "false").option("encoding", "ISO-8859-1").schema(schema).csv("hdfs:///opd_incident_csv/")
		df1=df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("PAN",when(trim(df.LAST_PAN) != "",concat(substring(df.LAST_PAN,0,7),lit("*"*6),substring(df.LAST_PAN,14,4))).otherwise(df.LAST_PAN)).selectExpr('DATE_PROCESS_FILE','NAME_FILE', 'ID_ROW','NUMDOC','CODE','NUMINC','NUMEXP','INDNORCOR','TIPOFAC','PAN','TIPFRAN','SECOPE','NUMREF','FECFAC','NUMAUT','NOMCOMRED','CODCOM','CODACT','INDDEBCRE','CLAMONDIV','CMBAPLI','IMPDIV','IMPLIQ','CLAMONLIQ','FECALTAINC','TIPOINC','INDERROR','FECSOLINC','CLAMON','IMPFAC')
		df1.write.format("bigquery").option("table", self.output).mode("overwrite").save()

		logging.info("success write data in bigquery")

	def run(self):

		self.convert_incident_to_csv()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")
