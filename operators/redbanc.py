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

class Redbanc:
	def __init__(self,type_process, spark,source,mode_deploy,output):
		self.mode_deploy = mode_deploy
		self.timestamp = strftime("%Y-%m-%d %H:%M:%S", gmtime())
		self.timestamp_bucket = strftime("%Y-%m-%d_%H-%M-%S", gmtime())
		self.output = output
		self.spark = spark
		self.source = source
		self.type_process = type_process


	def convert_redbanc_to_df(self):

		df = self.spark.read.option("header", "false").option("encoding", "ISO-8859-1").csv(self.source)
		df_redbanc = df.withColumn("DATE_PROCESS_FILE",lit(self.timestamp)).withColumn("NAME_FILE_ENTIRE", input_file_name()).withColumn("NAME_SPLIT",split("NAME_FILE_ENTIRE",'/')).withColumn("NAME_FILE",col("NAME_SPLIT").getItem(size("NAME_SPLIT") - 1)).withColumn("ID_ROW", udf(lambda : str(uuid4()), StringType())()).withColumn("TRANS_FI_NBR",substring(df._c0,1,4)).withColumn("TRANS_ABA_NBR",substring(df._c0,5,9)).withColumn("TRANS_BR_NBR",substring(df._c0,14,4)).withColumn("ACCT_NBR",substring(df._c0,18,18)).withColumn("TRML_NBR",substring(df._c0,36,4)).withColumn("TRML_RECEIPT_NBR",substring(df._c0,40,9)).withColumn("TRML_TYPE_CODE",substring(df._c0,49,1)).withColumn("TRML_SUBTYPE_CODE",substring(df._c0,50,1)).withColumn("OPERATING_FI_NBR",substring(df._c0,51,4)).withColumn("OPERATING_ABA_NBR",substring(df._c0,55,9)).withColumn("OPERATING_BR_NBR",substring(df._c0,64,4)).withColumn("OPERATOR_NBR",substring(df._c0,68,4)).withColumn("TRAN_CODE",substring(df._c0,72,4)).withColumn("TRAN_DATE",substring(df._c0,76,9)).withColumn("TRAN_TIME",substring(df._c0,85,9)).withColumn("CARD_FI_NBR",substring(df._c0,94,4)).withColumn("CARD_BR_NBR",substring(df._c0,98,4)).withColumn("CARD_ACCT_NBR",substring(df._c0,102,18)).withColumn("CARD_APPL_CODE",substring(df._c0,120,2)).withColumn("AUTH_METHOD",substring(df._c0,122,1)).withColumn("RESULT_CODE",substring(df._c0,123,4)).withColumn("CTA_VTA_IND",substring(df._c0,127,2)).withColumn("AMT_DEBIT",substring(df._c0,129,13)).withColumn("AMT_MDA_ORIG",substring(df._c0,142,12)).withColumn("AMT_MDA_COMP",substring(df._c0,154,6)).withColumn("TASA_CAMBIO",substring(df._c0,160,6)).withColumn("FECHA_TASA_CAMBIO",substring(df._c0,166,4)).withColumn("COD_MDA_ORIG",substring(df._c0,170,3)).withColumn("ACCEPTOR_NAME",substring(df._c0,173,22)).withColumn("OTROS_DATOS_VARIOS",substring(df._c0,195,83)).drop("NAME_FILE_ENTIRE").drop("NAME_SPLIT").drop("_c0")
		self.df_redbanc_final = df_redbanc.withColumn("CARD_ACCT_NBR", translate(df_redbanc.CARD_ACCT_NBR,"ABCDEFGHI{","1234567890")).withColumn("CARD_ACCT_NBR",substring("CARD_ACCT_NBR",3,30)).withColumn("CARD_ACCT_NBR", when(trim("CARD_ACCT_NBR") != "",concat(substring("CARD_ACCT_NBR",0,6),lit("*"*6),substring("CARD_ACCT_NBR",13,4))).otherwise("")).withColumn("AMT_DEBIT",translate(df_redbanc.AMT_DEBIT,"{","0")).withColumn("AMT_DEBIT",regexp_replace("AMT_DEBIT",r'^[0]*',"")).selectExpr('DATE_PROCESS_FILE', 'NAME_FILE', 'ID_ROW', 'TRANS_FI_NBR','TRANS_ABA_NBR','TRANS_BR_NBR','ACCT_NBR','TRML_NBR','TRML_RECEIPT_NBR','TRML_TYPE_CODE','TRML_SUBTYPE_CODE','OPERATING_FI_NBR','OPERATING_ABA_NBR','OPERATING_BR_NBR','OPERATOR_NBR','TRAN_CODE','TRAN_DATE','TRAN_TIME','CARD_FI_NBR','CARD_BR_NBR','CARD_ACCT_NBR','CARD_APPL_CODE','AUTH_METHOD','RESULT_CODE','CTA_VTA_IND','AMT_DEBIT','AMT_MDA_ORIG','AMT_MDA_COMP','TASA_CAMBIO','FECHA_TASA_CAMBIO','COD_MDA_ORIG','ACCEPTOR_NAME','OTROS_DATOS_VARIOS')

		logging.info("success to convert_redbanc_to_df")

	def write_files_csv_gcs(self):

		list_files = self.df_redbanc_final.select('NAME_FILE').distinct().rdd.flatMap(lambda x: x).collect()

		for name_file in list_files :

			try :
				self.df_redbanc_final.filter(self.df_redbanc_final.NAME_FILE == name_file).coalesce(1).write.options(header='True', delimiter=',').csv(self.output+"OPD/ANULATION/"+name_file+"/"+self.timestamp_bucket+"/")
				logging.info(f"success when copy redbanc-csv {name_file} to bucket \n")

			except Exception as e:
				logging.error(f"failed when copy redbanc-csv  {name_file} to bucket "+str(e)+"\n")

	def write_df_to_bigquery(self):

		try:
			self.df_redbanc_final.write.format("bigquery").option("table", self.output).mode("overwrite").save()
			logging.info("success when write to bigquery \n")

		except Exception as e:
			logging.error("failed when write in bigquery :"+str(e)+"\n")

	def run(self):

		self.convert_redbanc_to_df()

		if (self.mode_deploy == "test") :
			self.write_files_csv_gcs()
		elif (self.mode_deploy == "prod") :
			self.write_df_to_bigquery()
		else:
			logging.info(f"not exist this type mode_deploy: {self.mode_deploy}")

