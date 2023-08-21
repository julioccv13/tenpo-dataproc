from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import pgpy
from datetime import datetime
import sys
from google.cloud import bigquery
from google.cloud import storage
import findspark
import logging


start_process_time = datetime.now().microsecond

findspark.init()
logging.basicConfig(level=logging.INFO)

#Get parameters
files_operators = sys.argv[1]
input_path = sys.argv[2]
logging.info("input_path:"+input_path)
type_file = sys.argv[3]
output = sys.argv[4]
mode_deploy = sys.argv[5]

#get files operators
get_files_operators = "gsutil -m cp {0} .".format(files_operators)
os.system(get_files_operators)
logging.info("copy files operators")

from utils import get_secret,decrypt,rejected,upload_rejected
from ipm import Ipm
from ipm_historic import Ipm_historic
from opd import Opd
from opd_historic import Opd_historic
from anulation import Anulation
from incident import Incident
from cca import Cca
from pdc import Pdc
from recargas_app import Recargas

#create spark session
client = storage.Client()
spark = SparkSession.builder\
        .appName("Spark-Tenpo")\
        .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2') \
        .config('temporaryGcsBucket', 'mark-vii-conciliacion/temporal_bq/') \
        .config("spark.sql.debug.maxToStringFields", 1000) \
        .getOrCreate()

#name_file = "MCI.AR.T112.M.E0073610.D221030.T213611.A002_encrypted"

#Custominaze decrypt program
os.system("rm /opt/conda/miniconda3/lib/python3.8/site-packages/cardutil/config.py;rm /opt/conda/miniconda3/lib/python3.8/site-packages/cardutil/iso8583.py")
os.system("gcloud secrets versions access latest --secret=cardutil_config > /opt/conda/miniconda3/lib/python3.8/site-packages/cardutil/config.py;gcloud secrets versions access latest --secret=cardutil_iso8583 > /opt/conda/miniconda3/lib/python3.8/site-packages/cardutil/iso8583.py")

list_file_encrypted = []
list_file_decrypted = []

if (type_file == "ipm" or type_file == "opd" or type_file=="incident" or type_file=="ipm_historic" or type_file=="opd_historic"):

    if (type_file == "incident") :
        file_process = "opd"
    elif (type_file == "ipm_historic") :
        file_process = "ipm"
    elif (type_file == "opd_historic") :
        file_process = "opd"
    else :
        file_process = type_file
    
    #Download secrets keys
    get_secret(file_process)

    os.system("mkdir encrypted;mkdir decrypted;mkdir ipm_csv;mkdir opd_incident_csv;mkdir rejected")
    get_files = "gsutil -m cp {0} ./encrypted/".format(input_path)
    logging.info("get_files:"+get_files)
    #print("get_files:"+get_files)
    os.system(get_files)
    logging.info('copy file encrypted OK \n')
    #print('copy file encrypted OK \n')

    #list_file = []
    for file in [doc for doc in os.listdir("./encrypted/")]:
        list_file_encrypted.append(file)

    for name_file in list_file_encrypted :
        try:
            emsg=pgpy.PGPMessage.from_file("./encrypted/"+name_file)

            try:
                decrypted=decrypt(file_process,emsg,name_file)
                
                file_decrypted = open("./decrypted/"+name_file, "wb")
                file_decrypted.write(decrypted)
                file_decrypted.close()
                logging.info("success decrypted "+type_file+" "+name_file)

                list_file_decrypted.append(name_file)

            except Exception as e:
                logging.error('failed with key to decrypted file'+name_file+ " "+str(e))
                rejected(type_file,name_file,"failed when try decrypt file with key",str(e))

        except Exception as e:
            logging.error('failed to get binary of file:'+name_file+ " "+str(e))
            rejected(type_file,name_file,"failed when try to get binary content of file",str(e))
            
        #test="cat ./decrypted/"+name_file+"|head -15"
        #os.system(test)
        #os.system("gsutil -m cp ./decrypted/* gs://tenpo-mark-vii/test/incident_decrypt/")
        #os.system("ls -la /opt/conda/miniconda3/lib/python3.8/site-packages/cardutil/")
        #os.system("cd ../.. ;find / -type d -name \"cardutil\"") 
try:
    upload_rejected(spark,output)
except Exception as e:
    print("failed when try to upload data rejected or not exists data rejected")

print('list_file_decrypted:'+str(list_file_decrypted))
#write file csv in storage or bigquery
if (type_file == "ipm"):
    process_ipm = Ipm(type_file,spark,list_file_decrypted,mode_deploy,output)
    process_ipm.run()

if (type_file == "ipm_historic"):
    process_ipm_historic = Ipm_historic(type_file,spark,list_file_decrypted,mode_deploy,output)
    process_ipm_historic.run()

if (type_file == "opd"):
    process_opd = Opd(type_file,spark,list_file_decrypted,mode_deploy,output)
    process_opd.run()

if (type_file == "opd_historic"):
    process_opd_historic = Opd_historic(type_file,spark,list_file_decrypted,mode_deploy,output)
    process_opd_historic.run()

if (type_file == "anulation"):
    process_anulation = Anulation(type_file,spark,input_path,mode_deploy,output)
    process_anulation.run()

if (type_file == "incident"):
    process_incident = Incident(type_file,spark,list_file_decrypted,mode_deploy,output)
    process_incident.run()

if (type_file == "cca"):
    process_cca = Cca(type_file,spark,input_path,mode_deploy,output)
    process_cca.run()

if (type_file == "pdc"):
    process_pdc = Pdc(type_file,spark,input_path,mode_deploy,output)
    process_pdc.run()

if (type_file == "recargas"):
    process_recargas = Recargas(type_file,spark,input_path,mode_deploy,output)
    process_recargas.run()

end_process_time = datetime.now().microsecond
time_elapsed = end_process_time - start_process_time
if time_elapsed < 0 :
    time_elapsed=time_elapsed*-1

logging.info("Time process elapsed in "+str(time_elapsed) +" miliseconds" )
