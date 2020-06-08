# initialise modules..
import time
import cx_Oracle
import oci
import os
import gzip
import shutil
import io
import json
import csv
import zipfile
import pandas as pd
from fdk import response
import logging
logging.basicConfig(level=logging.INFO)

# use oracle resource principal provider to extract credentials from rpst token..
def handler(ctx, data: io.BytesIO=None):
   signer = oci.auth.signers.get_resource_principals_signer()
   resp = do(signer)
   return response.Response(ctx,
      response_data=json.dumps(resp),
      headers={"Content-Type": "application/json"})

def do(signer):
# return data..
   file_list = None
   file_list = []

# establish function start-time..
   runsec = None
   date1 = time.time()

   # usage report dependencies..
   usage_report_namespace = 'bling'
   report_path = '/tmp/downloaded_reports'
   wallet_path = '/tmp/wallet'
   usage_report_bucket = os.environ['usage_report_bucket']

   # autonomous database dependencies..
   autonomous_database_id = os.environ['db_ocid']
   generate_autonomous_database_wallet_details = oci.database.models.GenerateAutonomousDatabaseWalletDetails(password=os.environ['db_pass'])

   # create local directories..
   if not os.path.exists(report_path):
      os.mkdir(report_path)
   if not os.path.exists(wallet_path):
      os.mkdir(wallet_path)

   # initialise clients..
   object_storage = oci.object_storage.ObjectStorageClient({}, signer=signer)
   autonomous_db = oci.database.DatabaseClient({}, signer=signer)

   # download db client credential package..
   with open(wallet_path + '/' + 'wallet.zip', 'wb') as f:
      wallet_details = autonomous_db.generate_autonomous_database_wallet(autonomous_database_id, generate_autonomous_database_wallet_details)
      for chunk in wallet_details.data.raw.stream(1024 * 1024, decode_content=False):
         f.write(chunk)
         logging.info('finished downloading ' + wallet_path + '/' + 'wallet.zip')

      # extract..
      with zipfile.ZipFile(wallet_path + '/' + 'wallet.zip', 'r') as zip_obj:
         zip_obj.extractall(wallet_path)

      # update sqlnet.ora..
      with open(wallet_path + '/sqlnet.ora', 'r') as sqlnet_file:
         sqlnet_filedata = sqlnet_file.read()
      sqlnet_filedata = sqlnet_filedata.replace('?/network/admin', '/tmp/wallet')
      with open(wallet_path + '/sqlnet.ora', 'w') as sqlnet_file:
         sqlnet_file.write(sqlnet_filedata)

   # iterate over reports in usage reports bucket - process where applicable..
   report_bucket_objects = object_storage.list_objects(usage_report_namespace, usage_report_bucket)
   for o in report_bucket_objects.data.objects:
      gz_filename = o.name.rsplit('/', 1)[-1]
      csv_filename = gz_filename[:-3]
      filename = csv_filename[:-4]

      # adw connection..
      con = cx_Oracle.connect(user=os.environ['db_user'], password=os.environ['db_pass'], dsn=os.environ['db_dsn'])
      cur = con.cursor()

      # check if current file has been previously uploaded to database..
      sql = "SELECT COUNT(usage_report) FROM oci_billing WHERE usage_report = :report_id"
      cur.execute(sql, {"report_id":os.environ['usage_report_bucket'] + "-" + filename})
      val, = cur.fetchone()
      bucket = os.environ['usage_report_bucket']
      logging.info(f'report_id: {bucket}-{filename}: {val}')

      # calculate function run time..
      date2 = time.time()
      runsec = (date2 - date1)
      logging.info(f'runtime: {runsec} seconds')

      if (runsec >= 115):
         # break if too close to function timeout..
         break
      elif (val == 0):
         # no record of this usage report found in db..
         #   - let's process this file..
         #   - copy report to local file system..
         with open(report_path + '/' + gz_filename, 'wb') as f:
            object_details = object_storage.get_object(usage_report_namespace,usage_report_bucket,o.name)
            for chunk in object_details.data.raw.stream(1024 * 1024, decode_content=False):
               f.write(chunk)
               logging.info('finished downloading ' + gz_filename)

         # unzip usage report file..
         with gzip.open(report_path + '/' + gz_filename, 'r') as f_in, open(report_path + '/' + csv_filename, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

         # format usage report..
         #   - use pandas df to format csv data..
         #   - rename headers to remove '/'..
         #   - include 'lineItem_backreferenceNo' col if not present..
         df = pd.read_csv(report_path + '/' + csv_filename,
         index_col=False,
         parse_dates=[0])

         if ("lineItem/backreferenceNo" not in df.columns[df.columns.str.contains(pat = 'lineItem/backreferenceNo')]):
            csv_brn = False
            pd_cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]
            pd_names = ["lineItem_referenceNo", "lineItem_tenantId", "lineItem_intervalUsageStart", "lineItem_intervalUsageEnd", "product_service", "product_resource", "product_compartmentId", "product_compartmentName", "product_region", "product_availabilityDomain", "product_resourceId", "usage_consumedQuantity", "usage_billedQuantity", "usage_consumedQuantityUnits", "usage_consumedQuantityMeasure", "lineItem_isCorrection"]
         else:
            csv_brn = True
            pd_cols = [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
            pd_names = ["lineItem_referenceNo", "lineItem_tenantId", "lineItem_intervalUsageStart", "lineItem_intervalUsageEnd", "product_service", "product_resource", "product_compartmentId", "product_compartmentName", "product_region", "product_availabilityDomain", "product_resourceId", "usage_consumedQuantity", "usage_billedQuantity", "usage_consumedQuantityUnits", "usage_consumedQuantityMeasure", "lineItem_isCorrection", "lineItem_backreferenceNo"]

         df = pd.read_csv(report_path + '/' + csv_filename,
         index_col=False,
         usecols=pd_cols,
         parse_dates=[0],
         header=0,
         names=pd_names)

         # insert additional column(s) into df & write out csv file..
         if csv_brn == False:
            df.insert(16, "lineItem_backreferenceNo", '')
            logging.info('inserting col lineItem_backreferenceNo into csv..')
         df.insert(0, "usage_report", os.environ['usage_report_bucket'] + "-" + filename)
         logging.info('inserting col usage_report into csv..')
         export_csv = df.to_csv(report_path + '/' + 'trim_' + csv_filename, index = None, header=True)

         # insert usage data into adw..
         with open(report_path + '/' + 'trim_' + csv_filename, "r") as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader)
            pylist = list(csv_reader)
            cur.executemany("INSERT INTO oci_billing (usage_report, lineItem_referenceNo, lineItem_tenantId, lineItem_intervalUsageStart, lineItem_intervalUsageEnd, product_service, product_resource, product_compartmentId, product_compartmentName, product_region, product_availabilityDomain, product_resourceId, usage_consumedQuantity, usage_billedQuantity, usage_consumedQuantityUnits, usage_consumedQuantityMeasure, lineItem_isCorrection, lineItem_backreferenceNo) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18)", pylist)
            logging.info('finished uploading ' + gz_filename)
         cur.close()
         con.commit()
         con.close()

         # curate return data..
         file_list.append(csv_filename)
         # clean-up working dir..
         os.system('rm -rf %s/*' % report_path)

   # data for function response..
   return file_list
