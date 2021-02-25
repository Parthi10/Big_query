# import functools
# import json
# from google.cloud.bigquery.opentelemetry_tracing import create_span
# from google.cloud.bigquery.table import Table
# from google.cloud.bigquery.table import TableReference
# from pprint import pprint
# from google.cloud.bigquery import DEFAULT_RETRY, retry
# from googleapiclient import discovery
# from google.cloud import bigquery
# from google.oauth2 import service_account
# from google.api_core.iam import Policy
#
# credentials = service_account.Credentials.from_service_account_file("role/key.json")
#
# project_id = 'onedpo'
# region = "region-us"
# client = bigquery.Client(credentials=credentials, project=project_id)
# service = discovery.build('iam', 'v1', credentials=credentials)

# def api_request(*args, **kwargs):
#     return _call_api(
#         retry,
#         span_name="BigQuery.listTables",
#         span_attributes=span_attributes,
#         *args,
#         timeout=timeout,
#         **kwargs,
#     )

#
# def _call_api(
#         self, retry, span_name=None, span_attributes=None, job_ref=None, **kwargs
# ):
#     call = functools.partial(self._connection.api_request, **kwargs)
#     if retry:
#         call = retry(call)
#     if span_name is not None:
#         with create_span(
#                 name=span_name, attributes=span_attributes, client=self, job_ref=job_ref
#         ):
#             return call()
#     return call()
#
#
# def get_iam_policy('insurance', requested_policy_version=1, retry=DEFAULT_RETRY, timeout=None,):
#     if not isinstance(table, (Table, TableReference)):
#         raise TypeError("table must be a Table or TableReference")
#
#     if requested_policy_version != 1:
#         raise ValueError("only IAM policy version 1 is supported")
#
#     body = {"options": {"requestedPolicyVersion": 1}}
#
#     path = "{}:getIamPolicy".format(table.path)
#     span_attributes = {"path": path}
#     response = _call_api(
#         retry,
#         span_name="BigQuery.getIamPolicy",
#         span_attributes=span_attributes,
#         method="POST",
#         path=path,
#         data=body,
#         timeout=timeout,
#     )
#
#     return Policy.from_api_repr(response)


# def list_tables(dataset_id):
#
#     tables = client.list_tables(dataset_id)  # Make an API request.
#
#     print("Tables contained in '{}':".format(dataset_id))
#     for table in tables:
#         print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
#
#
# list_tables("sample")


import json
from pprint import pprint
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("role/key.json")

project_id = 'onedpo'
client = bigquery.Client(credentials=credentials, project=project_id)


# query_job = client.query("select * from region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT")
#
# print(query_job)


# sql = """
# SELECT
# job_id,
# creation_time,
# user_email,
# query,
# total_bytes_processed
# FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
# WHERE project_id ='onedpo'
# AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY)
# AND CURRENT_TIMESTAMP()
# ORDER BY creation_time DESC
# LIMIT 100
# """

sql = """SELECT creation_time,destination_table,end_time,error_result,job_id,job_type,
labels,project_id,project_number,query,referenced_tables,user_email
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE project_id ='onedpo'
AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY)
AND CURRENT_TIMESTAMP() ORDER BY creation_time DESC LIMIT 100"""

query_job = client.query(sql)  # Make an API request.
df = query_job.to_dataframe()
a = df.to_json(orient='records')
b = json.loads(a)
pprint(b)


# def bq_query_access_log():
#     sql = "SELECT creation_time,destination_table,end_time,error_result,job_id,job_type," \
#           "labels,project_id,project_number,query,referenced_tables,user_email " \
#           "FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE project_id ='onedpo' " \
#           "AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY) " \
#           "AND CURRENT_TIMESTAMP() ORDER BY creation_time DESC LIMIT 100"
#     query_job = client.query(sql)  # Make an API request.
#     df = query_job.to_dataframe()
#     a = df.to_json(orient='records')
#     b = json.loads(a)
#     pprint(b)
#     return b
#
#
# # This function will return Dataset info
# def bq_tables():
#     # sql = "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA" # data-set info
#     # sql = "SELECT * FROM region-us.INFORMATION_SCHEMA.TABLES"  # table list in all data-set
#     sql = "SELECT * FROM sample.INFORMATION_SCHEMA.TABLES"  # this query return all tables info in specific dataset
#     # sql = "roles"
#     query_job = client.get_iam_policy(table="region-us.sample.complaint_rank")  # Make an API request.
#     df = query_job.to_dataframe()
#     a = df.to_json(orient='records')
#     b = json.loads(a)
#     pprint(b)
#     return b


# bq_query_access_log()
bq_tables()




# """
# BEFORE RUNNING:
# ---------------
# 1. If not already done, enable the Identity and Access Management (IAM) API
#    and check the quota for your project at
#    https://console.developers.google.com/apis/api/iam
# 2. This sample uses Application Default Credentials for authentication.
#    If not already done, install the gcloud CLI from
#    https://cloud.google.com/sdk and run
#    `gcloud beta auth application-default login`.
#    For more information, see
#    https://developers.google.com/identity/protocols/application-default-credentials
# 3. Install the Python client library for Google APIs by running
#    `pip install --upgrade google-api-python-client`
# """
# from pprint import pprint
#
# from googleapiclient import discovery
# from oauth2client.client import GoogleCredentials
#
# credentials = GoogleCredentials.get_application_default()
#
# service = discovery.build('iam', 'v1', credentials=credentials)
#
# # The resource name of the role in one of the following formats:
# # `roles/{ROLE_NAME}`
# # `organizations/{ORGANIZATION_ID}/roles/{ROLE_NAME}`
# # `projects/{PROJECT_ID}/roles/{ROLE_NAME}`
# name = 'projects/my-project/roles/my-role'  # TODO: Update placeholder value.
#
# request = service.projects().roles().get(name=name)
# response = request.execute()
#
# # TODO: Change code below to process the `response` dict:
# pprint(response)