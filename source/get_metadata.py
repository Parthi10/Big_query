import json
from pprint import pprint
from googleapiclient import discovery
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file("role/key.json")

project_id = 'onedpo'
region = "region-us"
client = bigquery.Client(credentials=credentials, project=project_id)
service = discovery.build('iam', 'v1', credentials=credentials)


def bq_query_access_log():
    query = "SELECT creation_time,destination_table,end_time,error_result,job_id,job_type," \
            "labels,project_id,project_number,query,referenced_tables,user_email " \
            "FROM `{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT WHERE project_id ='{id}' " \
            "AND creation_time BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 183 DAY) " \
            "AND CURRENT_TIMESTAMP() ORDER BY creation_time DESC LIMIT 100".format(region=region, id=project_id)
    query_job = client.query(query)  # Make an API request.

    df = query_job.to_dataframe()
    string = df.to_json(orient='records')
    query_list = json.loads(string)
    # rows = query_job.result()
    pprint(query_list)
    return query_list


def bq_tables():
    sql = "SELECT * FROM {region}.INFORMATION_SCHEMA.TABLES".format(region=region)  # table list in all data-set
    query_job = client.query(sql)  # Make an API request.
    df = query_job.to_dataframe()
    a = df.to_json(orient='records')
    b = json.loads(a)
    pprint(b)
    return b


def bq_schema():
    sql = "SELECT * FROM region-us.INFORMATION_SCHEMA.SCHEMATA"
    query_job = client.query(sql)  # Make an API request.
    df = query_job.to_dataframe()
    a = df.to_json(orient='records')
    b = json.loads(a)
    pprint(b)
    return b


def bq_roles_permissions():
    request = service.roles().list()
    while True:
        response = request.execute()

        for role in response.get('roles', []):
            # TODO: Change code below to process each `role` resource:
            pprint(role)

        request = service.roles().list_next(previous_request=request, previous_response=response)
        if request is None:
            break


def bq_service_account():
    name = 'projects/onedpo'  # TODO: Update placeholder value.
    request = service.projects().serviceAccounts().list(name=name)
    while True:
        response = request.execute()

        for service_account in response.get('accounts', []):
            # TODO: Change code below to process each `service_account` resource:
            pprint(service_account)

        request = service.projects().serviceAccounts().list_next(previous_request=request, previous_response=response)
        if request is None:
            break


def bq_roles_in_project():
    resource = 'projects/onedpo/serviceAccounts/developer@onedpo.iam.gserviceaccount.com'  # TODO: Update placeholder value.

    request = service.projects().serviceAccounts().getIamPolicy(resource=resource)
    response = request.execute()

    # TODO: Change code below to process the `response` dict:
    pprint(response)


def bq_roles_in_datasets():
    # services = discovery.build('iam', 'v2', credentials=credentials)
    resource = 'projects/onedpo/datasets/sample/tables/*'

    request = service.projects().datasets().tables().getIamPolicy(resource=resource)
    response = request.execute()

    # TODO: Change code below to process the `response` dict:
    pprint(response)


def query_testable_permissions(resource):
    """Lists valid permissions for a resource."""

    # pylint: disable=no-member
    permissions = service.permissions().queryTestablePermissions(body={
        'fullResourceName': resource,
    }).execute()['permissions']
    for p in permissions:
        pprint(p)


def test():
    resource = 'projects/onedpo/serviceAccounts/developer@onedpo.iam.gserviceaccount.com'

    request = service.projects().serviceAccounts().getIamPolicy(resource=resource)
    response = request.execute()

    # TODO: Change code below to process the `response` dict:
    pprint(response)


def sas(resource):
    """Lists valid permissions for a resource."""

    permissions = service.projects().datasets(body={
        'fullResourceName': resource,
    }).execute()['permissions']
    for p in permissions:
        pprint(p)


bq_query_access_log()
# bq_tables()
# bq_schema()
# bq_roles_permissions()
# bq_service_account()
# bq_roles_in_project()
# bq_roles_in_datasets()
# test()
# query_testable_permissions('//bigquery.googleapis.com/projects/onedpo/datasets/sample')
# sas('//bigquery.googleapis.com/projects/onedpo/datasets/sample')
