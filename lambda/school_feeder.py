import json
import boto3
import time


def lambda_handler(event, context):
    DATABASE_NAME = 'school-ratings-gold-bucket'
    output_dir = 's3://datateer-school-rating/athena-apigateway-query/'
    query_resource = event['resource']
    query_string = ''
    results = []
    if query_resource == '/schools/findByRange':
        lat = event['queryStringParameters']['lat']
        lng = event['queryStringParameters']['lng']
        radius = event['queryStringParameters']['radius']
        if radius.isdigit():
            radius = int(radius)
            radius = radius if radius > 0 else 10
        else:
            radius = 10
        query_string = 'select school_id,school_name,school_type,name_suburb_postcode,address,longitude,latitude from school where st_distance(to_spherical_geography(st_point('+lng + ','+lat+')),to_spherical_geography(st_point(longitude,latitude))) <= '+str(
            radius*1000)+';'
        results = handle_query(DATABASE_NAME, output_dir, query_string)
    elif query_resource == '/schools/searchByRadius':
        key = event['queryStringParameters']['key'].lower()
        lat = event['queryStringParameters']['lat']
        lng = event['queryStringParameters']['lng']
        radius = event['queryStringParameters']['radius']
        if radius.isdigit():
            radius = int(radius)
            radius = radius if radius > 0 else 10
        else:
            radius = 10
        query_string = 'select school_id,school_name,school_type,name_suburb_postcode,address,longitude,latitude from school where lower(name_suburb_postcode) like \'%' + \
            key+'%\' and st_distance(to_spherical_geography(st_point('+lng+','+lat + \
            ')),to_spherical_geography(st_point(longitude,latitude))) <=' + \
            str(radius*1000)+';'
        results = handle_query(DATABASE_NAME, output_dir, query_string)
    elif query_resource == '/schools/search':
        key = event['queryStringParameters']['key'].lower()
        query_string = 'select school_id,school_name,school_type,name_suburb_postcode,address,longitude,latitude from school where lower(name_suburb_postcode) like \'%' + \
            key+'%\';'
        results = handle_query(DATABASE_NAME, output_dir, query_string)
    elif query_resource == '/school/{school_id}':
        school_id = event['pathParameters']['school_id']
        school_query_string = 'select * from school where school_id =' + \
            str(school_id)+';'
        naplan_query_string = 'select A.grade,B.* from school_naplan A inner join naplan B on A.naplan_id=B.naplan_id where A.school_id=' + \
            str(school_id)+';'
        fees_query_string = 'select A.boarding,A.international,B.* from school_fees A inner join fees B on A.fees_id=B.fees_id where A.school_id=' + \
            str(school_id)+';'
        school_results = handle_query(
            DATABASE_NAME, output_dir, school_query_string)
        # check if school result is empty
        if school_results:
            naplan_results = handle_query(
                DATABASE_NAME, output_dir, naplan_query_string)
            fees_results = handle_query(
                DATABASE_NAME, output_dir, fees_query_string)
            school_results[0]["naplan"] = naplan_results
            school_results[0]["fees"] = fees_results
            results = school_results[0]

    # 3. Construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {
        'Access-Control-Allow-Headers': 'Content-Type',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json.dumps(results)

    # 4. Return the response object
    return responseObject


def handle_query(database_name, output_dir, query_string):
    client = boto3.client('athena')
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': database_name
        },
        ResultConfiguration={
            'OutputLocation': output_dir
        },
        ResultReuseConfiguration={
            'ResultReuseByAgeConfiguration': {
                'Enabled': True,
                'MaxAgeInMinutes': 60
            }
        }
    )['QueryExecutionId']

    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        query_status = client.get_query_execution(QueryExecutionId=query_id)[
            'QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception(
                'Athena query with the string "{}" failed or was cancelled'.format(query_string))
    results_obj = client.get_query_results(
        QueryExecutionId=query_id,
        MaxResults=100
    )
    results = []
    data_list = []
    for row in results_obj['ResultSet']['Rows']:
        data_list.append(row['Data'])
    for datum in data_list[1:]:
        data_dict = {}
        for index, item in enumerate(datum):
            if item:
                data_dict[data_list[0][index]
                          ['VarCharValue']] = item['VarCharValue']
            else:
                data_dict[data_list[0][index]['VarCharValue']] = 'None'
        results.append(data_dict)
    return results
