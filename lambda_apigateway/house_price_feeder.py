import json
import boto3

def lambda_handler(event, context):
    # Extract the local_gov_code from the path parameters
    local_gov_code = event['pathParameters']['local_gov_code']

    # Define the Athena query string
    query_string = f"select distinct houseprice.local_government_code, \
                    school.local_government, \
                    houseprice.median_house_price, \
                    houseprice.year \
                    from houseprice \
                    join school on houseprice.local_government_code = school.local_government_code \
                    where houseprice.local_government_code = '{local_gov_code}' \
                    order by year;"

    # Define the Athena database name and output directory
    DATABASE_NAME = 'school-ratings-gold-bucket'
    output_dir = 's3://datateer-school-rating/athena-apigateway-query/'

    # Create a Boto3 Athena client
    client = boto3.client('athena')

    # Start the Athena query execution
    query_id = client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': DATABASE_NAME
        },
        ResultConfiguration={
            'OutputLocation': output_dir
        }
    )['QueryExecutionId']

    query_status = None
    while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
        # Check the status of the query execution
        query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
        if query_status == 'FAILED' or query_status == 'CANCELLED':
            raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))

    # Retrieve the query results
    results_paginator = client.get_paginator('get_query_results')
    results_iter = results_paginator.paginate(
        QueryExecutionId=query_id,
        PaginationConfig={
            'PageSize': 1000
        }
    )

    results = []
    data_list = []
    for results_page in results_iter:
        for row in results_page['ResultSet']['Rows']:
            data_list.append(row['Data'])

    for datum in data_list[1:]:
        data_dict = {}
        for index, item in enumerate(datum):
            if item:
                data_dict[data_list[0][index]['VarCharValue']] = item['VarCharValue']
            else:
                data_dict[data_list[0][index]['VarCharValue']] = 'None'
        results.append(data_dict)

    # Construct the response object
    responseObject = {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,GET',
            'Content-Type': 'application/json'
        },
        'body': json.dumps(results)
    }

    # Return the response object
    return responseObject