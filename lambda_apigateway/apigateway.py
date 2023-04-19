import json
import boto3

def lambda_handler(event, context):
    # Get query parameters from API Gateway request
    schoolname = event['queryStringParameters'].get('school', '')
    
    if not schoolname:
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'A "school" parameter is required.'})
        }
    
    # Create Athena client
    athena_client = boto3.client('athena')
    
    # Execute Athena query
    print(schoolname)
    query = f'SELECT * FROM "AwsDataCatalog"."school-test"."school" WHERE school_name=\'{schoolname}\''
    response = athena_client.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': 's3://athena-apigateway-test'
        }
    )
    
    # Get query execution ID
    query_execution_id = response['QueryExecutionId']
    
    # Poll for query execution status
    while True:
        query_status = athena_client.get_query_execution(QueryExecutionId=query_execution_id)['QueryExecution']['Status']['State']
        if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
    
    # Check if query succeeded
    if query_status != 'SUCCEEDED':
        return {
            'statusCode': 400,
            'body': json.dumps({'message': 'Query execution failed'})
        }
    
    # Get query results
    query_results = athena_client.get_query_results(QueryExecutionId=query_execution_id)

    # Format and return the query results
    return {
        'statusCode': 200,
        'body': json.dumps(query_results)
    }