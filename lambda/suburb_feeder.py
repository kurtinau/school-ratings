import json
import boto3
import time



def lambda_handler(event,context):
	
	
	client = boto3.client('athena')

	postcode = event['pathParameters']['postcode']
	
	
	col_name = "suburb"

	query_string = 'select * from suburbinfo where postcode ='+str(postcode)+';'
	DATABASE_NAME='school-ratings-gold-bucket'
	
	
    

	output_dir = 's3://datateer-school-rating/athena-apigateway-query/'


	query_id = client.start_query_execution(QueryString = query_string,QueryExecutionContext = {'Database': DATABASE_NAME },ResultConfiguration = {'OutputLocation': output_dir})['QueryExecutionId']
		
	query_status = None
    
		
	while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
		query_status = client.get_query_execution(QueryExecutionId=query_id)['QueryExecution']['Status']['State']
		if query_status == 'FAILED' or query_status == 'CANCELLED':
			raise Exception('Athena query with the string "{}" failed or was cancelled'.format(query_string))
		# This time is required and can be bring down to 5 sec
		# time.sleep(5)
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
		# print("result:: ", results_page)
		# print("rows: ",results_page['ResultSet']['Rows'])
	# 	print("column: ",results_page['ResultSet']['ResultSetMetadata']['ColumnInfo'])
		for row in results_page['ResultSet']['Rows']:
			data_list.append(row['Data'])
			# print("data: ",row)
	for datum in data_list[1:]:
		data_dict = {}
		for index,item in enumerate(datum):
			if item:
				data_dict[data_list[0][index]['VarCharValue']] = item['VarCharValue']
			else:
				data_dict[data_list[0][index]['VarCharValue']] = 'None'
		results.append(data_dict)
	# print(column_names)
	
	#2. Construct the body of the response object
	# queryResponse = {}
	# queryResponse[col_name] = results


	#3. Construct http response object
	responseObject = {}
	responseObject['statusCode'] = 200
	responseObject['headers'] = {
		'Access-Control-Allow-Headers': 'Content-Type',
		'Access-Control-Allow-Origin': '*',
		'Access-Control-Allow-Methods': 'OPTIONS,GET'
	}
	responseObject['headers']['Content-Type'] = 'application/json'
	responseObject['body'] = json.dumps(results)
	
	#4. Return the response object
	return responseObject