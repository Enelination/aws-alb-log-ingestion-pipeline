import boto3
import gzip
import io
import json
import time

# Initializing S3 and CloudWatch Logs clients
s3_client = boto3.client('s3')
logs_client = boto3.client('logs')

# CloudWatch log group name
log_group_name = '/aws/alb/prodbox_alb'

# Create a log group if it doesn't exist
def create_log_group_if_not_exists(log_group_name):
    try:
        logs_client.create_log_group(logGroupName=log_group_name)
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log group '{log_group_name}' already exists.")

# Create a log stream
def create_log_stream(log_group_name, log_stream_name):
    try:
        logs_client.create_log_stream(
            logGroupName=log_group_name,
            logStreamName=log_stream_name
        )
        print(f"Log stream '{log_stream_name}' created.")
    except logs_client.exceptions.ResourceAlreadyExistsException:
        print(f"Log stream '{log_stream_name}' already exists.")

# Process log file from S3
def process_s3_object(bucket_name, key):
    s3_object = s3_client.get_object(Bucket=bucket_name, Key=key)
    log_data = s3_object['Body'].read()
    log_lines = []

    try:
        with gzip.GzipFile(fileobj=io.BytesIO(log_data)) as gzipfile:
            log_lines = gzipfile.read().decode('utf-8').splitlines()
    except OSError:
        log_lines = log_data.decode('utf-8').splitlines()

    return log_lines

# Main Lambda handler function
def lambda_handler(event, context):
    try:
        create_log_group_if_not_exists(log_group_name)
        
        # Retrieve S3 bucket and prefix from the event
        records = event.get('Records', [])
        if not records:
            raise ValueError("No 'Records' found in the event")

        s3_bucket = 'ejara-alb-logs'
        prefix = 'AWSLogs/041482868249/elasticloadbalancing/us-east-2/2024/09/03/'
        
        # Continue listing S3 objects if there are more than 1,000 objects
        continuation_token = None
        while True:
            list_params = {
                'Bucket': s3_bucket,
                'Prefix': prefix
            }
            if continuation_token:
                list_params['ContinuationToken'] = continuation_token
            
            response = s3_client.list_objects_v2(**list_params)
            
            if 'Contents' not in response:
                print(f"No objects found in the bucket '{s3_bucket}' with prefix '{prefix}'")
                break

            for obj in response['Contents']:
                s3_key = obj['Key']
                print(f"Processing bucket: {s3_bucket}, key: {s3_key}")

                try:
                    log_lines = process_s3_object(s3_bucket, s3_key)

                    log_events = []
                    for line in log_lines:
                        log_events.append({
                            'timestamp': int(round(time.time() * 1000)),
                            'message': line
                        })

                    if log_events:
                        log_stream_name = f"log_stream-{int(time.time() // 86400)}"
                        create_log_stream(log_group_name, log_stream_name)
                        
                        # Upload batched log events to CloudWatch Logs
                        batch_size = 0
                        batch_events = []
                        for event in log_events:
                            event_size = len(json.dumps(event))
                            if batch_size + event_size > 1048576:
                                if batch_events:
                                    logs_client.put_log_events(
                                        logGroupName=log_group_name,
                                        logStreamName=log_stream_name,
                                        logEvents=batch_events
                                    )
                                    print(f"Uploaded batch of size {batch_size} bytes.")
                                batch_events = []
                                batch_size = 0
                            batch_events.append(event)
                            batch_size += event_size
                        
                        if batch_events:
                            logs_client.put_log_events(
                                logGroupName=log_group_name,
                                logStreamName=log_stream_name,
                                logEvents=batch_events
                            )
                            print(f"Uploaded final batch of size {batch_size} bytes.")

                except s3_client.exceptions.NoSuchKey:
                    print(f"Error: The key '{s3_key}' does not exist in bucket '{s3_bucket}'")
                    continue

            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break

        return {
            'statusCode': 200,
            'body': json.dumps('Success')
        }

    except Exception as e:
        print(f"Exception: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
