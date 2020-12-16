import boto3

client = boto3.client('glue',region_name = 'us-east-1')

response = client.delete_job(
    JobName='string'
)
print(response)