import json
import boto3


def lambda_handler(event, context):
    # Get the service resource

    # TODO implement
    print(event)
    print(context)
    sqs_client = boto3.client('sqs')

    messages = []
    queue_url = 'https://sqs.us-east-1.amazonaws.com//practice'
    j = 1
    while j <= 3:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url
        )
        for i in resp['Messages']:
            print(i)
            message = str(i['Body'])
            receipt_handle = i['ReceiptHandle']
        messages.append(message)
        j = j + 1
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
    sns = boto3.client('sns')
    number = '+1'
    sns.publish(PhoneNumber=number, Message='|'.join(messages))

    return 'ss'

