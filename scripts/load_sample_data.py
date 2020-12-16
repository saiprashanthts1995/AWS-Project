__purpose__ = 'to load sample data'
__Author__ = 'Sai Prashanth Thalanayar Swaminathan'


from utils import log_steps, udf_exception, timeit, read_config, udf_print, current_path
import pandas as pd
import boto3
import argparse

# create a logger to load data to my sql instance
logger = log_steps('load_sample_data')


def read_data(position):
    if position >= 28:
        print('No rows to process')
        exit(1)
    df = pd.read_csv('../data/departments', sep=',', skiprows=position, nrows=5)
    df.columns = 'department_id,department_name,manager_id,location_id'.split(',')
    return df


def find_start_position():
    df = pd.read_csv('../data/data_position.txt')
    position = df['start_position'].tolist()[0]
    return position


def overwrite_start_position(position):
    df = pd.DataFrame(data=[position+5], columns=['start_position'])
    df.to_csv('../data/data_position.txt', index=False)


def send_data_to_kinesis(df):
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')
    kinesis_name = read_config()['KINESIS']['Stream']
    for i, j in df.iterrows():
        data = '|'.join([str(i) for i in df.iloc[i, :].tolist()])
        partition_key = '{}'.format(j['department_id'])
        kinesis_client.put_record(StreamName=kinesis_name,
                                  Data=data,
                                  PartitionKey=partition_key
                                  )
    return None


def send_data_to_sqs(df):
    sqs = boto3.resource('sqs', region_name='us-east-1')
    queue = sqs.get_queue_by_name(QueueName='practice')
    for i, j in df.iterrows():
        response = queue.send_message(
            MessageBody='boto3',
            MessageAttributes={
                             'department_id': {
                                 'StringValue': '{}'.format(j['department_id']),
                                 'DataType': 'String'
                             },
                             'department_name': {
                                 'StringValue': '{}'.format(j['department_name']),
                                 'DataType': 'String'
                             },
                             'manager_id': {
                                 'StringValue': '{}'.format(j['manager_id']),
                                 'DataType': 'String'
                             },
                             'location_id': {
                                 'StringValue': '{}'.format(j['location_id']),
                                 'DataType': 'String'
                             }
                         }
        )
        print(response['MessageId'])
        return None


@timeit
@udf_exception
def run():

    udf_print('Welcome', type_of_print='Important')
    logger.info('Process Started')
    position = find_start_position()
    data = read_data(position)
    parser = argparse.ArgumentParser()
    parser.add_argument('-mode',
                        '--mode',
                        dest="mode",
                        choices=['sqs', 'kinesis']
                        )
    args = parser.parse_args()
    print(args.mode)

    mode = args.mode

    if mode == 'sqs':
        send_data_to_sqs(data)
    else:
        send_data_to_kinesis(data)

    overwrite_start_position(position)
    logger.info('Process Completed')
    return data


print(run())
