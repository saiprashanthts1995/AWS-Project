__purpose__ = 'to load data to ec2'
__Author__ = 'Sai Prashanth Thalanayar Swaminathan'


from utils import log_steps, load_sql_connection, udf_exception, timeit, read_config, udf_print
import pandas as pd

# create a logger to load data to my sql instance
logger = log_steps('load_to_ec2')


def read_data(table_name):
    connection = load_sql_connection()
    df = pd.read_sql_table(table_name=table_name, con=connection)
    return df


def write_data(table_name):
    df = read_data(table_name)
    df.to_csv("../data/{table_name}".format(table_name=table_name), index=False)
    return None


@timeit
@udf_exception
def run():
    udf_print('Welcome', type_of_print='Important')
    logger.info('Process Started')
    logger.info('Process to write employees table started')
    write_data('employees')
    logger.info('Process to write employees table ended')
    logger.info('Process to write departments table started')
    write_data('departments')
    logger.info('Process to write departments table ended')
    logger.info('Process Completed')


print(run())
