__purpose__ = "To put all the default functionality so that other modules can reuse"
__Author__ = 'Sai Prashanth Thalanayar Swaminathan'

from loguru import logger
from sqlalchemy import create_engine
from datetime import datetime
import json
import os
from mysql import connector as mc


####################### to log an activity #####################


def log_steps(filename):
    logger.add(
        '../logs/{filename}.log'.format(filename=filename),
        level="INFO"
    )
    return logger


####################### DB connection ###########################

def mysql_connection():
    config = read_config()
    config = config['RDS']
    connection = mc.connect(**config)
    logger.info('Connection to MYSQL is successful')
    return connection


def load_sql_connection():
    config = read_config()
    print(config)
    config = config['RDS']
    print(config)
    engine_string = 'mysql+pymysql://{user}:{password}@{host}:{port}/{database}'.format(user=config['user'],
                                                                                        password=config['password'],
                                                                                        host=config['host'],
                                                                                        database=config['database'],
                                                                                        port=config['port']
                                                                                        )
    engine = create_engine(engine_string, echo=False)
    print(engine)
    connection = engine.connect()
    return connection


###################### exception ################################


def udf_exception(method1):
    def udf_method(*args, **kwargs):
        try:
            flag = True
            result = method1(*args, **kwargs)
            flag = False
            return result
        except Exception as e:
            print(e)
            logger.exception(e)
        finally:
            if flag:
                print('exiting the process! due to error above')
                logger.exception('exiting the process! due to error above')
                exit(1)

    return udf_method


##################### time ######################################


def timeit(method1):
    def time_method(*args, **kwargs):
        start_time = datetime.now()
        result = method1(*args, **kwargs)
        end_time = datetime.now()
        print('Total time taken is {}'.format(end_time - start_time))
        return result

    return time_method


##################### read config ##############################


def read_config():
    with open('../config/config.json') as config_file:
        content = json.load(config_file)
    return content


#################### printer ####################################


def udf_print(message, type_of_print='usual'):
    if type_of_print == 'usual':
        print('{}'.format(message))
    else:
        print('#' * 30)
        print('{}'.format(message))
        print('#' * 30)


##################### current path ###############################

def current_path():
    return os.path.dirname(os.path.realpath(__file__))
