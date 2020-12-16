__purpose__ = 'to load data to mysql'
__Author__ = 'Sai Prashanth Thalanayar Swaminathan'


from utils import log_steps, load_sql_connection, udf_exception, timeit, read_config, udf_print, \
    current_path, mysql_connection
import pandas as pd
import json

# create a logger to load data to my sql instance
logger = log_steps('load_to_mysql')


def run_sql_statements(statement, option='dml'):
    connection = mysql_connection()
    cursor = connection.cursor()
    if option != 'ddl':
        sql = statement['Statements']
    else:
        sql = statement
    cursor.execute('{}'.format(sql))
    connection.commit()
    connection.close()
    return None


def read_data():
    df = pd.read_csv('../data/insert_statements.txt', sep='|')
    return df


def read_ddl_statements():
    with open('../data/ddl_statement.json') as ddl_config:
        content = json.load(ddl_config)
    return content


def load_data():
    data = read_data()
    data.apply(run_sql_statements, axis=1)
    logger.info('Data Load Completed Successfully')
    return None


def execute_ddl():
    statements = read_ddl_statements()
    drop_statement_employees = statements['drop_statement_employees']
    drop_statement_departments = statements['drop_statement_departments']
    create_statement_departments = statements['create_statement_departments']
    create_statement_employees = statements['create_statement_employees']
    print(statements)
    logger.info('dropping the table employees')
    run_sql_statements(drop_statement_employees, 'ddl')
    logger.info('dropping the table departments')
    run_sql_statements(drop_statement_departments, 'ddl')
    logger.info('creating the table employees')
    run_sql_statements(create_statement_departments, 'ddl')
    logger.info('creating the table departments')
    run_sql_statements(create_statement_employees, 'ddl')
    udf_print('DDL Completed', type_of_print='Important')
    logger.info('DDL Completed Successfully')
    return None


@timeit
@udf_exception
def run():
    udf_print('Welcome', type_of_print='Important')
    logger.info('Process Started')
    execute_ddl()
    load_data()
    logger.info('Process Completed')


print(run())
