import pyodbc

def get_connector(config):
    driver = config['driver']
    sql_server = config['server']
    database = config['database']
    connection = config['connection']
    sql_server_connect = '{} {} {} {}'.format(driver, sql_server, database, connection)
    return pyodbc.connect(sql_server_connect)