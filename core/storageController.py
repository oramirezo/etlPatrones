import os
import time
import pymysql.cursors
import jaydebeapi
from auth.credentials import MYSQL_SECURITY, HIVE_SECURITY


class MySQL:
    def __init__(self):
        try:
            self.connection = pymysql.connect(host=MYSQL_SECURITY['HOST'],
                                              user=MYSQL_SECURITY['USERNAME'],
                                              password=MYSQL_SECURITY['PASS'],
                                              db=MYSQL_SECURITY['DATABASE'],
                                              port=MYSQL_SECURITY['PORT'])
            self.cursor = self.connection.cursor()
            #print(f'[ok] Success MySQL connection')
        except Exception as e:
            print(f'[error] MySQL connection')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[5.0]',)
            raise

    def insert_data(self, table_name, params_name, params_value):
        params_model = ''
        for c in range(0, len(params_name)):
            if c < (len(params_name) - 1):
                params_model += f'{params_name[c]}, '
            else:
                params_model += params_name[c]
        params_model = '(' + params_model + ')'

        values_model = ''
        for v in range(0, len(params_value)):
            if v < (len(params_value) - 1):
                values_model += '%s, '
            else:
                values_model += '%s'
        values_model = '(' + values_model + ')'

        sql = f'insert into {table_name} {params_model} values {values_model}'
        parameters = tuple(params_value)
        print(f'> Query: {sql}')
        print(f'> Params: {parameters}')
        try:
            self.cursor.execute(sql, parameters)
            self.connection.commit()
            self.connection.close()
        except Exception as e:
            print(f'[error] insert_data. {e}')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[3.1]',)
            raise

    def get_data(self, table_name, data_samples=None):
        if data_samples:
            sql = f'select * from {table_name} order by id desc limit {data_samples}'
        else:
            sql = f'select * from {table_name}'
        try:
            self.cursor.execute(sql)
            result = [dict((self.cursor.description[i][0], value) for i, value in enumerate(row)) for row in
                      self.cursor.fetchall()]
            self.connection.close()
        except Exception as e:
            print(f'[error] get_data. {e}')
            raise
        return result


class Hive:
    def __init__(self):
        try:
            os.environ['javax.net.ssl.trustStore'] = HIVE_SECURITY['trustStore']
            os.environ['javax.net.ssl.trustStorePassword'] = HIVE_SECURITY['trustStorePassword']
            self.connection = jaydebeapi.connect('org.apache.hive.jdbc.HiveDriver',
                                                 f'{HIVE_SECURITY["URL"]}:{HIVE_SECURITY["PORT"]}/;ssl=true;transportMode=http;httpPath=gateway/cdp-proxy-api/hive;'
                                                 f'sslTrustStore={HIVE_SECURITY["trustStore"]};trustStorePassword={HIVE_SECURITY["trustStorePassword"]}',
                                                 ['hesf', 'hesf123_'],
                                                 'hive-jdbc-uber-2.6.5.0-292.jar')
            self.cursor = self.connection.cursor()
            print(f'[ok] Success Hive connection')
        except Exception as e:
            print(f'[error] Hive connection')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[5.0]',)
            raise

    def get_registers(self, db, table):
        sql = f'select count(1) from {db}.{table}'
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()[0]
            return int(result)
        except Exception as e:
            print(f'[error] get_registers. {e}')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[3.2]',)
            raise

    def exec_count_query(self, query):
        try:
            self.cursor.execute(query)
            result = int(self.cursor.fetchone()[0])
            print('[ok] Count query executed successfully!')
            return result
        except Exception as e:
            print(f'[error] exec_count_query. {e}')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[3.2]',)
            raise

    def exec_query(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
            status_exec_query = True
        except Exception as e:
            if 'method not supported' in str(e).lower():
                status_exec_query = True
            else:
                status_exec_query = False
                print(f'[error] exec_query. {e}')
        if status_exec_query:
            print('[ok] Query executed successfully!')
        return status_exec_query

    def select_query(self, query):
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            print('[ok] select_query executed successfully!')
        except Exception as e:
            print(f'[error] select_query. {e}')
            result = None
        return result
