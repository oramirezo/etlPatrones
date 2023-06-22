SYSTEM_PERFORMANCE_PERIOD = 5.0
SYSTEM_RUNTIME_PERIOD_H = 12.0

SYSTEM_PERFORMANCE_MONITOR_CONTROL = False
CIFRAS_ERROR_LOG = True

# Para deshabilitar = None
SERVER_LIMIT_DATA_LEN = None

SPARK_LT_DRIVER = '/data/users/oscar_ramirez/etlPatrones/core/sparkDriver.py'

PATH_LOCAL_TEMP_LT = '/data/users/oscar_ramirez/aficobranza/ssregp/lt'
PATH_HDFS_TEMP_LT = '/bdaimss/la/lt_aficobranza/prueba_tmp'

TO_EMAIL_LIST = 'hermo.rodriguez@people-media.com.mx,oscar.ramirez@people-media.com.mx,jose.saldana@people-media.com.mx'
#TO_EMAIL_LIST='susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

#'hdfs://cnhcsepraphadoop-0001.imss.gob.mx:8020/bdaimss/la/lt_aficobranza/prueba_tmp'
PATH_LOCAL_TEMP_LA = '/data/users/oscar_ramirez/aficobranza/ssregp/la'
#PATH_LOCAL_TEMP_LA_WIN = 'C:/Users/hrodriguez/Documents/imss/la'
PATH_LOCAL_TEMP_LA_WIN = 'C:/proyectos/imss/patrones/la'
ERROR_TYPE = {
            '0': '',
            '0.0': '',
            '1.0': 'Error in the processing of variables',
            '2.0': 'Sqoop process error',
            '2.1': 'Consolidation process error',
            '2.2': 'Load process error',
            '2.3': 'Integration process error',
            '3.0': 'Cleaning process error',
            '3.1': 'Export process error',
            '3.2': 'Ingest process error',
            '5.0': 'Connection error'}
