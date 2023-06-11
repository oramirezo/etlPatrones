import os
import shutil
import subprocess
import time
import findspark
from pyspark.sql.types import BooleanType, StringType, StructType, StructField, IntegerType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import when, row_number, lit, col
from core.appTools import appTools
from core.constants import PATH_LOCAL_TEMP_LT, SPARK_LT_DRIVER
from core.storageController import Hive


class pySparkTools:
    def sparkSession_creator(self):
        try:
            os_running = appTools().get_os_system()[0]
            print(f'> Creating Spark session...')
            """
            Use local[x] when running in Standalone mode.
            x should be an integer value and should be greater than 0; this represents how many partitions it 
            should create when using RDD, DataFrame, and Dataset.
            Ideally, x value should be the number of CPU cores you have.
            """

            if os_running == 'windows':
                findspark.init()
                spark_session = SparkSession.builder \
                    .master('local[*]') \
                    .config("spark.executor.memory", "12g") \
                    .config("spark.driver.memory", "1536M") \
                    .config("spark.memory.offHeap.enabled", "true") \
                    .config("spark.memory.offHeap.size", "12288M") \
                    .enableHiveSupport() \
                    .getOrCreate()
            else:
                #.master('spark://master:7077')
                spark_session = SparkSession\
                    .builder \
                    .master('local[*]')\
                    .config("spark.executor.memory", "64g") \
                    .config("spark.driver.memory", "65536M")\
                    .config("spark.memory.offHeap.enabled", "true")\
                    .config("spark.memory.offHeap.size", "12288M") \
                    .enableHiveSupport()\
                    .getOrCreate()
            print(f'[ok] Spark session created successfully!')
            return spark_session
        except Exception as e:
            print(f'[error] sparkSession_creator. {e}')
            return None

    def dataframe_from_file(self, spark_obj, path, delimiter=',', header=False):
        try:
            if header:
                header = 'true'
            else:
                header = 'false'
            df = spark_obj.read.option('header', header).option('delimiter', delimiter).csv(path)
            return df
        except Exception as e:
            print(f'[error] dataFrame_creator. {e}')
            return None

    def dataframe_from_struct(self, spark_obj, dataframe_struct):
        """dataFrame_struct = {'Nomb_VAR': 'varchar2',
                                'C2_NOMBRE_ARCHIVO': 'VARCHAR2',
                                'C3_ESTATUS': 'VARCHAR2'}"""
        try:
            struct_description = []

            for col in dataframe_struct.keys():
                dataType = str(dataframe_struct[col]).lower()
                if (dataType == 'varchar') or (dataType == 'varchar2'):
                    dataType = StringType()
                elif dataType == 'int':
                    dataType = IntegerType()
                elif (dataType == 'double') or (dataType == 'dob'):
                    dataType = DoubleType()
                elif dataType == 'bool':
                    dataType = BooleanType()
                struct_description.append(StructField(col, dataType, True))

            schema = StructType(struct_description)
            emptyRDD = spark_obj.sparkContext.emptyRDD()
            df = spark_obj.createDataFrame(emptyRDD, schema=schema)
        except Exception as e:
            print(f'[error] dataFrame_from_struct. {e}')
            df = None, None
        return df, schema

    def rename_col(self, dataframe, new_colsNames):
        try:
            colsNames = dataframe.columns
            for c in range(0, len(colsNames)):
                dataframe = dataframe.withColumnRenamed(colsNames[c], new_colsNames[c])
            return dataframe
        except Exception as e:
            print(f'[error] rename_col. {e}')
            return dataframe

    def lt_to_dataframe(self, spark_obj, hdfs_path_file):
        if hdfs_path_file:
            dataframe = self.dataframe_from_file(spark_obj=spark_obj, path=hdfs_path_file,
                                                 delimiter='|', header=True)
        else:
            dataframe = None
        return dataframe

    def dataframe_to_lt(self, start_datetime_params, file_name, dataframe, db_table_lt):
        start_datetime = start_datetime_params[0]
        start_datetime_proc = start_datetime_params[1]
        process_name = appTools().get_proc_etl_name()
        if type(process_name) == list():
            print(process_name)
            process_name = process_name[0]
        print(f'> Exporting data from LT -> Hive LT: {db_table_lt}')
        try:
            dataframe.coalesce(1).write.format('csv').option('header', 'true')\
                .mode('overwrite').option('sep', '|').save(f'{PATH_LOCAL_TEMP_LT}/{file_name}')
            print(f'[ok] File csv created in: {PATH_LOCAL_TEMP_LT}/{file_name}')
            csv_created = True
        except Exception as e:
            err = f'dataframe_to_lt. {e}'
            print(f'[error] {err}')
            error_id = '1.0'
            error_description = err
            print('> Sending error to cifras_control...')
            ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_lt,
                                                  start_datetime=start_datetime,
                                                  start_datetime_proc=start_datetime_proc,
                                                  end_datetime_proc=appTools().get_datime_now(),
                                                  error_id=error_id,
                                                  error_description=error_description,
                                                  process_name=process_name)
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            csv_created = False

        if csv_created:
            csv_file_path = ''
            try:
                for dirOrFile in os.listdir(f'{PATH_LOCAL_TEMP_LT}/{file_name}'):
                    if dirOrFile.endswith(('.csv', '.CSV')):
                        csv_file_path = f'{PATH_LOCAL_TEMP_LT}/{file_name}/{dirOrFile}'
                        break
                if not os.path.isfile(csv_file_path):
                    raise ValueError(f'File does not exist {csv_file_path}')
            except Exception as e:
                err = f'dataframe_to_lt. Validate csv_file_path. {e}'
                print(f'[error] {err}')
                error_id = '1.0'
                error_description = err
                print('> Sending error to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_lt,
                                                      start_datetime=start_datetime,
                                                      start_datetime_proc=start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)
                csv_file_path = ''
            if csv_file_path:
                try:
                    print(f'> Moving Local csv file to hdfs...')
                    command = f'hdfs dfs -put {csv_file_path} /bdaimss/la/lt_aficobranza/prueba_tmp'
                    print(f'> exec command: {command}')
                    subprocess.run([command], shell=True, check=True)
                except subprocess.CalledProcessError as e:
                    err = f'dataframe_to_lt HDFS. {e}'
                    print(f'[error] {err}')
                    error_id = '1.0'
                    error_description = err
                    print('> Sending error to cifras_control...')
                    ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_lt,
                                                          start_datetime=start_datetime,
                                                          start_datetime_proc=start_datetime_proc,
                                                          end_datetime_proc=appTools().get_datime_now(),
                                                          error_id=error_id,
                                                          error_description=error_description,
                                                          process_name=process_name)
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    return None
                status_error_lt = True
                try:
                    db_table_name = db_table_lt.split('.')
                    db_name = db_table_name[0]
                    table_name = db_table_name[1]
                    hdfs_csv_file_path = f'hdfs://cnhcsepraphadoop-0001.imss.gob.mx:8020/bdaimss/la/' \
                                         f'lt_aficobranza/prueba_tmp/{dirOrFile}'
                    command = f'spark-submit {SPARK_LT_DRIVER} {hdfs_csv_file_path} {db_name} {table_name}'
                    print('> Exporting data as LT table...')
                    print(f'> exec command: {command}')
                    subprocess.run([command], shell=True, check=True)
                    status_error_lt = False
                except subprocess.CalledProcessError as e:
                    err = f'dataframe_to_lt. Spark driver. {e}'
                    print(f'[error] {err}')
                    error_id = '1.0'
                    error_description = err
                    print('> Sending [error] status to cifras_control...')
                    ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_lt,
                                                          start_datetime=start_datetime,
                                                          start_datetime_proc=start_datetime_proc,
                                                          end_datetime_proc=appTools().get_datime_now(),
                                                          error_id=error_id,
                                                          error_description=error_description,
                                                          process_name=process_name)
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)
                    status_error_lt = True
                    return None
                finally:
                    if not status_error_lt:
                        print('> Sending [ok] status to cifras_control...')
                        ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_lt,
                                                              start_datetime=start_datetime,
                                                              start_datetime_proc=start_datetime_proc,
                                                              end_datetime_proc=appTools().get_datime_now(),
                                                              error_id='0',
                                                              error_description='',
                                                              process_name=process_name)
                        appTools().error_logger(ctrl_cif=ctrl_cfrs)
                """
                try:
                    if os.path.isfile(csv_file_path):
                        print(f'> Removing temp CSV file {csv_file_path}...')
                        #shutil.rmtree(f'{PATH_LOCAL_TEMP_LT}/{file_name}')
                        command = f'rm -r {PATH_LOCAL_TEMP_LT}/{file_name}'
                        print(f'> exec command: {command}')
                        subprocess.run([command], shell=True, check=True)
                        print(f'[ok] Directory {PATH_LOCAL_TEMP_LT}/{file_name} is deleted')
                except subprocess.CalledProcessError as e:
                    print(f'[error] dataframe_to_lt. Removing dir/file {csv_file_path}. {e}')
                try:
                    command = f'hdfs dfs -rm /bdaimss/la/lt_aficobranza/prueba_tmp/{dirOrFile}'
                    print(f'> exec command: {command}')
                    subprocess.run([command], shell=True, check=True)
                except subprocess.CalledProcessError as e:
                    print(f'[error] dataframe_to_lt. Removing file in HDFS. {e}')
                """
                return f'{PATH_LOCAL_TEMP_LT}/{file_name}'
            else:
                return None
        else:
            return None

    def dataframe_to_te(self, start_datetime_params, file_name, dataframe, db_table_lt, optional_query_to_te=None):
        os_running = appTools().get_os_system()[0]
        te_insert_status = None
        if os_running != 'windows':
            process_name = appTools().get_proc_etl_name()
            if type(process_name) == list():
                print(f'> process_name: {process_name}')
                process_name = process_name[0]
            starting_insert_time = time.time()
            file_name = file_name.split('.')[0]
            # Insert to LT
            lt_insert_status = self.dataframe_to_lt(start_datetime_params=start_datetime_params, file_name=file_name,
                                                    dataframe=dataframe, db_table_lt=db_table_lt)
            # Insert to TE
            if lt_insert_status:
                try:
                    db_table_te = f'te{db_table_lt[2:]}'
                    print(f'> Exporting data from LT -> TE: {db_table_te}')
                    if optional_query_to_te:
                        query = optional_query_to_te
                    else:
                        query = f'insert into {db_table_te} select * from {db_table_lt}'
                    print(f'> Query to Hive: {query}')
                    status_query_hive = Hive().exec_query(query)
                    if not status_query_hive:
                        error_id = '1.0'
                        error_description = f'Hive query: {query}'
                    else:
                        elapsed_insert_seconds = round(time.time() - starting_insert_time, 3)
                        print(f'[ok] Dataframe inserted successfully -> TE {db_table_te}. '
                              f'Executed on {elapsed_insert_seconds}sec.')
                        error_id = None
                except Exception as e:
                    error_id, error_description = appTools().get_error(error=e)
                finally:
                    if error_id:
                        print('> Sending [error] status to cifras_control...')
                        ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_te,
                                                              start_datetime=start_datetime_params[0],
                                                              start_datetime_proc=start_datetime_params[1],
                                                              end_datetime_proc=appTools().get_datime_now(),
                                                              error_id=error_id,
                                                              error_description=error_description,
                                                              process_name=process_name)
                        appTools().error_logger(ctrl_cif=ctrl_cfrs)
                        te_insert_status = None
                    else:
                        print('> Sending [ok] status to cifras_control...')
                        ctrl_cfrs = appTools().cifras_control(db_table_name=db_table_te,
                                                              start_datetime=start_datetime_params[0],
                                                              start_datetime_proc=start_datetime_params[1],
                                                              end_datetime_proc=appTools().get_datime_now(),
                                                              error_id='0',
                                                              error_description='',
                                                              process_name=process_name)
                        appTools().error_logger(ctrl_cif=ctrl_cfrs)
                        te_insert_status = lt_insert_status
        else:
            print(f'[pending] dataframe_to_te {db_table_lt}. Process running on {os_running}')
            te_insert_status = None
        return te_insert_status

    def describe_dataframe(self, dataframe, dataframe_name):
        os_running = appTools().get_os_system()[0]
        if os_running != 'windows':
            DF_LEN_ROWS = dataframe.count()
        else:
            DF_LEN_ROWS = '-'
        DF_LEN_COLS = len(dataframe.columns)
        print(f'[ok] Dataframe {dataframe_name} complete. '
              f'Rows = {DF_LEN_ROWS}, Cols = {DF_LEN_COLS}.')
        dataframe.printSchema()

    def enumerate_rows(self, dataframe, col_name):
        windowSpec = Window.orderBy(lit('*'))
        dataframe = dataframe.withColumn(col_name, row_number().over(windowSpec).cast(StringType()))
        return dataframe.withColumn(col_name, when(col(col_name).isNotNull(), col(col_name)).otherwise(lit(None)))
