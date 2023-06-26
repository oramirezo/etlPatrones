import os
import re
import subprocess
import threading
import datetime
import sys
import time
from core.appTools import appTools
from core.constants import SERVER_LIMIT_DATA_LEN, TO_EMAIL_LIST
from core.storageController import Hive
from core.pySparkTools import pySparkTools
from core.constants import CIFRAS_ERROR_LOG
from pyspark.sql.functions import col, lit, udf, substring, when, sum, concat, count, first
from pyspark.sql.types import StringType
import dateutil


@udf(returnType=StringType())
def decodificador_rsua(s):
    """
    (p_string VARCHAR2)
    RETURN VARCHAR2 AS v_string VARCHAR2(32767);
    """
    p_string = str(s)[:32767]

    """
    V_Posicion1 VARCHAR2(10);
    V_Posicion2 VARCHAR2(10);
    V_Posicion3 VARCHAR2(10);
    V_Bandera NUMBER;
    ValorPosicion1 VARCHAR2(32767);
    ValorPosicion2 VARCHAR2(32767);
    ValorPosicion3 VARCHAR2(32767);
    """
    V_Posicion1 = p_string[0]
    V_Posicion2 = p_string[1]
    V_Posicion3 = p_string[2]
    V_Bandera = 1

    if 48 <= ord(V_Posicion1) <= 57:
        ValorPosicion1 = (ord(V_Posicion1) - 48) * 3844
    elif 65 <= ord(V_Posicion1) <= 90:
        ValorPosicion1 = (ord(V_Posicion1) - 55) * 3844
    elif 97 <= ord(V_Posicion1) <= 122:
        ValorPosicion1 = (ord(V_Posicion1) - 61) * 3844
    else:
        # ValorPosicion1 = ord(V_Posicion1)
        V_Bandera = 1

    if 48 <= ord(V_Posicion2) <= 57:
        ValorPosicion2 = (ord(V_Posicion2) - 48) * 62
    elif 65 <= ord(V_Posicion2) <= 90:
        ValorPosicion2 = (ord(V_Posicion2) - 55) * 62
    elif 97 <= ord(V_Posicion2) <= 122:
        ValorPosicion2 = (ord(V_Posicion2) - 61) * 62
    else:
        # ValorPosicion2 = ord(V_Posicion2)
        V_Bandera = 1

    if 48 <= ord(V_Posicion3) <= 57:
        ValorPosicion3 = (ord(V_Posicion3) - 48) * 1
    elif 65 <= ord(V_Posicion3) <= 90:
        ValorPosicion3 = (ord(V_Posicion3) - 55) * 1
    elif 97 <= ord(V_Posicion3) <= 122:
        ValorPosicion3 = (ord(V_Posicion3) - 61) * 1
    else:
        # ValorPosicion3 = ord(V_Posicion3)
        V_Bandera = 1

    if V_Bandera == 0:
        v_string = (ValorPosicion1 + ValorPosicion2 + ValorPosicion3) / 100
    else:
        v_string = 0
    return str(v_string)


@udf(returnType=StringType())
def replace_comma_func(s):
    s = str(s).replace(',', '')
    return s


@udf(returnType=StringType())
def special_func_replace_0353(s):
    s = str(s).replace('0353', '')
    return s


@udf(returnType=StringType())
def special_func_cve_nss(s):
    if s is None:
        return '999999999999999'
    else:
        if (s == 'Null') or (s == 'None') or (s == '    ') or (s == '        ') or (s == '           '):
            return '999999999999999'
        else:
            return s


@udf(returnType=StringType())
def special_func_folio_incapacidad(s):
    # DECODE(
    # dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8))
    # s, NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8))) AS FOLIO_INCAPACIDAD
    if (s == 'Null') or (s == 'None') or (s == '        ') or (s == '') or (s is None):
        return '0'
    else:
        return s


@udf(returnType=StringType())
def special_func_fec_movto_incidencia(s):
    # DECODE(
    # dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8))
    # s, NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8))) AS FEC_MOVTO_INCIDENCIA
    if (s == 'Null') or (s == 'None') or (s == '        ') or (s == '') or (s is None):
        return '99991231'
    elif s == '00000000':
        return '99991231'
    else:
        return s


@udf(returnType=StringType())
def special_func_fec_ini_desc_info(s):
    try:
        dateutil.parser.parse(s, fuzzy=False)
        is_date = True
    except ValueError:
        is_date = False
    if is_date:
        return s
    else:
        return '19000101'


@udf(returnType=StringType())
def div_10_func(s):
    return float(s) / 10


@udf(returnType=StringType())
def div_100_func(s):
    return float(s) / 100


@udf(returnType=StringType())
def div_100000_func(s):
    return float(s) / 100000


@udf(returnType=StringType())
def div_1000000_func(s):
    return float(s) / 1000000


@udf(returnType=StringType())
def file_name_rsua(s):
    return f'RSUA{s}'


@udf(returnType=StringType())
def trim_func(s):
    original_s = s
    try:
        s = s.lstrip(' ')
        s = s.rstrip(' ')
        return s
    except Exception as e:
        print(f'[error] trim_func. {e}')
        return original_s


@udf(returnType=StringType())
def limpia_char(s):
    original_s = s
    try:
        """
        CREATE OR REPLACE FUNCTION DM_ADMIN.Limpiachar                                                
          (p_string    VARCHAR2)                                                                
        RETURN VARCHAR2                                                                 
        AS                                                                                    
          v_string    VARCHAR2(32767);  
        BEGIN             
          v_string := REPLACE(REPLACE(REPLACE(replace(replace(
          replace(replace(replace(replace(replace(
          replace(replace(replace(replace(trim(p_string),'|'),','),'$',' '),'"'),'_'),'}'),'')),'@'),'}'),'     ????'),'????'),'?'), 'yyyymmdd');
          RETURN v_string
        """
        s = s[:32767]
        # del_chars = ['|', ',', '$', ' ', '"', '_', '}', '@', '}', '     ????', '????', '?', 'yyyymmdd']
        s = str(s).replace('$', ' ').replace('_', ' ').replace(chr(92), '')
        del_chars = ['|', ',', '"', '}', '@', '}', '     ????', '????', '?', 'yyyymmdd']
        for c in del_chars:
            s = str(s).replace(c, '')
        s = s.lstrip(' ')
        s = s.rstrip(' ')
        return s
    except Exception as e:
        print(f'[error] limpia_char. {e}')
        return original_s


@udf(returnType=StringType())
def get_number(s):
    original_s = s
    try:
        """
        CREATE OR REPLACE FUNCTION DM_ADMIN.GET_NUMBER 
        (
          CARACTERES IN VARCHAR2 
        ) RETURN NUMBER
        IS 
          l_num NUMBER;
        BEGIN
            l_num := to_number(regexp_replace(CARACTERES, '^[0-9]*\.(^[0-9])+', ''));
            l_num := NVL(l_num,0); -> si es null lo cambia por 0
            return l_num;
        EXCEPTION
          WHEN value_error THEN
            RETURN 0;  
        END GET_NUMBER;"""
        s = re.sub(r'^[0-9]*\.(^[0-9])+', '', s)

        if not s:
            return '0'
        else:
            if s.isnumeric():
                return s
            else:
                return '0'
    except Exception as e:
        print(f'[error] get_number. {e}')
        return original_s


@udf(returnType=StringType())
def decode_99_func(s):
    try:
        """
        DECODE(s), 0, 99, s) AS DEL_IMSS
        """
        if s == '0':
            return '99'
        else:
            return s
    except Exception as e:
        print(f'[error] decode_func. {e}')


class etl(threading.Thread):
    def __init__(self, start_datetime, start_datetime_etl, start_datetime_proc, C1_PROYECTO_v, C2_NOMBRE_ARCHIVO_v,
                 C3_ESTATUS_v):
        threading.Thread.__init__(self)
        self.start_datetime = start_datetime
        self.start_datetime_etl = start_datetime_etl
        self.start_datetime_proc = start_datetime_proc
        self.C1_PROYECTO_v = C1_PROYECTO_v
        self.C2_NOMBRE_ARCHIVO_v = C2_NOMBRE_ARCHIVO_v
        self.C3_ESTATUS_v = C3_ESTATUS_v

    def run(self):
        os_running = appTools().get_os_system()[0]
        if os_running == 'windows':
            from core.constants import PATH_LOCAL_TEMP_LA_WIN as PATH_LOCAL_TEMP_LA
        else:
            appTools().get_kerberos_ticket()
            from core.constants import PATH_LOCAL_TEMP_LA

        process_name = appTools().get_proc_etl_name()
        print(f'> Running process appEtl for \033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m...')
        print(f'> Project: {self.C1_PROYECTO_v} | File name: {self.C2_NOMBRE_ARCHIVO_v} | Status: {self.C3_ESTATUS_v}')

        spark_obj = pySparkTools().sparkSession_creator()
        if not spark_obj:
            e = f'Failure sparkSession is None.'
            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e
            ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                       start_datetime_proc=self.start_datetime_proc,
                                                       end_datetime_proc=appTools().get_datime_now(),
                                                       error_id=error_id,
                                                       error_description=error_description,
                                                       process_name=process_name,
                                                       des_proceso='Inico de sparkSession', fuente='cobranza')
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()

        # Dataframe principal e_carga_rsua
        DM_ADMIN_E_CARGA_PATRONES = pySparkTools().dataframe_from_file(spark_obj=spark_obj,
                                                                   path=f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}',
                                                                   delimiter=',', header=False)
        if DM_ADMIN_E_CARGA_PATRONES:

            new_colsNames = ['resto']
            DM_ADMIN_E_CARGA_PATRONES = pySparkTools().rename_col(dataframe=DM_ADMIN_E_CARGA_PATRONES,
                                                              new_colsNames=new_colsNames)

            # Caracteristicas del Dataframe
            DM_ADMIN_E_CARGA_PATRONES_LEN_COLS = len(DM_ADMIN_E_CARGA_PATRONES.columns)
            DM_ADMIN_E_CARGA_PATRONES_LEN_ROWS = DM_ADMIN_E_CARGA_PATRONES.count()
            file_spec = os.stat(f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}')
            print(f'[ok] File <\033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m> loaded to dataframe '
                  f'DM_ADMIN_E_CARGA_RSUA successfully!')
            print(f'> Dataframe size: Rows= {DM_ADMIN_E_CARGA_PATRONES_LEN_ROWS}, Cols = {DM_ADMIN_E_CARGA_PATRONES_LEN_COLS}.')
            print(f'> File size: {round(file_spec.st_size / 1024 / 1024, 1)}MB')
            DM_ADMIN_E_CARGA_PATRONES.printSchema()
            DM_ADMIN_E_CARGA_PATRONES.show(20)
            dt_now = appTools().get_datime_now()
            SYS_DATE = dt_now.strftime('%Y-%m-%d %H:%M:%S.%f')
            print(SYS_DATE)

            Patrones_TEMP_DF = DM_ADMIN_E_CARGA_PATRONES.withColumn('CVE_REG_PATRONAL', substring(col('resto'), 1, 11)) \
                .withColumn('REG_PATRONAL', substring(col('resto'), 1, 8)) \
                .withColumn('CVE_MODALIDAD', substring(col('resto'), 9, 2)) \
                .withColumn('DIG_VERIFICADOR', substring(col('resto'), 11, 1)) \
                .withColumn('FEC_REGISTRO', substring(col('resto'), 12, 10)) \
                .withColumn('HORA_REGISTRO', substring(col('resto'), 23, 8)) \
                .withColumn('CVE_DELEGACION', substring(col('resto'), 31, 2)) \
                .withColumn('CVE_SUBDELEGACION', substring(col('resto'), 33, 2)) \
                .withColumn('FEC_CARGA', lit(SYS_DATE))
            drop_cols = ['resto']

            Patrones_TEMP_DF = Patrones_TEMP_DF.drop(*drop_cols)
            Patrones_TEMP_DF.show(20)
            Patrones_TEMP_DF.printSchema()

            # insert into SUT_SIPA_REGISTRO_PATRONES -> Hive
            new_colsNames = [
                'cve_reg_patronal',
                'reg_patronal',
                'cve_modalidad',
                'dig_verificador',
                'fec_registro',
                'hora_registro',
                'cve_delegacion',
                'cve_subdelegacion',
                'fec_carga']
            Patrones_DF_TO_HIVE = pySparkTools().rename_col(dataframe=Patrones_TEMP_DF, new_colsNames=new_colsNames)
            Patrones_DF_TO_HIVE= Patrones_DF_TO_HIVE.withColumn('fecha_carga', lit(SYS_DATE))
            Patrones_DF_TO_HIVE = Patrones_DF_TO_HIVE.withColumn('fecha_proceso', lit(SYS_DATE))

            Patrones_DF_TO_HIVE.show(20)
            # Insert dataframe to TE stage
            path_file_sut_sipa_02encabezado_patron = pySparkTools().dataframe_to_te(
                start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                file_name=self.C2_NOMBRE_ARCHIVO_v,
                dataframe=Patrones_DF_TO_HIVE,
                db_table_lt='lt_aficobranza.SUT_SIPA_REGISTRO_PATRONES')

            Patrones_TEMP_DF.unpersist()

            Patrones_DF_TO_HIVE.unpersist()




        else:
            e = f'Failure on dataframe creation: DM_ADMIN_E_CARGA_PATRONES'
            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e
            ctrl_cfrs = appTools().cifras_controlError(start_datetime=self.start_datetime,
                                                       start_datetime_proc=self.start_datetime_proc,
                                                       end_datetime_proc=appTools().get_datime_now(),
                                                       error_id=error_id,
                                                       error_description=error_description,
                                                       process_name=process_name,
                                                       des_proceso='paso de datos de txt a dataframe',
                                                       fuente='cobranza')
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()
