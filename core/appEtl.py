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
            ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                  start_datetime=self.start_datetime,
                                                  start_datetime_proc=self.start_datetime_proc,
                                                  end_datetime_proc=appTools().get_datime_now(),
                                                  error_id=error_id,
                                                  error_description=error_description,
                                                  process_name=process_name)
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()

        # Dataframe principal e_carga_rsua
        DM_ADMIN_E_CARGA_RSUA = pySparkTools().dataframe_from_file(spark_obj=spark_obj,
                                                                   path=f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}',
                                                                   delimiter='^', header=False)
        if DM_ADMIN_E_CARGA_RSUA:
            new_colsNames = ['id', 'tp_mov', 'resto']
            DM_ADMIN_E_CARGA_RSUA = pySparkTools().rename_col(dataframe=DM_ADMIN_E_CARGA_RSUA,
                                                              new_colsNames=new_colsNames)

            # Caracteristicas del Dataframe
            DM_ADMIN_E_CARGA_RSUA_LEN_COLS = len(DM_ADMIN_E_CARGA_RSUA.columns)
            DM_ADMIN_E_CARGA_RSUA_LEN_ROWS = DM_ADMIN_E_CARGA_RSUA.count()
            file_spec = os.stat(f'{PATH_LOCAL_TEMP_LA}/{self.C2_NOMBRE_ARCHIVO_v}')
            print(f'[ok] File <\033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m> loaded to dataframe '
                  f'DM_ADMIN_E_CARGA_RSUA successfully!')
            print(f'> Dataframe size: Rows= {DM_ADMIN_E_CARGA_RSUA_LEN_ROWS}, Cols = {DM_ADMIN_E_CARGA_RSUA_LEN_COLS}.')
            print(f'> File size: {round(file_spec.st_size / 1024 / 1024, 1)}MB')
            DM_ADMIN_E_CARGA_RSUA.printSchema()
        else:
            e = f'Failure on dataframe creation: DM_ADMIN_E_CARGA_RSUA'
            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e
            ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                  start_datetime=self.start_datetime,
                                                  start_datetime_proc=self.start_datetime_proc,
                                                  end_datetime_proc=appTools().get_datime_now(),
                                                  error_id=error_id,
                                                  error_description=error_description,
                                                  process_name=process_name)
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()

        # Paso 2: Int. Carga nombre de archivos
        dt_now = appTools().get_datime_now()
        # FEC_INGRESO = dt_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        FEC_PROCESO = dt_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        print(f'> FEC_PROCESO: {FEC_PROCESO}')
        # print(f'> insert into DM_ADMIN.T_NOMBRE_ARCHIVO: {C1_PROYECTO_v}, {C2_NOMBRE_ARCHIVO_v}, {C3_ESTATUS_v}, {dt_now}')
        # 'insert into DM_ADMIN.T_NOMBRE_ARCHIVO (NMB_PROYECTO, NMB_ARCHIVO, ESTATUS, FEC_INGRESO, FEC_PROCESO) '
        #                  'values ({C1_PROYECTO_v}, {C2_NOMBRE_ARCHIVO_v}, {ESTATUS}, {dt_now}, {dt_now})')

        # Paso 3, 4: V_RSUA_ExisteArchivo -> Pass
        # validacion_existe_archivo = spark_obj.sql('select count(estatus) from T_NOMBRE_ARCHIVO where Estatus = "0" and NMB_PROYECTO="RSUA" AND ROWNUM = 1')

        # Paso 5: V_RSUA_NombreArchivo -> Pass
        # V_RSUA_NombreArchivo = spark_obj.sql('select NMB_ARCHIVO from T_NOMBRE_ARCHIVO where estatus="0" and ROWNUM = 1 and NMB_PROYECTO="RSUA"')

        # Paso 6: V_RSUA_NombreArchivoSalida
        V_RSUA_NombreArchivoSalida = str(self.C2_NOMBRE_ARCHIVO_v).split('.')[0]

        # Paso 7: Transfiere por FTP servidor -> Pass
        # OdiFtpPut "-HOST=172.16.8.218" "-USER=dsstdi_inp" "-PASSWORD=bFypKP4d3.lf91m0jYrUo" "-LOCAL_DIR=F:\DESCARGA_FTP\COBRANZA\RSUA\PROCESAR\" "-LOCAL_FILE=#COBRANZA.V_RSUA_NombreArchivo" "-REMOTE_DIR=\" "-REMOTE_FILE=RSUA.txt" "-PASSIVE_MODE=YES"

        # Paso 8: V_RSUA_FechaCarga
        # select  TO_CHAR(TO_DATE(SUBSTR(resto,15,8),'YYYYMMDD'),'dd/mm/yyyy') TRANS from DM_ADMIN.e_carga_rsua where tp_mov='00'
        try:
            V_RSUA_FechaCarga = DM_ADMIN_E_CARGA_RSUA.filter((col('tp_mov') == '00') & (col('id') == '1')).select(
                col('resto'))
            V_RSUA_FechaCarga = str(V_RSUA_FechaCarga.first()[0])[14:22]
            V_RSUA_FechaCarga = datetime.datetime(int(V_RSUA_FechaCarga[:4]), int(V_RSUA_FechaCarga[4:6]),
                                                  int(V_RSUA_FechaCarga[6:8]), 0, 0, 0, 0)
            # %H:%M:%S.%f
            # V_RSUA_FechaCarga = V_RSUA_FechaCarga.strftime('%Y-%m-%d %H:%M:%S.%f')
            V_RSUA_FechaCarga = V_RSUA_FechaCarga.strftime('%Y-%m-%d')
            print(f'> V_RSUA_FechaCarga= {V_RSUA_FechaCarga}')
        except Exception as e:
            print(e)
            email_content = f"Error en el paso : V_RSUA_FechaCarga. \n\nDescripción:{e}"
            appTools().email_sender(email_content=email_content,
                                    to_email_list=TO_EMAIL_LIST,
                                    subject='ERROR - RSUA')

        # Paso 9: V_RSUA_FechaRespaldo
        # "select  TO_CHAR(TO_DATE(SUBSTR(resto,15,8),'YYYYMMDD'),'yyyymm') TRANS from DM_ADMIN.e_carga_rsua where tp_mov='00'")
        V_RSUA_FechaRespaldo = DM_ADMIN_E_CARGA_RSUA.filter((col('tp_mov') == '00') & (col('id') == '1')).select(
            col('resto'))
        V_RSUA_FechaRespaldo = str(V_RSUA_FechaRespaldo.first()[0])[14:22]
        V_RSUA_FechaRespaldo = datetime.datetime(int(V_RSUA_FechaRespaldo[:4]), int(V_RSUA_FechaRespaldo[4:6]),
                                                 int(V_RSUA_FechaRespaldo[6:8]), 0, 0, 0, 0)
        V_RSUA_FechaRespaldo = V_RSUA_FechaRespaldo.strftime('%Y-%m')
        # print(V_RSUA_FechaRespaldo)

        # Paso 10: V_RSUA_RegistrosCargados
        # V_RSUA_RegistrosCargados = 'select count(1) from SUT_SIPA_00ENCABEZADO_LOTE where fec_transferencia = TO_DATE("V_RSUA_FechaCarga", "dd/mm/yyyy")')
        V_RSUA_RegistrosCargados = 0

        # Paso 11, 12, 13, 14:
        V_RSUA_TipoEjecucion = 1
        V_RSUA_NombreProceso = 20
        V_RSUA_PeriodoEjecucion = 1

        # Paso 15: Proc. Carga temporal T_CARGA_RSUA (DF resultado de paso)
        # RSUA IS SELECT ID AS ID, TP_MOV AS TP_MOV, RESTO AS RESTO FROM DM_ADMIN.e_carga_rsua ORDER BY ID; -> Carga DF

        # Variables usadas en el siguiente algoritmo
        # V_RSUA_CONSDIA VARCHAR2(3)
        # V_RSUA_TRANS DATE
        # V_RSUA_BANCO VARCHAR2(3)
        # V_RSUA_FOLSUA NUMBER
        # V_CONTADOR NUMBER
        # V_ID_PAT NUMBER

        # EXECUTE IMMEDIATE ('truncate table DM_ADMIN.T_CARGA_RSUA'); -> Pass

        # select SUBSTR(RESTO, 23, 3) CONSDIA INTO V_RSUA_CONSDIA from DM_ADMIN.e_carga_rsua where tp_mov='00';
        V_RSUA_CONSDIA = DM_ADMIN_E_CARGA_RSUA.filter((col('tp_mov') == '00') & (col('id') == '1')).select(col('resto'))
        V_RSUA_CONSDIA = str(V_RSUA_CONSDIA.first()[0])[22:25]
        # print(V_RSUA_CONSDIA)

        # select TO_DATE(SUBSTR(resto,15,8),'YYYYMMDD') TRANS INTO V_RSUA_TRANS from DM_ADMIN.e_carga_rsua where tp_mov='00';
        V_RSUA_TRANS = DM_ADMIN_E_CARGA_RSUA.filter((col('tp_mov') == '00') & (col('id') == '1')).select(col('resto'))
        V_RSUA_TRANS = str(V_RSUA_TRANS.first()[0])[14:22]
        V_RSUA_TRANS = datetime.datetime(int(V_RSUA_TRANS[:4]), int(V_RSUA_TRANS[4:6]),
                                         int(V_RSUA_TRANS[6:8]), 0, 0, 0, 0)
        V_RSUA_TRANS = V_RSUA_TRANS.strftime('%Y-%m-%d')
        print(f'> V_RSUA_TRANS= {V_RSUA_TRANS}')

        # Dataframe para algoritmo iterativo
        # DM_ADMIN_T_CARGA_RSUA_columns = ['ID', 'TP_MOV', 'RESTO', 'ID_PAT', 'FOLSUA', 'BANCO', 'FEC_TRANSFERENCIA', 'CONSDIA', 'GRUPO']
        dataframe_struct = {'ID': 'varchar',
                            'TP_MOV': 'varchar',
                            'RESTO': 'varchar',
                            'ID_PAT': 'varchar',
                            'FOLSUA': 'varchar',
                            'BANCO': 'varchar',
                            'FEC_TRANSFERENCIA': 'varchar',
                            'CONSDIA': 'varchar',
                            'GRUPO': 'varchar'}
        DM_ADMIN_T_CARGA_RSUA, DM_ADMIN_T_CARGA_RSUA_SCHEMA = pySparkTools().dataframe_from_struct(spark_obj=spark_obj,
                                                                                                   dataframe_struct=dataframe_struct)
        if DM_ADMIN_T_CARGA_RSUA:
            # Caracteristicas del Dataframe
            DM_ADMIN_T_CARGA_RSUA_LEN_COLS = len(DM_ADMIN_T_CARGA_RSUA.columns)
            print(f'[ok] New Dataframe DM_ADMIN_T_CARGA_RSUA created successfully!. '
                  f'Cols = {DM_ADMIN_T_CARGA_RSUA_LEN_COLS}.')
            DM_ADMIN_T_CARGA_RSUA.printSchema()
        else:
            e = 'New Dataframe DM_ADMIN_T_CARGA_RSUA failed'
            # if falla enviar error:
            # SUBJECT=ERROR - RSUA
            # CONTENIDO: DM_ADMIN_T_CARGA_RSUA. {e}
            # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

            e = 'Failure on dataframe creation: DM_ADMIN_T_CARGA_RSUA'

            email_content = f"Error en el paso : Proc. Carga temporal T_CARGA_RSUA. \n\nDescripción:{e}"
            appTools().email_sender(email_content=email_content,
                                    to_email_list=TO_EMAIL_LIST,
                                    subject='ERROR - RSUA')


            print(f'[error] {e}')
            error_id = '2.2'
            error_description = e
            ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                  start_datetime=self.start_datetime,
                                                  start_datetime_proc=self.start_datetime_proc,
                                                  end_datetime_proc=appTools().get_datime_now(),
                                                  error_id=error_id,
                                                  error_description=error_description,
                                                  process_name=process_name)
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()

        V_ID_PAT = 1
        V_CONTADOR = 0
        rowValues = []
        try:
            if os_running == 'windows':
                DM_ADMIN_E_CARGA_RSUA = DM_ADMIN_E_CARGA_RSUA.limit(112542)
            # DM_ADMIN_E_CARGA_RSUA.show(25, False)
            if SERVER_LIMIT_DATA_LEN:
                DM_ADMIN_E_CARGA_RSUA = DM_ADMIN_E_CARGA_RSUA.limit(SERVER_LIMIT_DATA_LEN)
                print(f'> DM_ADMIN_E_CARGA_RSUA limited: {SERVER_LIMIT_DATA_LEN}rows')

            # iter_dataframe = DM_ADMIN_E_CARGA_RSUA.rdd.toLocalIterator()
            iter_dataframe = DM_ADMIN_E_CARGA_RSUA.collect()
            print(f'[ok] Dataframe \033[95mDM_ADMIN_E_CARGA_RSUA\033[0m ready to iterate')
        except Exception as e:
            print(f'[error] Failure to collect dataframe. {e}')
            sys.exit()

        for r in iter_dataframe:
            if r['tp_mov'] == '00':
                V_CONTADOR = 0
                V_RSUA_FOLSUA = 'Null'
                V_RSUA_BANCO = 'Null'

            elif r['tp_mov'] == '01':
                # V_RSUA_BANCO := SUBSTR(R.RESTO,7,3);
                V_RSUA_BANCO = str(r['resto'])[6:9]
                # V_CONTADOR = V_CONTADOR

            elif r['tp_mov'] == '02':
                # V_RSUA_FOLSUA:=SUBSTR( R.RESTO,31,6);
                V_RSUA_FOLSUA = str(r['resto'])[30:36]
                V_CONTADOR += 1

            ord_row_values = [r['id'], r['tp_mov'], r['resto'], V_ID_PAT, V_RSUA_FOLSUA, V_RSUA_BANCO, V_RSUA_TRANS,
                              V_RSUA_CONSDIA, V_CONTADOR]
            rowValues.append([str(v) for v in ord_row_values])
            V_ID_PAT += 1
        try:
            newRows = spark_obj.createDataFrame(rowValues, DM_ADMIN_T_CARGA_RSUA_SCHEMA)
            DM_ADMIN_T_CARGA_RSUA = DM_ADMIN_T_CARGA_RSUA.union(newRows)

            DM_ADMIN_T_CARGA_RSUA_LEN_ROWS = DM_ADMIN_T_CARGA_RSUA.count()
            DM_ADMIN_T_CARGA_RSUA_LEN_COLS = len(DM_ADMIN_T_CARGA_RSUA.columns)
            print(
                f'[ok] {len(rowValues)} new rows inserted to Dataframe \033[95mDM_ADMIN_T_CARGA_RSUA\033[0m successfully!. '
                f'Rows = {DM_ADMIN_T_CARGA_RSUA_LEN_ROWS}, Cols = {DM_ADMIN_T_CARGA_RSUA_LEN_COLS}.')
            DM_ADMIN_T_CARGA_RSUA.printSchema()
        except Exception as e:
            err = f'Appending Rows data to DM_ADMIN_T_CARGA_RSUA'
            print(f'{err}. {e}')
            # if falla enviar error:
            # SUBJECT=ERROR - RSUA
            # CONTENIDO: DM_ADMIN_T_CARGA_RSUA. {err}
            # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

            email_content = f"Error en el paso : Proc. Carga tablas RSUA. \n\nDescripción:{err}. {e}"
            appTools().email_sender(email_content=email_content,
                                    to_email_list=TO_EMAIL_LIST,
                                    subject='ERROR - RSUA')

        # Paso 16: Proc. Carga tablas RSUA, from dm_admin.t_carga_rsua where TP_MOV = '00'
        # Inserta SUT_SIPA_00ENCABEZADO_LOTE

        RESTO_TEMP = DM_ADMIN_T_CARGA_RSUA.select(col('resto')).filter(col('tp_mov') == '00')

        # TO_DATE(SUBSTR(RESTO,15,8),'YYYYMMDD') AS FEC_TRAN
        FEC_TRAN = RESTO_TEMP.first()[0][14:22]
        FEC_TRAN = datetime.datetime(int(FEC_TRAN[:4]), int(FEC_TRAN[4:6]),
                                     int(FEC_TRAN[6:8])).date()
        FEC_TRAN = FEC_TRAN.strftime('%Y-%m-%d')
        print(f'> FEC_TRAN= {FEC_TRAN}')

        # SUBSTR(RESTO,23,3) AS CONS_DIA,
        CONS_DIA = RESTO_TEMP.first()[0][22:25]
        # print(CONS_DIA)

        # TP_MOV AS TIPO_REG
        TIPO_REG = DM_ADMIN_T_CARGA_RSUA.select(col('tp_mov')).filter(col('tp_mov') == '00').first()[0]
        # print(TIPO_REG)

        # SUBSTR(RESTO,1,2) AS ID_SERV
        ID_SERV = RESTO_TEMP.first()[0][0:2]
        # print(ID_SERV)

        # SUBSTR(RESTO,3,2) AS ID_OPER,
        ID_OPER = RESTO_TEMP.first()[0][2:4]
        # print(ID_OPER)

        # SUBSTR(RESTO,5,2) AS TPO_ENT_ORI,
        TPO_ENT_ORI = RESTO_TEMP.first()[0][4:6]
        # print(TPO_ENT_ORI)

        # SUBSTR(RESTO,7,3) AS CVE_ENT_ORI,
        CVE_ENT_ORI = RESTO_TEMP.first()[0][6:9]
        # print(CVE_ENT_ORI)

        # SUBSTR(RESTO,10,2) AS TPO_ENT_DES,
        TPO_ENT_DES = RESTO_TEMP.first()[0][9:11]
        # print(TPO_ENT_DES)

        # SUBSTR(RESTO,12,3) AS CVE_ENT_DES,
        CVE_ENT_DES = RESTO_TEMP.first()[0][11:14]
        # print(CVE_ENT_DES)

        # SUBSTR(RESTO,26,2) AS MOD_REC_ENV,
        MOD_REC_ENV = RESTO_TEMP.first()[0][25:27]
        # print(MOD_REC_ENV)

        # SUBSTR(RESTO,338,2) AS RES_OPER,
        RES_OPER = RESTO_TEMP.first()[0][337:339]
        # print(RES_OPER)

        # SUBSTR(RESTO,340,3) AS DIAG_1,
        DIAG_1 = RESTO_TEMP.first()[0][339:342]
        # print(DIAG_1)

        # SUBSTR(RESTO,343,3) AS DIAG_2,
        DIAG_2 = RESTO_TEMP.first()[0][342:345]
        # print(DIAG_2)

        # SUBSTR(RESTO,346,3) AS DIAG_3
        DIAG_3 = RESTO_TEMP.first()[0][345:348]
        # print(DIAG_3)

        # INSERT INTO SUT_SIPA_00ENCABEZADO_LOTE -> Hive
        """
        fec_transferencia
        num_consecutivo_dia
        cve_tipo_registro
        cve_identif_servicio
        cve_identif_operacion
        cve_tipo_entidad_origen
        cve_entidad_origen
        cve_tipo_entidad_destino
        cve_entidad_destino
        cve_modalidad_recepcion
        cve_resultado_operacion
        cve_diagnostico_1
        cve_diagnostico_2
        cve_diagnostico_3
        fecha_carga
        fecha_proceso
        """
        table_name = 'te_aficobranza.sut_sipa_00encabezado_lote'
        print(f'V_RSUA_FechaCarga sut_sipa_00encabezado_lote: {V_RSUA_FechaCarga}')
        insert_data_query = f"insert into {table_name} values ('{FEC_TRAN}', " \
                            f"'{CONS_DIA}', '{TIPO_REG}', '{ID_SERV}', '{ID_OPER}', '{TPO_ENT_ORI}', " \
                            f"'{CVE_ENT_ORI}', '{TPO_ENT_DES}', '{CVE_ENT_DES}', '{MOD_REC_ENV}', '{RES_OPER}', " \
                            f"'{DIAG_1}', '{DIAG_2}', '{DIAG_3}', '{V_RSUA_FechaCarga}', '{FEC_PROCESO}')"
        try:
            print(f'> Query to Hive: {insert_data_query}')
            status_query_hive = Hive().exec_query(insert_data_query)
            if not status_query_hive:
                error_id = '3.1'
                error_description = f'Hive query: {insert_data_query}'
            else:
                error_id = None
        except Exception as e:
            error_id, error_description = appTools().get_error(error=e)
        finally:
            if error_id:
                print('> Sending [error] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)
            else:
                print('> Sending [ok] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id='0',
                                                      error_description='',
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)

        print('> SUT_SIPA_00ENCABEZADO_LOTE_DF')
        SUT_SIPA_00ENCABEZADO_LOTE_TEMP = [{"Cve_Tipo_Registro": TIPO_REG, "FEC_TRANSFERENCIA": FEC_TRAN}]
        SUT_SIPA_00ENCABEZADO_LOTE_DF = spark_obj.createDataFrame(SUT_SIPA_00ENCABEZADO_LOTE_TEMP)
        RowsTemp = SUT_SIPA_00ENCABEZADO_LOTE_DF.count()
        # print(RowsTemp)
        SUT_SIPA_00ENCABEZADO_LOTE_DF = SUT_SIPA_00ENCABEZADO_LOTE_DF.withColumn('TOTAL_DE_REGISTROS',
                                                                                 when(lit(True), lit(RowsTemp)))
        SUT_SIPA_00ENCABEZADO_LOTE_DF.printSchema()

        # Paso 16: Proc. Carga tablas RSUA
        # insert into SUT_SIPA_01ENCABEZADO_ENT from DM_ADMIN.T_carga_rsua where TP_MOV = '01'

        RESTO_TEMP = DM_ADMIN_T_CARGA_RSUA.select(col('resto')).filter(col('tp_mov') == '01')

        # SUBSTR(RESTO,7,3) AS CVE_ENT_ORI
        CVE_ENT_ORI = RESTO_TEMP.first()[0][6:9]

        # TO_DATE(SUBSTR(RESTO,15,8),'YYYYMMDD') AS FEC_TRAN
        FEC_TRAN = RESTO_TEMP.first()[0][14:22]
        FEC_TRAN = datetime.datetime(int(FEC_TRAN[:4]), int(FEC_TRAN[4:6]),
                                     int(FEC_TRAN[6:8])).date()
        FEC_TRAN = FEC_TRAN.strftime('%Y-%m-%d')

        # SUBSTR(RESTO,23,3) AS CONS_DIA
        CONS_DIA = RESTO_TEMP.first()[0][22:25]

        # TP_MOV AS TIPO_REG
        TIPO_REG = DM_ADMIN_T_CARGA_RSUA.select(col('tp_mov')).filter(col('tp_mov') == '01').first()[0]

        # SUBSTR(RESTO,1,2) AS ID_SERV
        ID_SERV = RESTO_TEMP.first()[0][0:2]

        # SUBSTR(RESTO,3,2) AS ID_OPER
        ID_OPER = RESTO_TEMP.first()[0][2:4]

        # SUBSTR(RESTO,5,2) AS TPO_ENT_ORI
        TPO_ENT_ORI = RESTO_TEMP.first()[0][2:4]

        # SUBSTR(RESTO,10,2) AS TPO_ENT_DES,
        TPO_ENT_DES = RESTO_TEMP.first()[0][9:11]
        # print(TPO_ENT_DES)

        # SUBSTR(RESTO,12,3) AS CVE_ENT_DES,
        CVE_ENT_DES = RESTO_TEMP.first()[0][11:14]
        # print(CVE_ENT_DES)

        # SUBSTR(RESTO,26,2) AS MOD_REC_ENV,
        MOD_REC_ENV = RESTO_TEMP.first()[0][25:27]
        # print(MOD_REC_ENV)

        # SUBSTR(RESTO,28,3) AS VER_SUA
        VER_SUA = RESTO_TEMP.first()[0][27:30]

        # SUBSTR(RESTO,338,2) AS RES_OPER,
        RES_OPER = RESTO_TEMP.first()[0][337:339]

        # SUBSTR(RESTO,340,3) AS DIAG_1,
        DIAG_1 = RESTO_TEMP.first()[0][339:342]

        # SUBSTR(RESTO,343,3) AS DIAG_2,
        DIAG_2 = RESTO_TEMP.first()[0][342:345]

        # SUBSTR(RESTO,346,3) AS DIAG_3
        DIAG_3 = RESTO_TEMP.first()[0][345:348]

        # insert into SUT_SIPA_01ENCABEZADO_ENT -> Hive
        """
        cve_entidad_origen
        fec_transferencia
        num_consecutivo_dia
        cve_tipo_registro
        cve_identif_servicio
        cve_identif_operacion
        cve_tipo_entidad_origen
        cve_tipo_entidad_destino
        cve_entidad_destino
        cve_modalidad_recepcion
        cve_identif_version_pagos_pat
        cve_resultado_operacion
        cve_diagnostico_1
        cve_diagnostico_2
        cve_diagnostico_3
        fecha_carga
        fecha_proceso
        """
        table_name = 'te_aficobranza.sut_sipa_01encabezado_ent'
        print(f'V_RSUA_FechaCarga sut_sipa_01encabezado_ent: {V_RSUA_FechaCarga}')
        insert_data_query = f"insert into {table_name} values ('{CVE_ENT_ORI}', " \
                            f"'{FEC_TRAN}', '{CONS_DIA}', '{TIPO_REG}', '{ID_SERV}', '{ID_OPER}', '{TPO_ENT_ORI}', " \
                            f"'{TPO_ENT_DES}', '{CVE_ENT_DES}', '{MOD_REC_ENV}', '{VER_SUA}', '{RES_OPER}', " \
                            f"'{DIAG_1}', '{DIAG_2}', '{DIAG_3}', '{V_RSUA_FechaCarga}', '{FEC_PROCESO}')"
        try:
            print(f'> Query to Hive: {insert_data_query}')
            status_query_hive = Hive().exec_query(insert_data_query)
            if not status_query_hive:
                error_id = '3.1'
                error_description = f'Hive query: {insert_data_query}'
            else:
                error_id = None
        except Exception as e:
            error_id, error_description = appTools().get_error(error=e)
        finally:
            if error_id:
                print('> Sending [error] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)
            else:
                print('> Sending [ok] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id='0',
                                                      error_description='',
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)

        # SUT_SIPA_01ENCABEZADO_ENT_DF
        print('> SUT_SIPA_01ENCABEZADO_ENT_DF')
        SUT_SIPA_01ENCABEZADO_ENT_TEMP = [{"Cve_Tipo_Registro": TIPO_REG, "FEC_TRANSFERENCIA": FEC_TRAN}]
        SUT_SIPA_01ENCABEZADO_ENT_DF = spark_obj.createDataFrame(SUT_SIPA_01ENCABEZADO_ENT_TEMP)
        RowsTemp = SUT_SIPA_01ENCABEZADO_ENT_DF.count()
        # print(RowsTemp)
        SUT_SIPA_01ENCABEZADO_ENT_DF = SUT_SIPA_01ENCABEZADO_ENT_DF.withColumn('TOTAL_DE_REGISTROS',
                                                                               when(lit(True), lit(RowsTemp)))
        SUT_SIPA_01ENCABEZADO_ENT_DF.printSchema()

        print('> B78_DF')
        # B78_DF from dm_admin.T_carga_rsua where TP_MOV = '07' OR TP_MOV = '08')
        B78_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'), col('banco'),
                                              col('consdia'), col('fec_transferencia')) \
            .filter((col('tp_mov') == '07') | (col('tp_mov') == '08'))
        # B78_DF = B78_DF.withColumnRenamed('id', 'id_b78')

        """
        ok TP_MOV AS TIPO_REG
        ok SUBSTR(RESTO,1,2) AS ID_SERV
        ok SUBSTR(RESTO,3,8) AS PLA_SUC
        ok SUBSTR(RESTO,11,11)) AS REG_PATRON,
        ok SUBSTR(RESTO,22,1) AS INF_PAT,
        ok SUBSTR(RESTO,23,6) AS FOL_SUA
        ok SUBSTR(RESTO,29,6) AS PER_PAGO
        ok SUBSTR(RESTO,35,8) AS FEC_PAGO
        ok SUBSTR(RESTO,43,8) AS FEC_VAL_RCV,
        ok SUBSTR(RESTO,51,8) AS FEC_VAL_IMSS
        SUBSTR(RESTO,59,12)) AS IMP_IMSS
        SUBSTR(RESTO,71,12)) AS IMP_RCV
        SUBSTR(RESTO,83,12)) AS IMP_APO_PAT
        SUBSTR(RESTO,95,12)) AS IMP_AMOR_INFO
        ok SUBSTR(RESTO,107,231) AS FILLER1
        ok SUBSTR(RESTO,338,2) AS RES_OPER
        ok SUBSTR(RESTO,340,3) AS DIAG_1
        ok SUBSTR (RESTO,343,3) AS DIAG_2
        ok SUBSTR(RESTO,346,3) AS DIAG_3
        ok GRUPO AS GRUPO
        ok FEC_TRANSFERENCIA AS FEC_TRAN
        ok BANCO AS BANCO
        ok CONSDIA AS CONS_DIA
        """
        print('> B78_DF fase 1')
        B78_DF = B78_DF.withColumn('TIPO_REG_B78', col('tp_mov')) \
            .withColumn('ID_SERV', substring(col('resto'), 1, 2)) \
            .withColumn('PLA_SUC', substring(col('resto'), 3, 8)) \
            .withColumn('REG_PATRON_B78', substring(col('resto'), 11, 11)) \
            .withColumn('INF_PAT', substring(col('resto'), 22, 1)) \
            .withColumn('FOL_SUA_B78', substring(col('resto'), 23, 6)) \
            .withColumn('PER_PAGO_B78', substring(col('resto'), 29, 6)) \
            .withColumn('FEC_PAGO_B78', substring(col('resto'), 35, 8))
        B78_DF = B78_DF.withColumn('FEC_VAL_RCV', substring(col('resto'), 43, 8)) \
            .withColumn('FEC_VAL_IMSS', substring(col('resto'), 51, 8)) \
            .withColumn('IMP_IMSS', substring(col('resto'), 59, 12)) \
            .withColumn('IMP_RCV', substring(col('resto'), 71, 12)) \
            .withColumn('IMP_APO_PAT', substring(col('resto'), 83, 12)) \
            .withColumn('IMP_AMOR_INFO', substring(col('resto'), 95, 12)) \
            .withColumn('FILLER1', substring(col('resto'), 107, 231)) \
            .withColumn('RES_OPER_B78', substring(col('resto'), 338, 2))
        B78_DF = B78_DF.withColumn('DIAG_1_B78', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2_B78', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3_B78', substring(col('resto'), 346, 3)).withColumn('GRUPO_B78', col('grupo')) \
            .withColumn('FEC_TRAN_B78', col('FEC_TRANSFERENCIA')) \
            .withColumn('BANCO_B78', col('banco')) \
            .withColumn('CONS_DIA_B78', col('consdia'))

        """
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12)) AS IMP_IMSS
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12)) AS IMP_RCV
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12)) AS IMP_APO_PAT
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12)) AS IMP_AMOR_INFO
        FALTA dm_admin.LIMPIACHAR (SUBSTR(RESTO,11,11)) AS REG_PATRON,
        """
        print('> B78_DF fase 2')
        B78_DF = B78_DF.withColumn('IMP_IMSS', get_number(col('IMP_IMSS'))) \
            .withColumn('IMP_RCV', get_number(col('IMP_RCV'))) \
            .withColumn('IMP_APO_PAT', get_number(col('IMP_APO_PAT'))) \
            .withColumn('IMP_AMOR_INFO', get_number(col('IMP_AMOR_INFO'))) \
            .withColumn('REG_PATRON_B78', limpia_char(col('REG_PATRON_B78')))

        """
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT
        FALTA dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO
        """
        print('> B78_DF fase 3')
        B78_DF = B78_DF.withColumn('IMP_IMSS', div_100_func(col('IMP_IMSS'))) \
            .withColumn('IMP_RCV', div_100_func(col('IMP_RCV'))) \
            .withColumn('IMP_APO_PAT', div_100_func(col('IMP_APO_PAT'))) \
            .withColumn('IMP_AMOR_INFO', div_100_func(col('IMP_AMOR_INFO')))

        drop_cols = ['resto', 'tp_mov', 'grupo', 'fec_transferencia', 'consdia', 'banco']
        print(drop_cols)
        B78_DF = B78_DF.drop(*drop_cols)
        B78_DF.printSchema()
        # B78_DF.show(20, False)
        # Hasta agui el desarrollo Base

        # Paso 16: Proc. Carga tablas RSUA
        # INSERT  INTO SUT_SIPA_02ENCABEZADO_PATRON -> Hive

        # from dm_admin.T_carga_rsua where TP_MOV = '02') A2_DF
        A2_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                             col('fec_transferencia'), col('banco'),
                                             col('consdia')).filter(col('tp_mov') == '02')
        # A2_DF = A2_DF.withColumnRenamed('id', 'id_a2')

        """
        Fase 1 de transformacion A2_DF

        TP_MOV as TIPO_REG
        (SUBSTR(RESTO,1,11)) AS REG_PATRON
        (SUBSTR(RESTO,12,13)) AS RFC_PAT
        SUBSTR(RESTO,25,6) AS PER_PAGO
        SUBSTR(RESTO,31,6) AS FOL_SUA
        (SUBSTR(RESTO,37,50)) AS NOM_PAT
        (SUBSTR(RESTO,87,40)) AS DOM_PAT
        (SUBSTR(RESTO,127,40)) AS MUN
        SUBSTR(RESTO,167,2) AS ENT_FED
        
        (SUBSTR(RESTO,169,5)) AS COD_POS
        ok (SUBSTR(RESTO,174,15)) AS TEL
        ok SUBSTR(RESTO,189,7) AS PRI_RT
        SUBSTR(RESTO,196,6) AS FEC_PRI_RT
        (SUBSTR(RESTO,202,40)) AS ACT_ECO
        ok (SUBSTR(RESTO,242,2) AS DEL_IMSS
        ok SUBSTR(RESTO,244,2)) AS SUB_IMSS
        ok SUBSTR(RESTO,246,1) AS AREA_GEO
        ok SUBSTR(RESTO,247,2)) AS PTJ_APOR_INFO
        ok SUBSTR(RESTO,249,1) AS REE_SUBS
        SUBSTR(RESTO,250,1) AS TIPO_COTIZ
        ok SUBSTR(RESTO,251,7) AS TOT_DIAS_COTIZ
        ok SUBSTR(RESTO,258,9) AS NUM_TRA_COTIZ
        ok SUBSTR(RESTO,267,2) AS TIPO_DOC
        ok SUBSTR(RESTO,269,9) AS NUMERO_CREDITO
        ok SUBSTR(RESTO,338,2) AS RES_OPER
        ok SUBSTR(RESTO,340,3) AS DIAG_1
        ok SUBSTR(RESTO,343,3) AS DIAG_2
        ok SUBSTR(RESTO,346,3) AS DIAG_3
        ok FEC_TRANSFERENCIA AS FEC_TRAN
        ok BANCO AS BANCO
        ok CONSDIA AS CONS_DIA
        ok SUBSTR(RESTO,1,11) AS CVE_PATRON
        ok SUBSTR(RESTO,1,11) AS CVE_MODALIDAD
        ok SUBSTR(RESTO,1,11) AS DIG_VERIFICADOR
        ok GRUPO AS GRUPO
        ok 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
        """
        print('> A2_DF fase 1')
        A2_DF = A2_DF.withColumn('TIPO_REG', col('tp_mov')).withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('RFC_PAT', substring(col('resto'), 12, 13)) \
            .withColumn('PER_PAGO', substring(col('resto'), 25, 6)) \
            .withColumn('FOL_SUA', substring(col('resto'), 31, 6)).withColumn('NOM_PAT',
                                                                              substring(col('resto'), 37, 50)) \
            .withColumn('DOM_PAT', substring(col('resto'), 87, 40)).withColumn('MUN',
                                                                               substring(col('resto'), 127, 40)) \
            .withColumn('ENT_FED', substring(col('resto'), 167, 2))
        A2_DF = A2_DF.withColumn('COD_POS', substring(col('resto'), 169, 5)) \
            .withColumn('TEL', substring(col('resto'), 174, 15)) \
            .withColumn('PRI_RT', substring(col('resto'), 189, 7)) \
            .withColumn('FEC_PRI_RT', substring(col('resto'), 196, 6)) \
            .withColumn('ACT_ECO', substring(col('resto'), 202, 40)) \
            .withColumn('DEL_IMSS', substring(col('resto'), 242, 2))
        A2_DF = A2_DF.withColumn('SUB_IMSS', substring(col('resto'), 244, 2)) \
            .withColumn('AREA_GEO', substring(col('resto'), 246, 1)) \
            .withColumn('PTJ_APOR_INFO', substring(col('resto'), 247, 2)) \
            .withColumn('REE_SUBS', substring(col('resto'), 249, 1)) \
            .withColumn('TIPO_COTIZ', substring(col('resto'), 250, 1)) \
            .withColumn('TOT_DIAS_COTIZ', substring(col('resto'), 251, 7)) \
            .withColumn('NUM_TRA_COTIZ', substring(col('resto'), 258, 9)) \
            .withColumn('TIPO_DOC', substring(col('resto'), 267, 2)) \
            .withColumn('NUMERO_CREDITO', substring(col('resto'), 269, 9)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2))
        A2_DF = A2_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        A2_DF = A2_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        A2_DF = A2_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))
        A2_DF = A2_DF.withColumn('FEC_TRAN', col('fec_transferencia')).withColumn('CONS_DIA', col('consdia'))
        A2_DF = A2_DF.withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))
        A2_DF = A2_DF.withColumn('GRUPO', col('grupo')) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        """
        Fase 2 de transformacion A2_DF

        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,12,13)) AS RFC_PAT
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,37,50)) AS NOM_PAT
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,87,40)) AS DOM_PAT
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,127,40)) AS MUN
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,169,5)) AS COD_POS
        ok dm_admin.LIMPIACHAR (SUBSTR(RESTO,174,15)) AS TEL
        dm_admin.LIMPIACHAR (SUBSTR(RESTO,202,40)) AS ACT_ECO

        ok DECODE(SUBSTR(RESTO,242,2), 0, 99, SUBSTR(RESTO,242,2)) AS DEL_IMSS
        ok DECODE(SUBSTR(RESTO,244,2), 0, 99, SUBSTR(RESTO,244,2)) AS SUB_IMSS

        ok dm_admin.GET_NUMBER(SUBSTR(RESTO,247,2)) AS PTJ_APOR_INFO
        ok dm_admin.GET_NUMBER(SUBSTR(RESTO,189,7)) AS PRI_RT
        
        SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
        ok SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD
        ok SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR
        """
        print('> A2_DF fase 2')
        A2_DF = A2_DF.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('RFC_PAT', limpia_char(col('RFC_PAT'))) \
            .withColumn('NOM_PAT', limpia_char(col('NOM_PAT'))) \
            .withColumn('DOM_PAT', limpia_char(col('DOM_PAT'))) \
            .withColumn('MUN', limpia_char(col('MUN'))) \
            .withColumn('COD_POS', limpia_char(col('COD_POS'))).withColumn('TEL', limpia_char(col('TEL'))) \
            .withColumn('ACT_ECO', limpia_char(col('ACT_ECO')))
        A2_DF = A2_DF.withColumn('DEL_IMSS', decode_99_func(col('DEL_IMSS'))) \
            .withColumn('SUB_IMSS', decode_99_func(col('SUB_IMSS')))
        A2_DF = A2_DF.withColumn('PTJ_APOR_INFO', get_number(col('PTJ_APOR_INFO'))) \
            .withColumn('PRI_RT', get_number(col('PRI_RT')))
        A2_DF = A2_DF.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        """
        Fase 3 de transformacion A2_DF

        ok dm_admin.GET_NUMBER(SUBSTR(RESTO,247,2))/10 AS PTJ_APOR_INFO
        ok dm_admin.GET_NUMBER(SUBSTR(RESTO,189,7))/100000 AS PRI_RT
        """
        print('> A2_DF fase 3')
        A2_DF = A2_DF.withColumn('PTJ_APOR_INFO', div_10_func(col('PTJ_APOR_INFO'))) \
            .withColumn('PRI_RT', div_100000_func(col('PRI_RT')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia']
        print(drop_cols)
        A2_DF = A2_DF.drop(*drop_cols)
        A2_DF.printSchema()
        # A2_DF.show(20, False)

        """
        where A2_DF.REG_PATRON = B78_DF.REG_PATRON
        AND TO_NUMBER(A2_DF.FOL_SUA) = TO_NUMBER(B78_DF.FOL_SUA)
        AND A2_DF.FEC_TRAN = B78_DF.FEC_TRAN
        AND A2_DF.BANCO = B78_DF.BANCO
        AND A2_DF.CONS_DIA = B78_DF.CONS_DIA
        AND A2_DF.GRUPO = B78_DF.GRUPO
        """
        print('> A2_B78_JOIN')
        A2_B78_JOIN = A2_DF.join(B78_DF, (A2_DF['REG_PATRON'] == B78_DF['REG_PATRON_B78']) &
                                 (A2_DF['FEC_TRAN'] == B78_DF['FEC_TRAN_B78']) &
                                 (A2_DF['BANCO'] == B78_DF['BANCO_B78']) &
                                 (A2_DF['CONS_DIA'] == B78_DF['CONS_DIA_B78']) &
                                 (get_number(A2_DF['FOL_SUA']) == get_number(B78_DF['FOL_SUA_B78'])) &
                                 (get_number(A2_DF['GRUPO']) == get_number(B78_DF['GRUPO_B78'])))
        A2_B78_JOIN.printSchema()
        # A2_B78_JOIN.show(20, False)

        """
        select   /*+ PARALLEL (A2_DF,10)*/
        A2_DF.REG_PATRON
        A2_DF.PER_PAGO
        ok A2_DF.FOL_SUA
        A2_DF.FEC_TRAN
        # A2_DF.BANCO as CVE_ENTIDAD_ORIGEN
        ok A2_DF.CONS_DIA
        A2_DF.TIPO_REG
        A2_DF.RFC_PAT
        ok A2_DF.NOM_PAT
        ok A2_DF.DOM_PAT
        ok A2_DF.MUN
        A2_DF.ENT_FED
        A2_DF.COD_POS
        A2_DF.TEL
        ok A2_DF.PRI_RT
        A2_DF.FEC_PRI_RT
        A2_DF.ACT_ECO
        ok A2_DF.DEL_IMSS
        A2_DF.SUB_IMSS
        A2_DF.AREA_GEO
        TO_DATE(B78_DF.FEC_PAGO,'YYYYMMDD')
        ok A2_DF.PTJ_APOR_INFO
        A2_DF.REE_SUBS
        A2_DF.TIPO_COTIZ
        ok A2_DF.TOT_DIAS_COTIZ
        A2_DF.NUM_TRA_COTIZ
        A2_DF.TIPO_DOC
        ok A2_DF.NUMERO_CREDITO
        A2_DF.RES_OPER
        A2_DF.DIAG_1
        A2_DF.DIAG_2
        ok A2_DF.DIAG_3

        A2_DF.CVE_PATRON
        A2_DF.CVE_MODALIDAD
        A2_DF.DIG_VERIFICADOR
        """
        print('> A2_10_DF')
        A2_10_DF = A2_B78_JOIN.select(col('REG_PATRON'), col('PER_PAGO'),
                                      col('FOL_SUA'), col('FEC_TRAN'), col('banco').alias('CVE_ENTIDAD_ORIGEN'),
                                      col('CONS_DIA'), col('TIPO_REG'), col('RFC_PAT'),
                                      col('NOM_PAT'), col('DOM_PAT'), col('AREA_GEO'),
                                      col('FEC_PAGO_B78').alias('FEC_PAGO'),
                                      col('MUN'), col('ENT_FED'), col('COD_POS'), col('TEL'), col('PRI_RT'),
                                      col('FEC_PRI_RT'), col('ACT_ECO'), col('DEL_IMSS'), col('SUB_IMSS'),
                                      col('PTJ_APOR_INFO'), col('REE_SUBS'), col('TIPO_COTIZ'),
                                      col('TOT_DIAS_COTIZ'),
                                      col('NUM_TRA_COTIZ'), col('TIPO_DOC'), col('NUMERO_CREDITO'), col('RES_OPER'),
                                      col('CVE_PATRON'), col('CVE_MODALIDAD'), col('DIG_VERIFICADOR'),
                                      col('DIAG_1'), col('DIAG_2'), col('DIAG_3'))

        # trim(A2_DF.ENT_FED)
        A2_10_DF = A2_10_DF.withColumn('ENT_FED', trim_func(col('ENT_FED')))

        # 1 as TIPO_ORIG
        A2_10_DF = A2_10_DF.withColumn('TIPO_ORIG', when(lit(True), lit('1')))

        A2_10_DF.printSchema()
        # A2_10_DF.show(20, False)

        # insert into SUT_SIPA_02ENCABEZADO_PATRON -> Hive
        """
        cve_reg_patronal
        num_periodo_pago
        num_folio_sua
        fec_transferencia
        cve_entidad_origen
        num_consecutivo_dia
        cve_tipo_registro
        rfc_patron
        des_razon_social
        dom_calle_colonia
        dom_poblacion_mpio_delegacion
        cve_entidad_federativa
        cve_codigo_postal
        dom_telefono
        imp_prima_riesgo_trabajo
        fec_prima_riesgo_trabajo
        des_actividad_economica
        cve_delegacion
        cve_subdelegacion
        cve_area_geografica_cnsm
        por_aport_infonavit
        ind_conv_reembolso_subsidios
        cve_tipo_cotizacion
        num_tdias_cot_per_sin_incidenc
        num_trabajadores_cotizantes
        cve_tipo_documento
        num_credito
        cve_resultado_operacion
        cve_diagnostico_1
        cve_diagnostico_2
        cve_diagnostico_3
        fec_pago
        cve_patron
        cve_modalidad
        dig_verificador
        tipo_orig
        fecha_carga
        fecha_proceso
        """
        # Espacio en la direccion:
        A2_10_DF_TO_HIVE = A2_10_DF.select(col('REG_PATRON'), col('PER_PAGO'), col('FOL_SUA'), col('FEC_TRAN'),
                                           col('CVE_ENTIDAD_ORIGEN'), col('CONS_DIA'), col('TIPO_REG'),
                                           col('RFC_PAT'),
                                           col('NOM_PAT'), col('DOM_PAT'), col('MUN'), col('ENT_FED'),
                                           col('COD_POS'), col('TEL'), col('PRI_RT'), col('FEC_PRI_RT'),
                                           col('ACT_ECO'), col('DEL_IMSS'), col('SUB_IMSS'), col('AREA_GEO'),
                                           col('PTJ_APOR_INFO'), col('REE_SUBS'), col('TIPO_COTIZ'),
                                           col('TOT_DIAS_COTIZ'), col('NUM_TRA_COTIZ'),
                                           col('TIPO_DOC'), col('NUMERO_CREDITO'), col('RES_OPER'), col('DIAG_1'),
                                           col('DIAG_2'), col('DIAG_3'), col('FEC_PAGO'), col('CVE_PATRON'),
                                           col('CVE_MODALIDAD'), col('DIG_VERIFICADOR'), col('TIPO_ORIG'))
        A2_10_DF_TO_HIVE = A2_10_DF_TO_HIVE.withColumn('fecha_carga', when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A2_10_DF_TO_HIVE = A2_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))

        new_colsNames = [
            'cve_reg_patronal',
            'num_periodo_pago',
            'num_folio_sua',
            'fec_transferencia',
            'cve_entidad_origen',
            'num_consecutivo_dia',
            'cve_tipo_registro',
            'rfc_patron',
            'des_razon_social',
            'dom_calle_colonia',
            'dom_poblacion_mpio_delegacion',
            'cve_entidad_federativa',
            'cve_codigo_postal',
            'dom_telefono',
            'imp_prima_riesgo_trabajo',
            'fec_prima_riesgo_trabajo',
            'des_actividad_economica',
            'cve_delegacion',
            'cve_subdelegacion',
            'cve_area_geografica_cnsm',
            'por_aport_infonavit',
            'ind_conv_reembolso_subsidios',
            'cve_tipo_cotizacion',
            'num_tdias_cot_per_sin_incidenc',
            'num_trabajadores_cotizantes',
            'cve_tipo_documento',
            'num_credito',
            'cve_resultado_operacion',
            'cve_diagnostico_1',
            'cve_diagnostico_2',
            'cve_diagnostico_3',
            'fec_pago',
            'cve_patron',
            'cve_modalidad',
            'dig_verificador',
            'tipo_orig',
            'fecha_carga',
            'fecha_proceso']
        A2_10_DF_TO_HIVE = pySparkTools().rename_col(dataframe=A2_10_DF_TO_HIVE, new_colsNames=new_colsNames)

        pySparkTools().describe_dataframe(dataframe=A2_10_DF_TO_HIVE,
                                          dataframe_name='A2_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        path_file_sut_sipa_02encabezado_patron = pySparkTools().dataframe_to_te(
            start_datetime_params=[self.start_datetime, self.start_datetime_proc],
            file_name=self.C2_NOMBRE_ARCHIVO_v,
            dataframe=A2_10_DF_TO_HIVE,
            db_table_lt='lt_aficobranza.sut_sipa_02encabezado_patron')

        A2_DF.unpersist()
        A2_B78_JOIN.unpersist()
        # A2_10_DF.unpersist()
        A2_10_DF_TO_HIVE.unpersist()

        # Paso 16: Proc. Carga tablas RSUA
        # Inserta SUT_SIPA_03DETALLE_TRABAJADOR
        """
        insert into SUT_SIPA_03DETALLE_TRABAJADOR
        SELECT /*+ PARALLEL (A3,10)*/  A3.CVE_NSS, A3.REG_PATRON, A3.PERIODO_PAGO, A3.FEC_TRAN, A3.BANCO AS CVE_ENTIDAD_ORIGEN, A3.FOL_SUA, A3.CONS_DIA, A3.TIPO_REG, A3.RFC_PAT, A3.RFC_TRAB, A3.CURP, A3.NUM_CRED_INFONAVIT
                    , TO_DATE (A3.FEC_INI_DESC_INFO,'YYYYMMDD'), A3.NUM_MOVTOS_PERIODO, A3.NOMBRE, A3.SAL_DIA_INT, A3.TIPO_TRABAJADOR, A3.JOR_SEM_RED, A3.DIAS_COTIZADOS_MES, A3.DIAS_INCAP_MES, A3.DIAS_AUSENT_MES, A3.IMP_EYM_FIJA
                    , A3.IMP_EYM_EXCE, A3.IMP_EYM_DIN, A3.IMP_EYM_PEN, A3.IMP_RT, A3.IMP_IV, A3.IMP_GUAR, A3.IMP_ACTUALIZACION, A3.DIAS_COTIZADOS_BIM, A3.DIAS_INCAP_BIM, A3.DIAS_AUSENT_BIM, A3.TOT_RET
                    , A3.TOT_ACT_REC_RET, A3.TOT_CV_PAT, A3.TOT_CV_TRAB, A3.TOT_ACT_REC_CV, A3.APORT_VOL, A3.APORT_PAT_INFO, A3.AMORT_CRED_INFO, A3.IMP_OBR_EYM_EXCE, A3.IMP_OBR_EYM_DIN, A3.IMP_OBR_EYM_PEN
                    , A3.IMP_OBR_IV, A3.IMP_OBR_CV, A3.IMP_OBR_MUN,A3.DEST_APO_VIV, A3.TOT_APOPAT_INF_EMI, A3.TOT_AMOR_CRE_INF, A3.RES_OPER, A3.DIAG_CONFRONTA, A3.DIAG_1, A3.DIAG_2, A3.DIAG_3, TO_DATE (B78.FEC_PAGO,'YYYYMMDD')
                    , A3.CVE_PATRON, A3.CVE_MODALIDAD, A3.DIG_VERIFICADOR, 1 as TIPO_ORIG                                     
        FROM ( select TP_MOV as TIPO_REG, SUBSTR(RESTO,1,11) AS REG_PATRON, SUBSTR(RESTO,12,13) AS RFC_PAT, SUBSTR(RESTO,25,6) AS PERIODO_PAGO, SUBSTR(RESTO,31,11) AS CVE_NSS
                                      , dm_admin.LIMPIACHAR (SUBSTR(RESTO,42,13)) AS RFC_TRAB, dm_admin.LIMPIACHAR (SUBSTR(RESTO,55,18)) AS CURP, DECODE(SUBSTR(RESTO,73,10),'??????????','0000000000',SUBSTR(RESTO,73,10)) AS NUM_CRED_INFONAVIT
                                      , (case when is_date(SUBSTR(RESTO,83,8))='Y' then SUBSTR(RESTO,83,8) else '19000101' end) AS FEC_INI_DESC_INFO, SUBSTR(RESTO,91,2)  AS NUM_MOVTOS_PERIODO
                                      , dm_admin.LIMPIACHAR (SUBSTR(RESTO,93,50)) AS NOMBRE, dm_admin.GET_NUMBER (SUBSTR(RESTO,143,7))/100 AS SAL_DIA_INT, SUBSTR(RESTO,150,1) AS TIPO_TRABAJADOR, SUBSTR(RESTO,151,1) AS JOR_SEM_RED
                                      , SUBSTR(RESTO,152,2) AS DIAS_COTIZADOS_MES, SUBSTR(RESTO,154,2) AS DIAS_INCAP_MES, SUBSTR(RESTO,156,2) AS DIAS_AUSENT_MES, dm_admin.GET_NUMBER (SUBSTR(RESTO,158,7))/100 AS IMP_EYM_FIJA
                                      , dm_admin.GET_NUMBER (SUBSTR(RESTO,165,7))/100 AS IMP_EYM_EXCE, dm_admin.GET_NUMBER (SUBSTR(RESTO,172,7))/100 AS IMP_EYM_DIN, dm_admin.GET_NUMBER (SUBSTR(RESTO,179,7))/100 AS IMP_EYM_PEN, dm_admin.GET_NUMBER (SUBSTR(RESTO,186,7))/100 AS IMP_RT
                                      , dm_admin.GET_NUMBER (SUBSTR(RESTO,193,7))/100 AS IMP_IV, dm_admin.GET_NUMBER (SUBSTR(RESTO,200,7))/100 AS IMP_GUAR, dm_admin.GET_NUMBER (SUBSTR(RESTO,207,7))/100 AS IMP_ACTUALIZACION, SUBSTR(RESTO,214,2) AS DIAS_COTIZADOS_BIM
                                      , SUBSTR(RESTO,216,2) AS DIAS_INCAP_BIM, SUBSTR(RESTO,218,2) AS DIAS_AUSENT_BIM, dm_admin.GET_NUMBER (SUBSTR(RESTO,220,7))/100 AS TOT_RET, dm_admin.GET_NUMBER (SUBSTR(RESTO,227,7))/100 AS TOT_ACT_REC_RET
                                      , dm_admin.GET_NUMBER (SUBSTR(RESTO,234,7))/100 AS TOT_CV_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,241,7))/100 AS TOT_CV_TRAB, dm_admin.GET_NUMBER (SUBSTR(RESTO,248,7))/100 AS TOT_ACT_REC_CV, dm_admin.GET_NUMBER (SUBSTR(RESTO,255,7))/100 AS APORT_VOL
                                      , dm_admin.GET_NUMBER (SUBSTR(RESTO,262,7))/100 AS APORT_PAT_INFO, dm_admin.GET_NUMBER (SUBSTR(RESTO,269,7))/100 AS AMORT_CRED_INFO, dm_admin.DECODIFICADORRSUA (SUBSTR(RESTO,276,3)) AS IMP_OBR_EYM_EXCE
                                      , dm_admin.DECODIFICADORRSUA (SUBSTR(RESTO,279,3)) AS IMP_OBR_EYM_DIN, dm_admin.DECODIFICADORRSUA (SUBSTR(RESTO,282,3)) AS IMP_OBR_EYM_PEN
                                      , dm_admin.DECODIFICADORRSUA (SUBSTR(RESTO,285,3)) AS IMP_OBR_IV, dm_admin.DECODIFICADORRSUA (SUBSTR(RESTO,288,3)) AS IMP_OBR_CV, SUBSTR(RESTO,291,3) AS IMP_OBR_MUN
                                      , SUBSTR(RESTO,294,2) AS DEST_APO_VIV, dm_admin.GET_NUMBER (SUBSTR(RESTO,296,7))/100 AS TOT_APOPAT_INF_EMI, dm_admin.GET_NUMBER (SUBSTR(RESTO,303,7))/100 AS TOT_AMOR_CRE_INF, SUBSTR(RESTO,310,2) AS DIAG_CONFRONTA
                                      --, SUBSTR(RESTO,312,26) AS FILLER1
                                      , SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO
                                      , FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON, SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD
                                      , SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                            from dm_admin.T_carga_rsua
                            where TP_MOV = '03') A3,
                            (select TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,8) AS PLA_SUC, dm_admin.LIMPIACHAR (SUBSTR(RESTO,11,11)) AS REG_PATRON, SUBSTR(RESTO,22,1) AS INF_PAT, SUBSTR(RESTO,23,6) AS FOL_SUA
                                 , SUBSTR(RESTO,29,6) AS PER_PAGO, SUBSTR(RESTO,35,8) AS FEC_PAGO, SUBSTR(RESTO,43,8) AS FEC_VAL_RCV, SUBSTR(RESTO,51,8) AS FEC_VAL_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS
                                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV, dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO, SUBSTR(RESTO,107,231) AS FILLER1, SUBSTR(RESTO,338,2) AS RES_OPER
                                 , SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3, GRUPO AS GRUPO, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, CONSDIA AS CONS_DIA
                        from dm_admin.T_carga_rsua
                        where TP_MOV = '07' OR TP_MOV = '08') B78
        where A3.REG_PATRON = B78.REG_PATRON
        AND TO_NUMBER(A3.FOL_SUA) = TO_NUMBER(B78.FOL_SUA)
        AND A3.FEC_TRAN = B78.FEC_TRAN  
        AND A3.BANCO = B78.BANCO 
        AND A3.CONS_DIA = B78.CONS_DIA   
        AND A3.GRUPO = B78.GRUPO;"""

        A3_DF = DM_ADMIN_T_CARGA_RSUA.select(col('id'), col('resto'), col('tp_mov'), col('grupo'),
                                             col('fec_transferencia'), col('banco'), col('folsua'),
                                             col('consdia')).filter(col('tp_mov') == '03')
        # A3_DF = A3_DF.withColumnRenamed('id', 'id_a3')

        print('> A3_DF fase 1')
        A3_DF = A3_DF.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11))
        A3_DF = A3_DF.withColumn('RFC_PAT', substring(col('resto'), 12, 13)) \
            .withColumn('PERIODO_PAGO', substring(col('resto'), 25, 6))
        A3_DF = A3_DF.withColumn('CVE_NSS', substring(col('resto'), 31, 11)) \
            .withColumn('RFC_TRAB', substring(col('resto'), 42, 13))
        A3_DF = A3_DF.withColumn('CURP', substring(col('resto'), 55, 18)) \
            .withColumn('NUM_CRED_INFONAVIT', substring(col('resto'), 73, 10))
        A3_DF = A3_DF.withColumn('FEC_INI_DESC_INFO', substring(col('resto'), 83, 8))
        A3_DF = A3_DF.withColumn('NUM_MOVTOS_PERIODO', substring(col('resto'), 91, 2))
        A3_DF = A3_DF.withColumn('NOMBRE', substring(col('resto'), 93, 50))
        A3_DF = A3_DF.withColumn('SAL_DIA_INT', substring(col('resto'), 143, 7))
        A3_DF = A3_DF.withColumn('TIPO_TRABAJADOR', substring(col('resto'), 150, 1))
        A3_DF = A3_DF.withColumn('JOR_SEM_RED', substring(col('resto'), 151, 1))
        A3_DF = A3_DF.withColumn('DIAS_COTIZADOS_MES', substring(col('resto'), 152, 2))
        A3_DF = A3_DF.withColumn('DIAS_INCAP_MES', substring(col('resto'), 154, 2))
        A3_DF = A3_DF.withColumn('DIAS_AUSENT_MES', substring(col('resto'), 156, 2))
        A3_DF = A3_DF.withColumn('IMP_EYM_FIJA', substring(col('resto'), 158, 7))
        A3_DF = A3_DF.withColumn('IMP_EYM_EXCE', substring(col('resto'), 165, 7))
        A3_DF = A3_DF.withColumn('IMP_EYM_DIN', substring(col('resto'), 172, 7))
        A3_DF = A3_DF.withColumn('IMP_EYM_PEN', substring(col('resto'), 179, 7))
        A3_DF = A3_DF.withColumn('IMP_RT', substring(col('resto'), 186, 7))
        A3_DF = A3_DF.withColumn('IMP_IV', substring(col('resto'), 193, 7))
        A3_DF = A3_DF.withColumn('IMP_GUAR', substring(col('resto'), 200, 7))
        A3_DF = A3_DF.withColumn('IMP_ACTUALIZACION', substring(col('resto'), 207, 7))
        A3_DF = A3_DF.withColumn('DIAS_COTIZADOS_BIM', substring(col('resto'), 214, 2))
        A3_DF = A3_DF.withColumn('DIAS_INCAP_BIM', substring(col('resto'), 216, 2))
        A3_DF = A3_DF.withColumn('DIAS_AUSENT_BIM', substring(col('resto'), 218, 2))
        A3_DF = A3_DF.withColumn('TOT_RET', substring(col('resto'), 220, 7))
        A3_DF = A3_DF.withColumn('TOT_ACT_REC_RET', col('resto')[227:7])
        A3_DF = A3_DF.withColumn('TOT_CV_PAT', substring(col('resto'), 234, 7))
        A3_DF = A3_DF.withColumn('TOT_CV_TRAB', substring(col('resto'), 241, 7))
        A3_DF = A3_DF.withColumn('TOT_ACT_REC_CV', substring(col('resto'), 248, 7))
        A3_DF = A3_DF.withColumn('APORT_VOL', substring(col('resto'), 255, 7))
        A3_DF = A3_DF.withColumn('APORT_PAT_INFO', substring(col('resto'), 262, 7))
        A3_DF = A3_DF.withColumn('AMORT_CRED_INFO', substring(col('resto'), 269, 7))
        A3_DF = A3_DF.withColumn('IMP_OBR_EYM_EXCE', substring(col('resto'), 276, 3))
        A3_DF = A3_DF.withColumn('IMP_OBR_EYM_DIN', substring(col('resto'), 279, 3))
        A3_DF = A3_DF.withColumn('IMP_OBR_EYM_PEN', substring(col('resto'), 282, 3))
        A3_DF = A3_DF.withColumn('IMP_OBR_IV', substring(col('resto'), 285, 3))
        A3_DF = A3_DF.withColumn('IMP_OBR_CV', substring(col('resto'), 288, 3))
        A3_DF = A3_DF.withColumn('IMP_OBR_MUN', substring(col('resto'), 291, 3))
        A3_DF = A3_DF.withColumn('DEST_APO_VIV', substring(col('resto'), 294, 2))
        A3_DF = A3_DF.withColumn('TOT_APOPAT_INF_EMI', substring(col('resto'), 296, 7))
        A3_DF = A3_DF.withColumn('TOT_AMOR_CRE_INF', substring(col('resto'), 303, 7))
        A3_DF = A3_DF.withColumn('DIAG_CONFRONTA', substring(col('resto'), 310, 2))
        A3_DF = A3_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        A3_DF = A3_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        A3_DF = A3_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        A3_DF = A3_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))
        A3_DF = A3_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        A3_DF = A3_DF.withColumn('BANCO', col('banco'))
        A3_DF = A3_DF.withColumn('FOL_SUA', col('folsua'))
        A3_DF = A3_DF.withColumn('CONS_DIA', col('consdia'))
        A3_DF = A3_DF.withColumn('CVE_PATRON', substring(col('resto'), 1, 11))
        A3_DF = A3_DF.withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11))
        A3_DF = A3_DF.withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))
        A3_DF = A3_DF.withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A3_DF fase 2')
        A3_DF = A3_DF.withColumn('RFC_TRAB', limpia_char(col('RFC_TRAB'))) \
            .withColumn('CURP', limpia_char(col('CURP'))) \
            .withColumn('NOMBRE', limpia_char(col('NOMBRE')))
        A3_DF = A3_DF.withColumn('SAL_DIA_INT', get_number(col('SAL_DIA_INT'))) \
            .withColumn('IMP_OBR_EYM_DIN', decodificador_rsua(col('IMP_OBR_EYM_DIN'))) \
            .withColumn('IMP_OBR_EYM_PEN', decodificador_rsua(col('IMP_OBR_EYM_PEN'))) \
            .withColumn('IMP_OBR_EYM_EXCE', decodificador_rsua(col('IMP_OBR_EYM_EXCE'))) \
            .withColumn('IMP_OBR_IV', decodificador_rsua(col('IMP_OBR_IV'))) \
            .withColumn('IMP_OBR_CV', decodificador_rsua(col('IMP_OBR_CV'))) \
            .withColumn('IMP_EYM_FIJA', get_number(col('IMP_EYM_FIJA'))) \
            .withColumn('IMP_EYM_EXCE', get_number(col('IMP_EYM_EXCE'))) \
            .withColumn('IMP_EYM_DIN', get_number(col('IMP_EYM_DIN'))) \
            .withColumn('IMP_EYM_PEN', get_number(col('IMP_EYM_PEN'))) \
            .withColumn('IMP_RT', get_number(col('IMP_RT'))) \
            .withColumn('IMP_IV', get_number(col('IMP_IV'))) \
            .withColumn('IMP_GUAR', get_number(col('IMP_GUAR'))) \
            .withColumn('IMP_ACTUALIZACION', get_number(col('IMP_ACTUALIZACION'))) \
            .withColumn('TOT_RET', get_number(col('TOT_RET'))) \
            .withColumn('TOT_ACT_REC_RET', get_number(col('TOT_ACT_REC_RET'))) \
            .withColumn('TOT_CV_PAT', get_number(col('TOT_CV_PAT'))) \
            .withColumn('TOT_CV_TRAB', get_number(col('TOT_CV_TRAB'))) \
            .withColumn('TOT_ACT_REC_CV', get_number(col('TOT_ACT_REC_CV'))) \
            .withColumn('APORT_VOL', get_number(col('APORT_VOL'))) \
            .withColumn('APORT_PAT_INFO', get_number(col('APORT_PAT_INFO'))) \
            .withColumn('AMORT_CRED_INFO', get_number(col('AMORT_CRED_INFO'))) \
            .withColumn('TOT_APOPAT_INF_EMI', get_number(col('TOT_APOPAT_INF_EMI'))) \
            .withColumn('TOT_AMOR_CRE_INF', get_number(col('TOT_AMOR_CRE_INF')))
        A3_DF = A3_DF.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A3_DF fase 3')
        A3_DF = A3_DF.withColumn('SAL_DIA_INT', div_100_func(col('SAL_DIA_INT'))) \
            .withColumn('IMP_EYM_FIJA', div_100_func(col('IMP_EYM_FIJA'))) \
            .withColumn('IMP_EYM_EXCE', div_100_func(col('IMP_EYM_EXCE'))) \
            .withColumn('IMP_EYM_DIN', div_100_func(col('IMP_EYM_DIN'))) \
            .withColumn('IMP_EYM_PEN', div_100_func(col('IMP_EYM_PEN'))) \
            .withColumn('IMP_RT', div_100_func(col('IMP_RT'))) \
            .withColumn('IMP_IV', div_100_func(col('IMP_IV'))) \
            .withColumn('IMP_GUAR', div_100_func(col('IMP_GUAR'))) \
            .withColumn('IMP_ACTUALIZACION', div_100_func(col('IMP_ACTUALIZACION'))) \
            .withColumn('TOT_RET', div_100_func(col('TOT_RET'))) \
            .withColumn('TOT_ACT_REC_RET', div_100_func(col('TOT_ACT_REC_RET'))) \
            .withColumn('TOT_CV_PAT', div_100_func(col('TOT_CV_PAT'))) \
            .withColumn('TOT_CV_TRAB', div_100_func(col('TOT_CV_TRAB'))) \
            .withColumn('TOT_ACT_REC_CV', div_100_func(col('TOT_ACT_REC_CV'))) \
            .withColumn('APORT_VOL', div_100_func(col('APORT_VOL'))) \
            .withColumn('APORT_PAT_INFO', div_100_func(col('APORT_PAT_INFO'))) \
            .withColumn('AMORT_CRED_INFO', div_100_func(col('AMORT_CRED_INFO'))) \
            .withColumn('TOT_APOPAT_INF_EMI', div_100_func(col('TOT_APOPAT_INF_EMI'))) \
            .withColumn('TOT_AMOR_CRE_INF', div_100_func(col('TOT_AMOR_CRE_INF')))

        # FEC_INI_DESC_INFO -> aplicar funcion
        A3_DF = A3_DF.withColumn('FEC_INI_DESC_INFO', special_func_fec_ini_desc_info(col('FEC_INI_DESC_INFO')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A3_DF = A3_DF.drop(*drop_cols)
        A3_DF.printSchema()
        # A3_DF.show(10, True)

        # B78_DF = queda igual
        print('> Select * from B78_DF')

        """
        where A3.REG_PATRON = B78.REG_PATRON
        AND TO_NUMBER(A3.FOL_SUA) = TO_NUMBER(B78.FOL_SUA)
        AND A3.FEC_TRAN = B78.FEC_TRAN  
        AND A3.BANCO = B78.BANCO 
        AND A3.CONS_DIA = B78.CONS_DIA   
        AND A3.GRUPO = B78.GRUPO;
        """
        print('> A3_B78_JOIN')
        A3_B78_JOIN = A3_DF.join(B78_DF, (A3_DF['REG_PATRON'] == B78_DF['REG_PATRON_B78']) &
                                 (A3_DF['FEC_TRAN'] == B78_DF['FEC_TRAN_B78']) &
                                 (A3_DF['BANCO'] == B78_DF['BANCO_B78']) &
                                 (get_number(A3_DF['CONS_DIA']) == get_number(B78_DF['CONS_DIA_B78'])) &
                                 (get_number(A3_DF['FOL_SUA']) == get_number(B78_DF['FOL_SUA_B78'])) &
                                 (get_number(A3_DF['GRUPO']) == get_number(B78_DF['GRUPO_B78'])))
        # A2_B78_JOIN.show(20, False)

        """
        SELECT /*+ PARALLEL (A3,10)*/  A3.CVE_NSS, A3.REG_PATRON, A3.PERIODO_PAGO, A3.FEC_TRAN, 
        A3.BANCO AS CVE_ENTIDAD_ORIGEN, A3.FOL_SUA, A3.CONS_DIA, A3.TIPO_REG, A3.RFC_PAT, A3.RFC_TRAB, 
        A3.CURP, A3.NUM_CRED_INFONAVIT
        , TO_DATE (A3.FEC_INI_DESC_INFO,'YYYYMMDD'), A3.NUM_MOVTOS_PERIODO, A3.NOMBRE, A3.SAL_DIA_INT, 
        A3.TIPO_TRABAJADOR, A3.JOR_SEM_RED, A3.DIAS_COTIZADOS_MES, A3.DIAS_INCAP_MES, A3.DIAS_AUSENT_MES, 
        A3.IMP_EYM_FIJA
        , A3.IMP_EYM_EXCE, A3.IMP_EYM_DIN, A3.IMP_EYM_PEN, A3.IMP_RT, A3.IMP_IV, A3.IMP_GUAR, 
        A3.IMP_ACTUALIZACION, A3.DIAS_COTIZADOS_BIM, A3.DIAS_INCAP_BIM, A3.DIAS_AUSENT_BIM, A3.TOT_RET
        , A3.TOT_ACT_REC_RET, A3.TOT_CV_PAT, A3.TOT_CV_TRAB, A3.TOT_ACT_REC_CV, A3.APORT_VOL, 
        A3.APORT_PAT_INFO, A3.AMORT_CRED_INFO, A3.IMP_OBR_EYM_EXCE, A3.IMP_OBR_EYM_DIN, A3.IMP_OBR_EYM_PEN
        , A3.IMP_OBR_IV, A3.IMP_OBR_CV, A3.IMP_OBR_MUN,A3.DEST_APO_VIV, A3.TOT_APOPAT_INF_EMI, 
        A3.TOT_AMOR_CRE_INF, A3.RES_OPER, A3.DIAG_CONFRONTA, A3.DIAG_1, A3.DIAG_2, A3.DIAG_3, 
        TO_DATE (B78.FEC_PAGO,'YYYYMMDD')
        , A3.CVE_PATRON, A3.CVE_MODALIDAD, A3.DIG_VERIFICADOR, 1 as TIPO_ORIG
        """
        print('> A3_10_DF')
        #
        A3_10_DF = A3_B78_JOIN.select(col('GRUPO'), col('CVE_NSS'), col('REG_PATRON'),
                                      col('PERIODO_PAGO'), col('FEC_TRAN'),
                                      col('BANCO').alias('cve_entidad_origen'),
                                      col('FOL_SUA'),
                                      col('CONS_DIA'), col('TIPO_REG'), col('RFC_PAT'), col('RFC_TRAB'),
                                      col('CURP'), col('NUM_CRED_INFONAVIT'), col('FEC_INI_DESC_INFO'),
                                      col('NUM_MOVTOS_PERIODO'), col('NOMBRE'), col('SAL_DIA_INT'),
                                      col('TIPO_TRABAJADOR'), col('JOR_SEM_RED'), col('DIAS_COTIZADOS_MES'),
                                      col('DIAS_INCAP_MES'), col('DIAS_AUSENT_MES'), col('IMP_EYM_FIJA'),
                                      col('IMP_EYM_EXCE'), col('IMP_EYM_DIN'), col('IMP_EYM_PEN'),
                                      col('IMP_RT'), col('IMP_IV'), col('IMP_GUAR'), col('IMP_ACTUALIZACION'),
                                      col('DIAS_COTIZADOS_BIM'), col('DIAS_INCAP_BIM'), col('DIAS_AUSENT_BIM'),
                                      col('TOT_RET'), col('TOT_ACT_REC_RET'), col('TOT_CV_PAT'), col('TOT_CV_TRAB'),
                                      col('TOT_ACT_REC_CV'), col('APORT_VOL'), col('APORT_PAT_INFO'),
                                      col('AMORT_CRED_INFO'), col('IMP_OBR_EYM_EXCE'), col('IMP_OBR_EYM_DIN'),
                                      col('IMP_OBR_EYM_PEN'), col('IMP_OBR_IV'), col('IMP_OBR_CV'),
                                      col('FEC_PAGO_B78').alias('FEC_PAGO'),
                                      col('IMP_OBR_MUN'),
                                      col('DEST_APO_VIV'), col('TOT_APOPAT_INF_EMI'), col('TOT_AMOR_CRE_INF'),
                                      col('RES_OPER'), col('DIAG_CONFRONTA'), col('DIAG_1'), col('DIAG_2'),
                                      col('DIAG_3'), col('CVE_PATRON'), col('CVE_MODALIDAD'),
                                      col('DIG_VERIFICADOR'))
        # 1 as TIPO_ORIG
        A3_10_DF = A3_10_DF.withColumn('TIPO_ORIG', when(lit(True), lit('1')))
        A3_10_DF.printSchema()
        # A3_10_DF.show(20, False)

        # insert into SUT_SIPA_03DETALLE_TRABAJADOR -> Hive
        """
        cve_nss
        cve_reg_patronal
        num_periodo_pago
        fec_transferencia
        cve_entidad_origen
        num_folio_sua
        num_consecutivo_dia
        cve_tipo_registro
        rfc_patron
        rfc_trabajador
        cve_curp
        num_cred_infonavit
        fec_ini_dcto_cred_infonavit
        num_mov_periodo
        nom_trabajador
        imp_ult_sal_dia_int_periodo
        tip_trabajador
        cve_jornada_semana_reducida
        num_dias_cotizados_mes
        num_dias_incapacidad_mes
        num_dias_ausentismo_mes
        imp_cuota_fija_eym
        imp_cuota_excedente_eym
        imp_prest_dinero_eym
        imp_gastos_medicos_pensionados
        imp_riesgos_trabajo
        imp_invalidez_vida
        imp_guarderias_prest_sociales
        imp_act_recargos_4seg_imss
        num_dias_cotizados_bim
        num_dias_incapacidad_bim
        num_dias_ausentismo_bim
        imp_retiro
        imp_act_recargos_retiro
        imp_ces_vej_pat
        imp_ces_vej_obr
        imp_act_recargos_ces_vej
        imp_aport_voluntarias
        imp_aport_patronal_infonavit
        imp_amort_cred_infonavit
        imp_cuota_excedente
        imp_prest_dinero
        imp_gastos_medicos_pen_cod
        imp_invalidez_vida_cod
        imp_ces_vej
        cve_municipio
        cve_des_aport_vivienda
        imp_taport_pat_infonavit_se
        imp_tamort_cred_infonavit_se
        cve_resultado_operacion
        cve_diagnostico_confronta
        cve_diagnostico_1
        cve_diagnostico_2
        cve_diagnostico_3
        fec_pago
        cve_patron
        cve_modalidad
        dig_verificador
        tipo_orig
        fecha_carga
        fecha_proceso
        """

        A3_10_DF_TO_HIVE = A3_10_DF.select(col('cve_nss'),
                                           col('REG_PATRON').alias('cve_reg_patronal'),
                                           col('PERIODO_PAGO').alias('num_periodo_pago'),
                                           col('FEC_TRAN').alias('fec_transferencia'),
                                           col('cve_entidad_origen'),
                                           col('FOL_SUA').alias('num_folio_sua'),
                                           col('CONS_DIA').alias('num_consecutivo_dia'),
                                           col('TIPO_REG').alias('cve_tipo_registro'),
                                           col('RFC_PAT').alias('rfc_patron'),
                                           col('RFC_TRAB').alias('rfc_trabajador'),
                                           col('CURP').alias('cve_curp'),
                                           col('NUM_CRED_INFONAVIT').alias('num_cred_infonavit'),
                                           col('FEC_INI_DESC_INFO').alias('fec_ini_dcto_cred_infonavit'),
                                           col('NUM_MOVTOS_PERIODO').alias('num_mov_periodo'),
                                           col('NOMBRE').alias('nom_trabajador'),
                                           col('SAL_DIA_INT').alias('imp_ult_sal_dia_int_periodo'),
                                           col('TIPO_TRABAJADOR').alias('tip_trabajador'),
                                           col('JOR_SEM_RED').alias('cve_jornada_semana_reducida'),
                                           col('DIAS_COTIZADOS_MES').alias('num_dias_cotizados_mes'),
                                           col('DIAS_INCAP_MES').alias('num_dias_incapacidad_mes'),
                                           col('DIAS_AUSENT_MES').alias('num_dias_ausentismo_mes'),
                                           col('IMP_EYM_FIJA').alias('imp_cuota_fija_eym'),
                                           col('IMP_EYM_EXCE').alias('imp_cuota_excedente_eym'),
                                           col('IMP_EYM_DIN').alias('imp_prest_dinero_eym'),
                                           col('IMP_EYM_PEN').alias('imp_gastos_medicos_pensionados'),
                                           col('IMP_RT').alias('imp_riesgos_trabajo'),
                                           col('IMP_IV').alias('imp_invalidez_vida'),
                                           col('IMP_GUAR').alias('imp_guarderias_prest_sociales'),
                                           col('IMP_ACTUALIZACION').alias('imp_act_recargos_4seg_imss'),
                                           col('DIAS_COTIZADOS_BIM').alias('num_dias_cotizados_bim'),
                                           col('DIAS_INCAP_BIM').alias('num_dias_incapacidad_bim'),
                                           col('DIAS_AUSENT_BIM').alias('num_dias_ausentismo_bim'),
                                           col('TOT_RET').alias('imp_retiro'),
                                           col('TOT_ACT_REC_RET').alias('imp_act_recargos_retiro'),
                                           col('TOT_CV_PAT').alias('imp_ces_vej_pat'),
                                           col('TOT_CV_TRAB').alias('imp_ces_vej_obr'),
                                           col('TOT_ACT_REC_CV').alias('imp_act_recargos_ces_vej'),
                                           col('APORT_VOL').alias('imp_aport_voluntarias'),
                                           col('APORT_PAT_INFO').alias('imp_aport_patronal_infonavit'),
                                           col('AMORT_CRED_INFO').alias('imp_amort_cred_infonavit'),
                                           col('IMP_OBR_EYM_EXCE').alias('imp_cuota_excedente'),
                                           col('IMP_OBR_EYM_DIN').alias('imp_prest_dinero'),
                                           col('IMP_OBR_EYM_PEN').alias('imp_gastos_medicos_pen_cod'),
                                           col('IMP_OBR_IV').alias('imp_invalidez_vida_cod'),
                                           col('IMP_OBR_CV').alias('imp_ces_vej'),
                                           col('IMP_OBR_MUN').alias('cve_municipio'),
                                           col('DEST_APO_VIV').alias('cve_des_aport_vivienda'),
                                           col('TOT_APOPAT_INF_EMI').alias('imp_taport_pat_infonavit_se'),
                                           col('TOT_AMOR_CRE_INF').alias('imp_tamort_cred_infonavit_se'),
                                           col('RES_OPER').alias('cve_resultado_operacion'),
                                           col('DIAG_CONFRONTA').alias('cve_diagnostico_confronta'),
                                           col('DIAG_1').alias('cve_diagnostico_1'),
                                           col('DIAG_2').alias('cve_diagnostico_2'),
                                           col('DIAG_3').alias('cve_diagnostico_3'),
                                           col('FEC_PAGO'),
                                           col('CVE_PATRON'),
                                           col('CVE_MODALIDAD'),
                                           col('DIG_VERIFICADOR'),
                                           col('TIPO_ORIG'))
        A3_10_DF_TO_HIVE = A3_10_DF_TO_HIVE.withColumn('fecha_carga', when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A3_10_DF_TO_HIVE = A3_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=A3_10_DF_TO_HIVE,
                                          dataframe_name='A3_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=A3_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_03detalle_trabajador')
        A3_DF.unpersist()
        A3_B78_JOIN.unpersist()
        # A3_10_DF.unpersist()
        A3_10_DF_TO_HIVE.unpersist()

        # Paso 16: Proc. Carga tablas RSUA
        # Inserta SUT_SIPA_04MOVTO_INCIDENCIA
        """
        FROM (select  /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,12,11)) AS CVE_NSS, SUBSTR(RESTO,23,2) AS TIPO_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,25,8))) AS FEC_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,33,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,33,8))) AS FOLIO_INCAPACIDAD
                                , SUBSTR(RESTO,41,2) AS DIAS_INCIDENCIA, dm_Admin.GET_NUMBER (SUBSTR(RESTO,43,7))/100 AS SAL_INT, SUBSTR(RESTO,338,2) AS RES_OPER
                                ,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3, FEC_TRANSFERENCIA AS FEC_TRAN , BANCO AS BANCO
                                , FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON, SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD
                                , SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR,  GRUPO AS GRUPO, 1 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'
            
                    UNION
                    select  /*+ PARALLEL (T_carga_rsua,10)*/TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,50,11)) AS CVE_NSS, SUBSTR(RESTO,61,2) AS TIPO_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,63,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,63,8))) AS FEC_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,71,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,71,8))) AS FOLIO_INCAPACIDAD, SUBSTR(RESTO,79,2) AS DIAS_INCIDENCIA
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,81,7))/100 AS SAL_INT, SUBSTR(RESTO,338,2) AS RES_OPER,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3
                                , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 2 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'

                    UNION
                    select  /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,88,11)) AS CVE_NSS, SUBSTR(RESTO,99,2) AS TIPO_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,101,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,101,8))) AS FEC_MOVTO_INCIDENCIA
                                , DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,109,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,109,8))) AS FOLIO_INCAPACIDAD, SUBSTR(RESTO,117,2) AS DIAS_INCIDENCIA
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,119,7))/100 AS SAL_INT, SUBSTR(RESTO,338,2) AS RES_OPER,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2
                                ,  SUBSTR(RESTO,346,3) AS DIAG_3, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 3 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'

                    UNION
                    select  /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,126,11)) AS CVE_NSS, SUBSTR(RESTO,137,2) AS TIPO_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,139,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,139,8))) AS FEC_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,147,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,147,8))) AS FOLIO_INCAPACIDAD,  SUBSTR(RESTO,155,2) AS DIAS_INCIDENCIA
                                ,  dm_Admin.GET_NUMBER (SUBSTR(RESTO,157,7))/100 AS SAL_INT, SUBSTR(RESTO,338,2) AS RES_OPER
                                ,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3
                                , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO   , FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 4 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'

                    UNION
                    select /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,164,11)) AS CVE_NSS,  SUBSTR(RESTO,175,2) AS TIPO_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,177,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,177,8))) AS FEC_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,185,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,185,8))) AS FOLIO_INCAPACIDAD
                                ,  SUBSTR(RESTO,193,2) AS DIAS_INCIDENCIA,  dm_Admin.GET_NUMBER (SUBSTR(RESTO,195,7))/100 AS SAL_INT,  SUBSTR(RESTO,338,2) AS RES_OPER
                                ,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3
                                , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO
                                , FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 5 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'

                    UNION
                    select /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,202,11)) AS CVE_NSS,  SUBSTR(RESTO,213,2) AS TIPO_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,215,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,215,8))) AS FEC_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,223,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,223,8))) AS FOLIO_INCAPACIDAD
                                ,  SUBSTR(RESTO,231,2) AS DIAS_INCIDENCIA,  dm_Admin.GET_NUMBER (SUBSTR(RESTO,233,7))/100 AS SAL_INT,  SUBSTR(RESTO,338,2) AS RES_OPER
                                ,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3 , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO
                                , FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 6 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua
                    where TP_MOV = '04'

                    UNION
                    select /*+ PARALLEL (T_carga_rsua,10)*/ TP_MOV AS TIPO_REG, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,1,11)) AS REG_PATRON, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,240,11)) AS CVE_NSS,  SUBSTR(RESTO,251,2) AS TIPO_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,253,8)),NULL,'99991231','00000000','99991231',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,253,8))) AS FEC_MOVTO_INCIDENCIA
                                ,  DECODE(dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8)),NULL,'0','        ','0',dm_Admin.LIMPIACHAR (SUBSTR(RESTO,261,8))) AS FOLIO_INCAPACIDAD,  SUBSTR(RESTO,269,2) AS DIAS_INCIDENCIA
                                ,  dm_Admin.GET_NUMBER (SUBSTR(RESTO,271,7))/100 AS SAL_INT,  SUBSTR(RESTO,338,2) AS RES_OPER,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2,  SUBSTR(RESTO,346,3) AS DIAG_3
                                , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, FOLSUA AS FOL_SUA, CONSDIA AS CONS_DIA, ROWNUM AS CONSEC, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON
                                , SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                                , 7 as NUM_MOVTO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
                    from T_carga_rsua where TP_MOV = '04') A4,

                    (select /*+ PARALLEL (T_carga_rsua,10)*/  TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,8) AS PLA_SUC, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,11,11)) AS REG_PATRON, SUBSTR(RESTO,22,1) AS INF_PAT, SUBSTR(RESTO,23,6) AS FOL_SUA
                         , SUBSTR(RESTO,29,6) AS PER_PAGO, SUBSTR(RESTO,35,8) AS FEC_PAGO, SUBSTR(RESTO,43,8) AS FEC_VAL_RCV, SUBSTR(RESTO,51,8) AS FEC_VAL_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS
                         , dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV, dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO, SUBSTR(RESTO,107,231) AS FILLER1, SUBSTR(RESTO,338,2) AS RES_OPER
                         , SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3, GRUPO AS GRUPO, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, CONSDIA AS CONS_DIA
                from T_carga_rsua where TP_MOV = '07' OR TP_MOV = '08') B78_DF

        where A4.REG_PATRON = B78_DF.REG_PATRON
        AND A4.FEC_TRAN = B78_DF.FEC_TRAN  
        AND A4.BANCO = B78_DF.BANCO 
        AND TO_NUMBER(A4.FOL_SUA) = TO_NUMBER(B78_DF.FOL_SUA)
        AND A4.CONS_DIA = B78_DF.CONS_DIA   
        AND A4.GRUPO = B78_DF.GRUPO
        AND A4.CVE_NSS <> '999999999999999';
        """

        # from T_carga_rsua where TP_MOV = '04'
        A4_DF_BASE = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                                  col('fec_transferencia'), col('banco'), col('folsua'),
                                                  col('consdia')).filter(col('tp_mov') == '04')

        print('> A4_DF_1 fase 1')
        A4_DF_1 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov'))
        A4_DF_1 = A4_DF_1.withColumn('REG_PATRON', substring(col('resto'), 1, 11))
        A4_DF_1 = A4_DF_1.withColumn('CVE_NSS', substring(col('resto'), 12, 11))
        A4_DF_1 = A4_DF_1.withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 23, 2))
        A4_DF_1 = A4_DF_1.withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 25, 8))
        A4_DF_1 = A4_DF_1.withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 33, 8))
        A4_DF_1 = A4_DF_1.withColumn('DIAS_INCIDENCIA', substring(col('resto'), 41, 2))
        A4_DF_1 = A4_DF_1.withColumn('SAL_INT', substring(col('resto'), 43, 7))
        A4_DF_1 = A4_DF_1.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        A4_DF_1 = A4_DF_1.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        A4_DF_1 = A4_DF_1.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        A4_DF_1 = A4_DF_1.withColumn('DIAG_3', substring(col('resto'), 346, 3))
        A4_DF_1 = A4_DF_1.withColumn('FEC_TRAN', col('fec_transferencia'))
        A4_DF_1 = A4_DF_1.withColumn('BANCO', col('banco'))
        A4_DF_1 = A4_DF_1.withColumn('FOL_SUA', col('folsua'))
        A4_DF_1 = A4_DF_1.withColumn('CONS_DIA', col('consdia'))
        A4_DF_1 = A4_DF_1.withColumn('CVE_PATRON', substring(col('resto'), 1, 11))
        A4_DF_1 = A4_DF_1.withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11))
        A4_DF_1 = A4_DF_1.withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))
        A4_DF_1 = A4_DF_1.withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_1 fase 2')
        A4_DF_1 = A4_DF_1.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_1 = A4_DF_1.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_1 = A4_DF_1.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_1 fase 3')
        A4_DF_1 = A4_DF_1.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_1 = A4_DF_1.drop(*drop_cols)
        A4_DF_1.printSchema()

        # MAPEO 2:
        print('> A4_DF_2 fase 1')
        A4_DF_2 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 50, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 61, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 63, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 71, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 79, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 81, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')).withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_2 fase 2')
        A4_DF_2 = A4_DF_2.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_2 = A4_DF_2.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_2 = A4_DF_2.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_2 fase 3')
        A4_DF_2 = A4_DF_2.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_2 = A4_DF_2.drop(*drop_cols)
        A4_DF_2.printSchema()
        # A4_DF_2.show(20, False)

        # MAPEO 3:
        print('> A4_DF_3 fase 1')
        A4_DF_3 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 88, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 99, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 101, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 109, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 117, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 119, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')).withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_3 fase 2')
        A4_DF_3 = A4_DF_3.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_3 = A4_DF_3.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_3 = A4_DF_3.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA')))

        print('> A4_DF_3 fase 3')
        A4_DF_3 = A4_DF_3.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_3 = A4_DF_3.drop(*drop_cols)
        A4_DF_3.printSchema()

        # MAPEO 4:
        print('> A4_DF_4 fase 1')
        A4_DF_4 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 126, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 137, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 139, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 147, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 155, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 157, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')) \
            .withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_4 fase 2')
        A4_DF_4 = A4_DF_4.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_4 = A4_DF_4.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_4 = A4_DF_4.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_4 fase 3')
        A4_DF_4 = A4_DF_4.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_4 = A4_DF_4.drop(*drop_cols)
        A4_DF_4.printSchema()

        # MAPEO 5:
        print('> A4_DF_5 fase 1')
        A4_DF_5 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 164, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 175, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 177, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 185, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 193, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 195, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')).withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)).withColumn('GRUPO', col('grupo')) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_5 fase 2')
        A4_DF_5 = A4_DF_5.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_5 = A4_DF_5.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_5 = A4_DF_5.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_5 fase 3')
        A4_DF_5 = A4_DF_5.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_5 = A4_DF_5.drop(*drop_cols)
        A4_DF_5.printSchema()

        # MAPEO 6:
        print('> A4_DF_6 fase 1')
        A4_DF_6 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 202, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 213, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 215, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 223, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 231, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 233, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')).withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_6 fase 2')
        A4_DF_6 = A4_DF_6.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_6 = A4_DF_6.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_6 = A4_DF_6.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_6 fase 3')
        A4_DF_6 = A4_DF_6.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_6 = A4_DF_6.drop(*drop_cols)
        A4_DF_6.printSchema()

        # MAPEO 7:
        print('> A4_DF_7 fase 1')
        A4_DF_7 = A4_DF_BASE.withColumn('TIPO_REG', col('tp_mov')) \
            .withColumn('REG_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_NSS', substring(col('resto'), 240, 11)) \
            .withColumn('TIPO_MOVTO_INCIDENCIA', substring(col('resto'), 251, 2)) \
            .withColumn('FEC_MOVTO_INCIDENCIA', substring(col('resto'), 253, 8)) \
            .withColumn('FOLIO_INCAPACIDAD', substring(col('resto'), 261, 8)) \
            .withColumn('DIAS_INCIDENCIA', substring(col('resto'), 269, 2)) \
            .withColumn('SAL_INT', substring(col('resto'), 271, 7)) \
            .withColumn('RES_OPER', substring(col('resto'), 338, 2)) \
            .withColumn('DIAG_1', substring(col('resto'), 340, 3)) \
            .withColumn('DIAG_2', substring(col('resto'), 343, 3)) \
            .withColumn('DIAG_3', substring(col('resto'), 346, 3)) \
            .withColumn('FEC_TRAN', col('fec_transferencia')) \
            .withColumn('BANCO', col('banco')).withColumn('FOL_SUA', col('folsua')) \
            .withColumn('CONS_DIA', col('consdia')) \
            .withColumn('CVE_PATRON', substring(col('resto'), 1, 11)) \
            .withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11)) \
            .withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11)) \
            .withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A4_DF_7 fase 2')
        A4_DF_7 = A4_DF_7.withColumn('REG_PATRON', limpia_char(col('REG_PATRON'))) \
            .withColumn('CVE_NSS', limpia_char(col('CVE_NSS'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', limpia_char(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', limpia_char(col('FOLIO_INCAPACIDAD')))
        A4_DF_7 = A4_DF_7.withColumn('SAL_INT', get_number(col('SAL_INT')))
        A4_DF_7 = A4_DF_7.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A4_DF_7 fase 3')
        A4_DF_7 = A4_DF_7.withColumn('SAL_INT', div_100_func(col('SAL_INT'))) \
            .withColumn('FEC_MOVTO_INCIDENCIA', special_func_fec_movto_incidencia(col('FEC_MOVTO_INCIDENCIA'))) \
            .withColumn('FOLIO_INCAPACIDAD', special_func_folio_incapacidad(col('FOLIO_INCAPACIDAD')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'folsua']
        print(drop_cols)
        A4_DF_7 = A4_DF_7.drop(*drop_cols)
        A4_DF_7.printSchema()

        # Uniones de dataframes
        print('> UNION_A4_DF')
        UNION_A4_DF = A4_DF_1.union(A4_DF_2).union(A4_DF_3).union(A4_DF_4) \
            .union(A4_DF_5).union(A4_DF_6).union(A4_DF_7).distinct()

        pySparkTools().describe_dataframe(dataframe=UNION_A4_DF,
                                          dataframe_name='UNION_A4_DF')
        # UNION_A4_DF.show(10, True)

        # B78_DF = queda igual
        print('> Select * from B78_DF')

        print('> A4_B78_JOIN')
        A4_B78_JOIN = UNION_A4_DF.join(B78_DF, (UNION_A4_DF['REG_PATRON'] == B78_DF['REG_PATRON_B78']) &
                                       (UNION_A4_DF['FEC_TRAN'] == B78_DF['FEC_TRAN_B78']) &
                                       (UNION_A4_DF['BANCO'] == B78_DF['BANCO_B78']) &
                                       (get_number(UNION_A4_DF['FOL_SUA']) == get_number(B78_DF['FOL_SUA_B78'])) &
                                       (UNION_A4_DF['CONS_DIA'] == B78_DF['CONS_DIA_B78']) &
                                       (get_number(UNION_A4_DF['GRUPO']) == get_number(B78_DF['GRUPO_B78'])) &
                                       (UNION_A4_DF['CVE_NSS'] != '999999999999999'))

        """
        insert into SUT_SIPA_04MOVTO_INCIDENCIA 
        SELECT /*+ PARALLEL (A4,10)*/ NVL(A4.CVE_NSS,'999999999999999'), A4.REG_PATRON, 
        dm_Admin.LIMPIACHAR(A4.TIPO_MOVTO_INCIDENCIA), TO_DATE(A4.FEC_MOVTO_INCIDENCIA,'YYYYMMDD'), 
        A4.FEC_TRAN, A4.BANCO AS CVE_ENTIDAD_ORIGEN, A4.FOL_SUA, ROWNUM AS CONSEC, A4.CONS_DIA, A4.TIPO_REG, 
        A4.FOLIO_INCAPACIDAD, dm_Admin.LIMPIACHAR(A4.DIAS_INCIDENCIA), A4.SAL_INT, A4.RES_OPER, A4.DIAG_1,  
        A4.DIAG_2,  A4.DIAG_3, TO_DATE(B78_DF.FEC_PAGO,'YYYYMMDD'), A4.CVE_PATRON, A4.CVE_MODALIDAD, 
        A4.DIG_VERIFICADOR, 1 as TIPO_ORIG
        """
        print('> A4_10_DF')
        A4_10_DF = A4_B78_JOIN.select(special_func_cve_nss(col('CVE_NSS')).alias('CVE_NSS'),
                                      col('REG_PATRON'),
                                      col('BANCO').alias('cve_entidad_origen'),
                                      col('TIPO_MOVTO_INCIDENCIA'),
                                      col('FEC_MOVTO_INCIDENCIA'), col('FEC_TRAN'), col('FOL_SUA'),
                                      col('CONS_DIA'), col('TIPO_REG'), col('FOLIO_INCAPACIDAD'),
                                      col('DIAS_INCIDENCIA'), col('SAL_INT'), col('RES_OPER'),
                                      col('DIAG_1'), col('DIAG_2'), col('DIAG_3'),
                                      col('FEC_PAGO_B78').alias('FEC_PAGO'),
                                      col('CVE_PATRON'), col('CVE_MODALIDAD'), col('DIG_VERIFICADOR'))
        A4_10_DF = A4_10_DF.withColumn('TIPO_MOVTO_INCIDENCIA', limpia_char(col('TIPO_MOVTO_INCIDENCIA')))
        A4_10_DF = A4_10_DF.withColumn('DIAS_INCIDENCIA', limpia_char(col('DIAS_INCIDENCIA')))

        A4_10_DF = A4_10_DF.withColumn('CVE_NSS', trim_func(col('CVE_NSS')))
        A4_10_DF = A4_10_DF.withColumn('CVE_NSS', when(col('CVE_NSS').isNotNull(), col('CVE_NSS'))
                                       .otherwise(lit('999999999999999')))
        A4_10_DF = A4_10_DF.withColumn('REG_PATRON', trim_func(col('REG_PATRON')))
        # A4_10_DF = A4_10_DF.withColumn('REG_PATRON', when(col('REG_PATRON').isNotNull(), col('REG_PATRON'))
        #                               .otherwise(lit('-----------')))
        # A4_10_DF = A4_10_DF.withColumn('TIPO_MOVTO_INCIDENCIA', trim_func(col('TIPO_MOVTO_INCIDENCIA')))
        # A4_10_DF = A4_10_DF.withColumn('TIPO_MOVTO_INCIDENCIA',
        #                               when(col('TIPO_MOVTO_INCIDENCIA').isNotNull(), col('TIPO_MOVTO_INCIDENCIA'))
        #                               .otherwise(lit('0')))
        A4_10_DF = pySparkTools().enumerate_rows(dataframe=A4_10_DF, col_name='ROWNUM')

        # print(f'> Null CVE_NSS: {A4_10_DF.filter(col("CVE_NSS").isNull()).count()}')
        # print(f'> Null REG_PATRON: {A4_10_DF.filter(col("REG_PATRON").isNull()).count()}')
        # print(f'> Null TIPO_MOVTO_INCIDENCIA: {A4_10_DF.filter(col("TIPO_MOVTO_INCIDENCIA").isNull()).count()}')

        # A4_10_DF.show(10, False)
        A4_10_DF.printSchema()

        """
        cve_nss
        cve_reg_patronal
        cve_tipo_movimiento_incidencia
        fec_movimiento_incidencia
        fec_transferencia
        cve_entidad_origen
        num_folio_sua
        num_consecutivo
        num_consecutivo_dia
        cve_tipo_registro
        cve_folio_incapacidad
        num_dias_incidencia
        imp_sal_dia_int
        cve_resultado_operacion
        cve_diagnostico_1
        cve_diagnostico_2
        cve_diagnostico_3
        fec_pago
        cve_patron
        cve_modalidad
        dig_verificador
        tipo_orig
        fecha_carga
        fecha_proceso
        
        CVE_NSS                         string NOT NULL,
        CVE_REG_PATRONAL                string NOT NULL,
        CVE_TIPO_MOVIMIENTO_INCIDENCIA  string NOT NULL,
        """

        print('> A4_10_DF_TO_HIVE')
        A4_10_DF_TO_HIVE = A4_10_DF.select(col('cve_nss'),
                                           col('REG_PATRON').alias('cve_reg_patronal'),
                                           col('TIPO_MOVTO_INCIDENCIA').alias('cve_tipo_movimiento_incidencia'),
                                           col('FEC_MOVTO_INCIDENCIA').alias('fec_movimiento_incidencia'),
                                           col('FEC_TRAN').alias('fec_transferencia'),
                                           col('cve_entidad_origen'),
                                           col('FOL_SUA').alias('num_folio_sua'),
                                           col('ROWNUM').alias('num_consecutivo'),
                                           col('CONS_DIA').alias('num_consecutivo_dia'),
                                           col('TIPO_REG').alias('cve_tipo_registro'),
                                           col('FOLIO_INCAPACIDAD').alias('cve_folio_incapacidad'),
                                           col('DIAS_INCIDENCIA').alias('num_dias_incidencia'),
                                           col('SAL_INT').alias('imp_sal_dia_int'),
                                           col('RES_OPER').alias('cve_resultado_operacion'),
                                           col('DIAG_1').alias('cve_diagnostico_1'),
                                           col('DIAG_2').alias('cve_diagnostico_2'),
                                           col('DIAG_3').alias('cve_diagnostico_3'),
                                           col('FEC_PAGO'),
                                           col('CVE_PATRON'),
                                           col('CVE_MODALIDAD'),
                                           col('DIG_VERIFICADOR'))
        A4_10_DF_TO_HIVE = A4_10_DF_TO_HIVE.withColumn('TIPO_ORIG', when(lit(True), lit('1')))
        A4_10_DF_TO_HIVE = A4_10_DF_TO_HIVE.withColumn('fecha_carga', when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A4_10_DF_TO_HIVE = A4_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=A4_10_DF_TO_HIVE,
                                          dataframe_name='A4_10_DF_TO_HIVE')
        # A4_10_DF_TO_HIVE.show(10, True)

        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=A4_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_04movto_incidencia',
                                       optional_query_to_te='insert into te_aficobranza.sut_sipa_04movto_incidencia '
                                                            'select * from lt_aficobranza.sut_sipa_04movto_incidencia '
                                                            'where cve_nss is not null and cve_reg_patronal is not null'
                                                            ' and cve_tipo_movimiento_incidencia is not null')
        A4_DF_1.unpersist()
        A4_DF_2.unpersist()
        A4_DF_3.unpersist()
        A4_DF_4.unpersist()
        A4_DF_5.unpersist()
        A4_DF_6.unpersist()
        A4_DF_7.unpersist()
        # A4_10_DF.unpersist()
        A4_B78_JOIN.unpersist()
        UNION_A4_DF.unpersist()
        A4_10_DF_TO_HIVE.unpersist()

        # Paso 16: Proc. Carga tablas RSUA
        # Inserta SUT_SIPA_05SUMARIO_PATRON
        """INSERT INTO SUT_SIPA_05SUMARIO_PATRON
        SELECT /*+ PARALLEL (A5,10)*/   A5.REG_PATRON, A5.NUM_FOLIO_SUA, A5.PERIODO_PAGO, A5.FEC_TRAN,  A5.BANCO AS CVE_ENTIDAD_ORIGEN, A5.CONS_DIA, A5.TIPO_REG, A5.RFC_PAT, A5.TASA_ACT, A5.TASA_REC, A5.IMP_EYM_CUOTA_FIJA_PAT
             , A5.IMP_EYM_EXCEDENTE_PAT, A5.IMP_EYM_DINERO_PAT, A5.IMP_GMP_PAT, A5.IMP_RT_PAT, A5.IMP_IV_PAT, A5.IMP_GUAR_PAT, A5.IMP_TOTAL_CUO_IMSS, A5.IMP_ACTUALIZACION_PAT, A5.IMP_REC_IMSS
             , A5.IMP_RET_PAT, A5.IMP_CYV_PAT, A5.IMP_REP_CYV_PAT, A5.IMP_ACT_REP_CYV_PAT, A5.IMP_REC_RCV_PAT, A5.IMP_APORT_VOL_PAT, A5.IMP_APORT_PAT_INF_CI_PAT, A5.IMP_APORT_PAT_INF_AC_PAT
             ,A5.IMP_AMO_CRE_INF_PAT, A5.IMP_ACT_APORT_PAT_INF, A5.IMP_REC_APORT_PAT_INF, A5.IMP_APO_COM, A5.RES_OPER, A5.DIAG_1, A5.DIAG_2, A5.DIAG_3, TO_DATE(B78.FEC_PAGO,'YYYYMMDD'), A5.CVE_PATRON, A5.CVE_MODALIDAD
             , A5.DIG_VERIFICADOR, '0' AS IMP_MULTA_IMSS,  '0' AS IMP_ACT_MULTA_IMSS,  '0' AS IMP_MULTA_RCV,  '0' AS IMP_ACT_MULTA_RCV, 1 as TIPO_ORIG
        FROM (select TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,11) AS REG_PATRON, SUBSTR(RESTO,12,13) AS RFC_PAT, SUBSTR(RESTO,25,6) AS PERIODO_PAGO, SUBSTR(RESTO,31,6) AS NUM_FOLIO_SUA
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,37,10))/1000000 AS TASA_ACT, dm_admin.GET_NUMBER (SUBSTR(RESTO,47,10))/1000000 AS TASA_REC, dm_admin.GET_NUMBER (SUBSTR(RESTO,57,9))/100 AS IMP_EYM_CUOTA_FIJA_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,66,9))/100 AS IMP_EYM_EXCEDENTE_PAT
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,75,9))/100 AS IMP_EYM_DINERO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,84,9))/100 AS IMP_GMP_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,93,9))/100 AS IMP_RT_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,102,9))/100 AS IMP_IV_PAT
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,111,9))/100 AS IMP_GUAR_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,120,11))/100 AS IMP_TOTAL_CUO_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,131,9))/100 AS IMP_ACTUALIZACION_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,140,9))/100 AS IMP_REC_IMSS
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,149,9))/100 AS IMP_RET_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,158,9))/100 AS IMP_CYV_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,167,11))/100 AS IMP_REP_CYV_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,178,9))/100 AS IMP_ACT_REP_CYV_PAT
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,187,9))/100 AS IMP_REC_RCV_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,196,11))/100 AS IMP_APORT_VOL_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,207,9))/100 AS IMP_APORT_PAT_INF_CI_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,216,9))/100 AS IMP_APORT_PAT_INF_AC_PAT
                 , dm_admin.GET_NUMBER (SUBSTR(RESTO,225,9))/100 AS IMP_AMO_CRE_INF_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,234,9))/100 AS IMP_ACT_APORT_PAT_INF, dm_admin.GET_NUMBER (SUBSTR(RESTO,243,9))/100 AS IMP_REC_APORT_PAT_INF, dm_admin.GET_NUMBER (SUBSTR(RESTO,252,9))/100 AS IMP_APO_COM
                 --, SUBSTR(RESTO,261,77) AS FILLER
                 , SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3 , FEC_TRANSFERENCIA AS FEC_TRAN
                 , BANCO AS BANCO, CONSDIA AS CONS_DIA, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON, SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR
                 , GRUPO AS GRUPO, 'RSUA'||TO_CHAR(FEC_TRANSFERENCIA,'YYYYMMDD') AS ARCHIVO
        from dm_Admin.T_carga_rsua
        where TP_MOV = '05') A5,
                    (select TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,8) AS PLA_SUC, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,11,11)) AS REG_PATRON, SUBSTR(RESTO,22,1) AS INF_PAT, SUBSTR(RESTO,23,6) AS FOL_SUA
                         , SUBSTR(RESTO,29,6) AS PER_PAGO, SUBSTR(RESTO,35,8) AS FEC_PAGO, SUBSTR(RESTO,43,8) AS FEC_VAL_RCV, SUBSTR(RESTO,51,8) AS FEC_VAL_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS
                         , dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV, dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO, SUBSTR(RESTO,107,231) AS FILLER1, SUBSTR(RESTO,338,2) AS RES_OPER
                         , SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3, GRUPO AS GRUPO, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, CONSDIA AS CONS_DIA
                from dm_Admin.T_carga_rsua
                where TP_MOV = '07' OR TP_MOV = '08') B78
        where A5.REG_PATRON = B78.REG_PATRON
        AND TO_NUMBER(A5.NUM_FOLIO_SUA) = TO_NUMBER(B78.FOL_SUA)
        AND A5.FEC_TRAN = B78.FEC_TRAN  
        AND A5.BANCO = B78.BANCO 
        AND A5.CONS_DIA = B78.CONS_DIA   
        AND A5.GRUPO = B78.GRUPO;"""

        A5_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                             col('fec_transferencia'), col('banco'),
                                             col('consdia')).filter(col('tp_mov') == '05')

        print('> A5_DF fase 1')
        A5_DF = A5_DF.withColumn('TIPO_REG', col('tp_mov'))
        A5_DF = A5_DF.withColumn('REG_PATRON', substring(col('resto'), 1, 11))
        A5_DF = A5_DF.withColumn('RFC_PAT', substring(col('resto'), 12, 13))
        A5_DF = A5_DF.withColumn('PERIODO_PAGO', substring(col('resto'), 25, 6))
        A5_DF = A5_DF.withColumn('NUM_FOLIO_SUA', substring(col('resto'), 31, 6))
        A5_DF = A5_DF.withColumn('TASA_ACT', substring(col('resto'), 37, 10))
        A5_DF = A5_DF.withColumn('TASA_REC', substring(col('resto'), 47, 10))
        A5_DF = A5_DF.withColumn('IMP_EYM_CUOTA_FIJA_PAT', substring(col('resto'), 57, 9))
        A5_DF = A5_DF.withColumn('IMP_EYM_EXCEDENTE_PAT', substring(col('resto'), 66, 9))
        A5_DF = A5_DF.withColumn('IMP_EYM_DINERO_PAT', substring(col('resto'), 75, 9))
        A5_DF = A5_DF.withColumn('IMP_GMP_PAT', substring(col('resto'), 84, 9))
        A5_DF = A5_DF.withColumn('IMP_RT_PAT', substring(col('resto'), 93, 3))
        A5_DF = A5_DF.withColumn('IMP_IV_PAT', substring(col('resto'), 102, 9))
        A5_DF = A5_DF.withColumn('IMP_GUAR_PAT', substring(col('resto'), 111, 9))
        A5_DF = A5_DF.withColumn('IMP_TOTAL_CUO_IMSS', substring(col('resto'), 120, 11))
        A5_DF = A5_DF.withColumn('IMP_ACTUALIZACION_PAT', substring(col('resto'), 131, 9))
        A5_DF = A5_DF.withColumn('IMP_REC_IMSS', substring(col('resto'), 140, 9))
        A5_DF = A5_DF.withColumn('IMP_RET_PAT', substring(col('resto'), 149, 9))
        A5_DF = A5_DF.withColumn('IMP_CYV_PAT', substring(col('resto'), 158, 9))
        A5_DF = A5_DF.withColumn('IMP_REP_CYV_PAT', substring(col('resto'), 167, 11))
        A5_DF = A5_DF.withColumn('IMP_ACT_REP_CYV_PAT', substring(col('resto'), 178, 9))
        A5_DF = A5_DF.withColumn('IMP_REC_RCV_PAT', substring(col('resto'), 187, 9))
        A5_DF = A5_DF.withColumn('IMP_APORT_VOL_PAT', substring(col('resto'), 196, 11))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_CI_PAT', substring(col('resto'), 207, 9))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_AC_PAT', substring(col('resto'), 216, 9))
        A5_DF = A5_DF.withColumn('IMP_AMO_CRE_INF_PAT', substring(col('resto'), 225, 9))
        A5_DF = A5_DF.withColumn('IMP_ACT_APORT_PAT_INF', substring(col('resto'), 234, 9))
        A5_DF = A5_DF.withColumn('IMP_REC_APORT_PAT_INF', substring(col('resto'), 243, 9))
        A5_DF = A5_DF.withColumn('IMP_APO_COM', substring(col('resto'), 252, 9))
        A5_DF = A5_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        A5_DF = A5_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        A5_DF = A5_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        A5_DF = A5_DF.withColumn('DIAG_3', substring(col('resto'), 346, 2))
        A5_DF = A5_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        A5_DF = A5_DF.withColumn('CONS_DIA', col('consdia'))
        A5_DF = A5_DF.withColumn('CVE_PATRON', substring(col('resto'), 1, 11))
        A5_DF = A5_DF.withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11))
        A5_DF = A5_DF.withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))
        A5_DF = A5_DF.withColumn('ARCHIVO', file_name_rsua(col('fec_transferencia')))

        print('> A5_DF fase 2')
        A5_DF = A5_DF.withColumn('TASA_ACT', get_number(col('TASA_ACT')))
        A5_DF = A5_DF.withColumn('TASA_REC', get_number(col('TASA_REC')))
        A5_DF = A5_DF.withColumn('IMP_EYM_CUOTA_FIJA_PAT', get_number(col('IMP_EYM_CUOTA_FIJA_PAT')))
        A5_DF = A5_DF.withColumn('IMP_EYM_EXCEDENTE_PAT', get_number(col('IMP_EYM_EXCEDENTE_PAT')))
        A5_DF = A5_DF.withColumn('IMP_EYM_DINERO_PAT', get_number(col('IMP_EYM_DINERO_PAT')))
        A5_DF = A5_DF.withColumn('IMP_GMP_PAT', get_number(col('IMP_GMP_PAT')))
        A5_DF = A5_DF.withColumn('IMP_RT_PAT', get_number(col('IMP_RT_PAT')))
        A5_DF = A5_DF.withColumn('IMP_IV_PAT', get_number(col('IMP_IV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_GUAR_PAT', get_number(col('IMP_GUAR_PAT')))
        A5_DF = A5_DF.withColumn('IMP_TOTAL_CUO_IMSS', get_number(col('IMP_TOTAL_CUO_IMSS')))
        A5_DF = A5_DF.withColumn('IMP_ACTUALIZACION_PAT', get_number(col('IMP_ACTUALIZACION_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REC_IMSS', get_number(col('IMP_REC_IMSS')))
        A5_DF = A5_DF.withColumn('IMP_RET_PAT', get_number(col('IMP_RET_PAT')))
        A5_DF = A5_DF.withColumn('IMP_CYV_PAT', get_number(col('IMP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REP_CYV_PAT', get_number(col('IMP_REP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_ACT_REP_CYV_PAT', get_number(col('IMP_ACT_REP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REC_RCV_PAT', get_number(col('IMP_REC_RCV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_VOL_PAT', get_number(col('IMP_APORT_VOL_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_CI_PAT', get_number(col('IMP_APORT_PAT_INF_CI_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_AC_PAT', get_number(col('IMP_APORT_PAT_INF_AC_PAT')))
        A5_DF = A5_DF.withColumn('IMP_AMO_CRE_INF_PAT', get_number(col('IMP_AMO_CRE_INF_PAT')))
        A5_DF = A5_DF.withColumn('IMP_ACT_APORT_PAT_INF', get_number(col('IMP_ACT_APORT_PAT_INF')))
        A5_DF = A5_DF.withColumn('IMP_REC_APORT_PAT_INF', get_number(col('IMP_REC_APORT_PAT_INF')))
        A5_DF = A5_DF.withColumn('IMP_APO_COM', get_number(col('IMP_APO_COM')))

        A5_DF = A5_DF.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A5_DF fase 3')
        A5_DF = A5_DF.withColumn('TASA_ACT', div_1000000_func(col('TASA_ACT')))
        A5_DF = A5_DF.withColumn('TASA_REC', div_1000000_func(col('TASA_REC')))
        A5_DF = A5_DF.withColumn('IMP_EYM_CUOTA_FIJA_PAT', div_100_func(col('IMP_EYM_CUOTA_FIJA_PAT')))
        A5_DF = A5_DF.withColumn('IMP_EYM_EXCEDENTE_PAT', div_100_func(col('IMP_EYM_EXCEDENTE_PAT')))
        A5_DF = A5_DF.withColumn('IMP_EYM_DINERO_PAT', div_100_func(col('IMP_EYM_DINERO_PAT')))
        A5_DF = A5_DF.withColumn('IMP_GMP_PAT', div_100_func(col('IMP_GMP_PAT')))
        A5_DF = A5_DF.withColumn('IMP_RT_PAT', div_100_func(col('IMP_RT_PAT')))
        A5_DF = A5_DF.withColumn('IMP_IV_PAT', div_100_func(col('IMP_IV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_GUAR_PAT', div_100_func(col('IMP_GUAR_PAT')))
        A5_DF = A5_DF.withColumn('IMP_TOTAL_CUO_IMSS', div_100_func(col('IMP_TOTAL_CUO_IMSS')))
        A5_DF = A5_DF.withColumn('IMP_ACTUALIZACION_PAT', div_100_func(col('IMP_ACTUALIZACION_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REC_IMSS', div_100_func(col('IMP_REC_IMSS')))
        A5_DF = A5_DF.withColumn('IMP_RET_PAT', div_100_func(col('IMP_RET_PAT')))
        A5_DF = A5_DF.withColumn('IMP_CYV_PAT', div_100_func(col('IMP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REP_CYV_PAT', div_100_func(col('IMP_REP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_ACT_REP_CYV_PAT', div_100_func(col('IMP_ACT_REP_CYV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_REC_RCV_PAT', div_100_func(col('IMP_REC_RCV_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_VOL_PAT', div_100_func(col('IMP_APORT_VOL_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_CI_PAT', div_100_func(col('IMP_APORT_PAT_INF_CI_PAT')))
        A5_DF = A5_DF.withColumn('IMP_APORT_PAT_INF_AC_PAT', div_100_func(col('IMP_APORT_PAT_INF_AC_PAT')))
        A5_DF = A5_DF.withColumn('IMP_AMO_CRE_INF_PAT', div_100_func(col('IMP_AMO_CRE_INF_PAT')))
        A5_DF = A5_DF.withColumn('IMP_ACT_APORT_PAT_INF', div_100_func(col('IMP_ACT_APORT_PAT_INF')))
        A5_DF = A5_DF.withColumn('IMP_REC_APORT_PAT_INF', div_100_func(col('IMP_REC_APORT_PAT_INF')))
        A5_DF = A5_DF.withColumn('IMP_APO_COM', div_100_func(col('IMP_APO_COM')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia']
        print(drop_cols)
        A5_DF = A5_DF.drop(*drop_cols)
        A5_DF.printSchema()

        print('> A5_B78_JOIN')
        A5_B78_JOIN = A5_DF.join(B78_DF).where((A5_DF['REG_PATRON'] == B78_DF['REG_PATRON_B78']) &
                                               (A5_DF['FEC_TRAN'] == B78_DF['FEC_TRAN_B78']) &
                                               (A5_DF['BANCO'] == B78_DF['BANCO_B78']) &
                                               (A5_DF['CONS_DIA'] == B78_DF['CONS_DIA_B78']) &
                                               (get_number(A5_DF['NUM_FOLIO_SUA']) == get_number(
                                                   B78_DF['FOL_SUA_B78'])) &
                                               (get_number(A5_DF['GRUPO']) == get_number(B78_DF['GRUPO_B78'])))

        print('> A5_10_DF')
        A5_10_DF = A5_B78_JOIN.select(col('REG_PATRON'), col('NUM_FOLIO_SUA'),
                                      col('PERIODO_PAGO'), col('FEC_TRAN'),
                                      col('BANCO_B78').alias('CVE_ENTIDAD_ORIGEN'),
                                      col('CONS_DIA'), col('TIPO_REG'), col('RFC_PAT'),
                                      col('TASA_ACT'), col('TASA_REC'), col('IMP_EYM_CUOTA_FIJA_PAT'),
                                      col('IMP_EYM_EXCEDENTE_PAT'), col('IMP_EYM_DINERO_PAT'),
                                      col('IMP_GMP_PAT'), col('IMP_RT_PAT'),
                                      col('IMP_IV_PAT'), col('IMP_GUAR_PAT'),
                                      col('IMP_TOTAL_CUO_IMSS'),
                                      col('IMP_ACTUALIZACION_PAT'), col('IMP_REC_IMSS'), col('IMP_RET_PAT'),
                                      col('IMP_CYV_PAT'),
                                      col('IMP_REP_CYV_PAT'), col('IMP_ACT_REP_CYV_PAT'), col('IMP_REC_RCV_PAT'),
                                      col('IMP_APORT_VOL_PAT'),
                                      col('IMP_APORT_PAT_INF_CI_PAT'), col('IMP_APORT_PAT_INF_AC_PAT'),
                                      col('IMP_AMO_CRE_INF_PAT'), col('IMP_ACT_APORT_PAT_INF'),
                                      col('IMP_REC_APORT_PAT_INF'), col('IMP_APO_COM'), col('RES_OPER'),
                                      col('DIAG_1'), col('DIAG_2'), col('DIAG_3'),
                                      col('FEC_PAGO_B78').alias('FEC_PAGO'),
                                      col('CVE_PATRON'), col('CVE_MODALIDAD'),
                                      col('DIG_VERIFICADOR'))
        # 1 as cte
        A5_10_DF = A5_10_DF.withColumn('TIPO_ORIG', when(lit(True), lit('1')))
        A5_10_DF = A5_10_DF.withColumn('IMP_MULTA_IMSS', when(lit(True), lit('0')))
        A5_10_DF = A5_10_DF.withColumn('IMP_ACT_MULTA_IMSS', when(lit(True), lit('0')))
        A5_10_DF = A5_10_DF.withColumn('IMP_MULTA_RCV', when(lit(True), lit('0')))
        A5_10_DF = A5_10_DF.withColumn('IMP_ACT_MULTA_RCV', when(lit(True), lit('0')))
        A5_10_DF.printSchema()

        A5_10_DF_TO_HIVE = A5_10_DF.select(col('REG_PATRON').alias('cve_reg_patronal'),
                                           col('NUM_FOLIO_SUA').alias('num_folio_sua'),
                                           col('PERIODO_PAGO').alias('num_periodo_pago'),
                                           col('FEC_TRAN').alias('fec_transferencia'),
                                           col('CVE_ENTIDAD_ORIGEN').alias('cve_entidad_origen'),
                                           col('CONS_DIA').alias('num_consecutivo_dia'),
                                           col('TIPO_REG').alias('cve_tipo_registro'),
                                           col('RFC_PAT').alias('rfc_patron'),
                                           col('TASA_ACT').alias('tasa_act_empleada'),
                                           col('TASA_REC').alias('tasa_recargos_empleada'),
                                           col('IMP_EYM_CUOTA_FIJA_PAT').alias('imp_cuota_fija_eym'),
                                           col('IMP_EYM_EXCEDENTE_PAT').alias('imp_cuota_excedente_eym'),
                                           col('IMP_EYM_DINERO_PAT').alias('imp_prest_dinero_eym'),
                                           col('IMP_GMP_PAT').alias('imp_gastos_medicos_pensionados'),
                                           col('IMP_RT_PAT').alias('imp_riesgos_trabajo'),
                                           col('IMP_IV_PAT').alias('imp_invalidez_vida'),
                                           col('IMP_GUAR_PAT').alias('imp_guarderia_prest_sociales'),
                                           col('IMP_TOTAL_CUO_IMSS').alias('imp_stot_4seg_imss'),
                                           col('IMP_ACTUALIZACION_PAT').alias('imp_act_4seg_imss'),
                                           col('IMP_REC_IMSS').alias('imp_recargos_4seg_imss'),
                                           col('IMP_RET_PAT').alias('imp_retiro'),
                                           col('IMP_CYV_PAT').alias('imp_ces_vej'),
                                           col('IMP_REP_CYV_PAT').alias('imp_stot_retiro_ces_vej'),
                                           col('IMP_ACT_REP_CYV_PAT').alias('imp_act_retiro_ces_vej'),
                                           col('IMP_REC_RCV_PAT').alias('imp_rec_retiro_ces_vej'),
                                           col('IMP_APORT_VOL_PAT').alias('imp_aport_voluntarias'),
                                           col('IMP_APORT_PAT_INF_CI_PAT').alias('imp_aport_patr_infonavit_ci'),
                                           col('IMP_APORT_PAT_INF_AC_PAT').alias('imp_aport_patr_infonavit_ac'),
                                           col('IMP_AMO_CRE_INF_PAT').alias('imp_amort_cred_infonavit'),
                                           col('IMP_ACT_APORT_PAT_INF').alias('imp_ac_appat_am_crd_infonavit'),
                                           col('IMP_REC_APORT_PAT_INF').alias('imp_rec_appat_am_crd_infonavit'),
                                           col('IMP_APO_COM').alias('imp_aport_complementarias'),
                                           col('RES_OPER').alias('cve_resultado_operacion'),
                                           col('DIAG_1').alias('cve_diagnostico_1'),
                                           col('DIAG_2').alias('cve_diagnostico_2'),
                                           col('DIAG_3').alias('cve_diagnostico_3'),
                                           col('FEC_PAGO'),
                                           col('CVE_PATRON'),
                                           col('CVE_MODALIDAD'),
                                           col('DIG_VERIFICADOR'),
                                           col('IMP_MULTA_IMSS'),
                                           col('IMP_ACT_MULTA_IMSS'),
                                           col('IMP_MULTA_RCV'),
                                           col('IMP_ACT_MULTA_RCV'),
                                           col('TIPO_ORIG'))
        A5_10_DF_TO_HIVE = A5_10_DF_TO_HIVE.withColumn('fecha_carga', when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A5_10_DF_TO_HIVE = A5_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=A5_10_DF_TO_HIVE,
                                          dataframe_name='A5_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=A5_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_05sumario_patron')
        A5_B78_JOIN.unpersist()
        A5_DF.unpersist()
        # A5_10_DF.unpersist()
        A5_10_DF_TO_HIVE.unpersist()

        # Paso 16: Proc. Carga tablas RSUA
        # Inserta SUT_SIPA_06VALIDACION
        """
        insert into SUT_SIPA_06VALIDACION
        SELECT /*+ PARALLEL (A6,10)*/  A6.REG_PATRON, A6.FOL_SUA, A6.PER_PAGO, A6.FEC_TRAN, A6.CONS_DIA, A6.BANCO AS CVE_ENTIDAD_ORIGEN,  A6.TIPO_REG, A6.RFC_PAT,   A6.SUM_ACT_REC, A6.TAM_ARCH, TO_DATE(A6.FEC_LIM_PAGO,'YYYYMMDD'), A6.NUM_DIS
            , A6.VER_SUA, A6.TOT_DEP_IMSS, A6.TOT_DEP_IMSS_RCV, A6.TOT_DEP_INFO, A6.TOT_DEP_APO_AMO_INFO, A6.CHECKSUM, A6.REV_EYM_FIJA, A6.REV_EYM_EXC_PAT, A6.REV_EYM_EXC_OBR, A6.REV_EYM_DIN_PAT
            , A6.REV_EYM_DIN_OBR, A6.REV_GAS_MED_PAT, A6.REV_GAS_MED_OBR, A6.REV_RTS, A6.REV_IV_PAT, A6.REV_IV_OBR, A6.REV_GUAR, A6.FAC_EYM_FIJA, A6.FAC_EYM_EXC_PAT, A6.FAC_EYM_EXC_OBR
            , A6.FAC_EYM_DIN_PAT, A6.FAC_EYM_DIN_OBR, A6.FAC_GAS_MED_PAT, A6.FAC_GAS_MED_OBR, A6.FAC_RTS, A6.FAC_IV_PAT, A6.FAC_IV_OBR, A6.FAC_GUAR, A6.FAC_AUSE, A6.RES_OPER, A6.DIAG_1, A6.DIAG_2
            , A6.DIAG_3,  TO_DATE(B78.FEC_PAGO,'YYYYMMDD'), A6.CVE_PATRON, A6.CVE_MODALIDAD, A6.DIG_VERIFICADOR, 1 as TIPO_ORIG
        FROM (   select  TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,11) AS REG_PATRON, SUBSTR(RESTO,12,13) AS RFC_PAT, SUBSTR(RESTO,25,6) AS PER_PAGO, SUBSTR(RESTO,31,6) AS FOL_SUA
                                , dm_admin.GET_NUMBER (SUBSTR(RESTO,37,10))/1000000 AS SUM_ACT_REC, SUBSTR(RESTO,47,10) AS TAM_ARCH, SUBSTR(RESTO,57,8) AS FEC_LIM_PAGO, SUBSTR(RESTO,65,2) AS NUM_DIS
                                , SUBSTR(RESTO,67,4) AS VER_SUA, dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS TOT_DEP_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS TOT_DEP_IMSS_RCV, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS TOT_DEP_INFO
                                , dm_admin.GET_NUMBER (SUBSTR(RESTO,107,12))/100 AS TOT_DEP_APO_AMO_INFO, SUBSTR(RESTO,119,19) AS CHECKSUM, dm_Admin.GET_NUMBER (SUBSTR(RESTO,138,9))/100 AS REV_EYM_FIJA
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,147,9))/100 AS REV_EYM_EXC_PAT, dm_Admin.GET_NUMBER (SUBSTR(RESTO,156,9))/100 AS REV_EYM_EXC_OBR, dm_Admin.GET_NUMBER (SUBSTR(RESTO,165,9))/100 AS REV_EYM_DIN_PAT
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,174,9))/100 AS REV_EYM_DIN_OBR, dm_Admin.GET_NUMBER (SUBSTR(RESTO,183,9))/100 AS REV_GAS_MED_PAT, dm_Admin.GET_NUMBER (SUBSTR(RESTO,192,9))/100 AS REV_GAS_MED_OBR
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,201,9))/100 AS REV_RTS, dm_Admin.GET_NUMBER (SUBSTR(RESTO,210,9))/100 AS REV_IV_PAT, dm_Admin.GET_NUMBER (SUBSTR(RESTO,219,9))/100 AS REV_IV_OBR
                                , dm_Admin.GET_NUMBER (SUBSTR(RESTO,228,9))/100 AS REV_GUAR, SUBSTR(RESTO,237,3) AS FAC_EYM_FIJA, SUBSTR(RESTO,240,3) AS FAC_EYM_EXC_PAT, SUBSTR(RESTO,243,3) AS FAC_EYM_EXC_OBR
                                , SUBSTR(RESTO,246,3) AS FAC_EYM_DIN_PAT, SUBSTR(RESTO,249,3) AS FAC_EYM_DIN_OBR, SUBSTR(RESTO,252,3) AS FAC_GAS_MED_PAT, SUBSTR(RESTO,255,3) AS FAC_GAS_MED_OBR
                                , SUBSTR(RESTO,258,3) AS FAC_RTS, SUBSTR(RESTO,261,3) AS FAC_IV_PAT, SUBSTR(RESTO,264,3) AS FAC_IV_OBR, SUBSTR(RESTO,267,3) AS FAC_GUAR, dm_Admin.GET_NUMBER (SUBSTR(RESTO,270,3))/100 AS FAC_AUSE
                                --, SUBSTR(RESTO,273,65) AS FILLER2
                                , SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3
                                , FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, CONSDIA AS CONS_DIA, SUBSTR(SUBSTR(RESTO,1,11),1,8) AS CVE_PATRON, SUBSTR(SUBSTR(RESTO,1,11),9,2) AS CVE_MODALIDAD
                                , SUBSTR(SUBSTR(RESTO,1,11),11,1) AS DIG_VERIFICADOR, GRUPO AS GRUPO
                    from dm_Admin.T_carga_rsua
                    where TP_MOV = '06') A6,
                    (select TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,8) AS PLA_SUC, dm_Admin.LIMPIACHAR (SUBSTR(RESTO,11,11)) AS REG_PATRON, SUBSTR(RESTO,22,1) AS INF_PAT, SUBSTR(RESTO,23,6) AS FOL_SUA
                         , SUBSTR(RESTO,29,6) AS PER_PAGO, SUBSTR(RESTO,35,8) AS FEC_PAGO, SUBSTR(RESTO,43,8) AS FEC_VAL_RCV, SUBSTR(RESTO,51,8) AS FEC_VAL_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS
                         , dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV, dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO, SUBSTR(RESTO,107,231) AS FILLER1, SUBSTR(RESTO,338,2) AS RES_OPER
                         , SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3, GRUPO AS GRUPO, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS BANCO, CONSDIA AS CONS_DIA
                from dm_Admin.T_carga_rsua
                where TP_MOV = '07' OR TP_MOV = '08') B78
                where A6.REG_PATRON = B78.REG_PATRON
                AND TO_NUMBER(A6.FOL_SUA) = TO_NUMBER(B78.FOL_SUA)
                AND A6.FEC_TRAN = B78.FEC_TRAN  
                AND A6.BANCO = B78.BANCO 
                AND A6.CONS_DIA = B78.CONS_DIA   
                AND A6.GRUPO = B78.GRUPO;
        """

        A6_DF = DM_ADMIN_T_CARGA_RSUA.select(col('id'), col('resto'), col('tp_mov'), col('grupo'),
                                             col('fec_transferencia'), col('banco'),
                                             col('consdia')).filter(col('tp_mov') == '06')

        print('> A6_DF fase 1')
        A6_DF = A6_DF.withColumn('TIPO_REG', col('tp_mov'))
        A6_DF = A6_DF.withColumn('REG_PATRON', substring(col('resto'), 1, 11))
        A6_DF = A6_DF.withColumn('RFC_PAT', substring(col('resto'), 12, 13))
        A6_DF = A6_DF.withColumn('PER_PAGO', substring(col('resto'), 25, 6))
        A6_DF = A6_DF.withColumn('FOL_SUA', substring(col('resto'), 31, 6))
        A6_DF = A6_DF.withColumn('SUM_ACT_REC', substring(col('resto'), 37, 10))
        A6_DF = A6_DF.withColumn('TAM_ARCH', substring(col('resto'), 47, 10))
        A6_DF = A6_DF.withColumn('FEC_LIM_PAGO', substring(col('resto'), 57, 8))
        A6_DF = A6_DF.withColumn('NUM_DIS', substring(col('resto'), 65, 2))
        A6_DF = A6_DF.withColumn('VER_SUA', substring(col('resto'), 67, 4))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS', substring(col('resto'), 71, 12))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS_RCV', substring(col('resto'), 83, 12))
        A6_DF = A6_DF.withColumn('TOT_DEP_INFO', substring(col('resto'), 95, 12))
        A6_DF = A6_DF.withColumn('TOT_DEP_APO_AMO_INFO', substring(col('resto'), 107, 12))
        A6_DF = A6_DF.withColumn('CHECKSUM', substring(col('resto'), 119, 19))
        A6_DF = A6_DF.withColumn('REV_EYM_FIJA', substring(col('resto'), 138, 9))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_PAT', substring(col('resto'), 147, 9))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_OBR', substring(col('resto'), 156, 9))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_PAT', substring(col('resto'), 165, 9))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_OBR', substring(col('resto'), 174, 9))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_PAT', substring(col('resto'), 183, 9))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_OBR', substring(col('resto'), 192, 9))
        A6_DF = A6_DF.withColumn('REV_RTS', substring(col('resto'), 201, 9))
        A6_DF = A6_DF.withColumn('REV_IV_PAT', substring(col('resto'), 210, 9))
        A6_DF = A6_DF.withColumn('REV_IV_OBR', substring(col('resto'), 219, 9))
        A6_DF = A6_DF.withColumn('REV_GUAR', substring(col('resto'), 228, 9))
        A6_DF = A6_DF.withColumn('FAC_EYM_FIJA', substring(col('resto'), 237, 3))
        A6_DF = A6_DF.withColumn('FAC_EYM_EXC_PAT', substring(col('resto'), 240, 3))
        A6_DF = A6_DF.withColumn('FAC_EYM_EXC_OBR', substring(col('resto'), 243, 3))
        A6_DF = A6_DF.withColumn('FAC_EYM_DIN_PAT', substring(col('resto'), 246, 3))
        A6_DF = A6_DF.withColumn('FAC_EYM_DIN_OBR', substring(col('resto'), 249, 3))
        A6_DF = A6_DF.withColumn('FAC_GAS_MED_PAT', substring(col('resto'), 252, 3))
        A6_DF = A6_DF.withColumn('FAC_GAS_MED_OBR', substring(col('resto'), 255, 3))
        A6_DF = A6_DF.withColumn('FAC_RTS', substring(col('resto'), 258, 3))
        A6_DF = A6_DF.withColumn('FAC_IV_PAT', substring(col('resto'), 261, 3))
        A6_DF = A6_DF.withColumn('FAC_IV_OBR', substring(col('resto'), 264, 3))
        A6_DF = A6_DF.withColumn('FAC_GUAR', substring(col('resto'), 267, 3))
        A6_DF = A6_DF.withColumn('FAC_AUSE', substring(col('resto'), 270, 3))
        A6_DF = A6_DF.withColumn('RES_OPER', substring(col('resto'), 338, 3))
        A6_DF = A6_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        A6_DF = A6_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        A6_DF = A6_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))
        A6_DF = A6_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        A6_DF = A6_DF.withColumn('BANCO', col('banco'))
        A6_DF = A6_DF.withColumn('CONS_DIA', col('consdia'))
        A6_DF = A6_DF.withColumn('CVE_PATRON', substring(col('resto'), 1, 11))
        A6_DF = A6_DF.withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11))
        A6_DF = A6_DF.withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))

        print('> A6_DF fase 2')
        A6_DF = A6_DF.withColumn('SUM_ACT_REC', get_number(col('SUM_ACT_REC')))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS', get_number(col('TOT_DEP_IMSS')))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS_RCV', get_number(col('TOT_DEP_IMSS_RCV')))
        A6_DF = A6_DF.withColumn('TOT_DEP_INFO', get_number(col('TOT_DEP_INFO')))
        A6_DF = A6_DF.withColumn('TOT_DEP_APO_AMO_INFO', get_number(col('TOT_DEP_APO_AMO_INFO')))
        A6_DF = A6_DF.withColumn('REV_EYM_FIJA', get_number(col('REV_EYM_FIJA')))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_PAT', get_number(col('REV_EYM_EXC_PAT')))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_OBR', get_number(col('REV_EYM_EXC_OBR')))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_PAT', get_number(col('REV_EYM_DIN_PAT')))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_OBR', get_number(col('REV_EYM_DIN_OBR')))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_PAT', get_number(col('REV_GAS_MED_PAT')))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_OBR', get_number(col('REV_GAS_MED_OBR')))
        A6_DF = A6_DF.withColumn('REV_RTS', get_number(col('REV_RTS')))
        A6_DF = A6_DF.withColumn('REV_IV_PAT', get_number(col('REV_IV_PAT')))
        A6_DF = A6_DF.withColumn('REV_IV_OBR', get_number(col('REV_IV_OBR')))
        A6_DF = A6_DF.withColumn('REV_GUAR', get_number(col('REV_GUAR')))
        A6_DF = A6_DF.withColumn('FAC_AUSE', get_number(col('FAC_AUSE')))

        A6_DF = A6_DF.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> A6_DF fase 3')
        A6_DF = A6_DF.withColumn('SUM_ACT_REC', div_1000000_func(col('SUM_ACT_REC')))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS', div_100_func(col('TOT_DEP_IMSS')))
        A6_DF = A6_DF.withColumn('TOT_DEP_IMSS_RCV', div_100_func(col('TOT_DEP_IMSS_RCV')))
        A6_DF = A6_DF.withColumn('TOT_DEP_INFO', div_100_func(col('TOT_DEP_INFO')))
        A6_DF = A6_DF.withColumn('TOT_DEP_APO_AMO_INFO', div_100_func(col('TOT_DEP_APO_AMO_INFO')))
        A6_DF = A6_DF.withColumn('REV_EYM_FIJA', div_100_func(col('REV_EYM_FIJA')))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_PAT', div_100_func(col('REV_EYM_EXC_PAT')))
        A6_DF = A6_DF.withColumn('REV_EYM_EXC_OBR', div_100_func(col('REV_EYM_EXC_OBR')))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_PAT', div_100_func(col('REV_EYM_DIN_PAT')))
        A6_DF = A6_DF.withColumn('REV_EYM_DIN_OBR', div_100_func(col('REV_EYM_DIN_OBR')))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_PAT', div_100_func(col('REV_GAS_MED_PAT')))
        A6_DF = A6_DF.withColumn('REV_GAS_MED_OBR', div_100_func(col('REV_GAS_MED_OBR')))
        A6_DF = A6_DF.withColumn('REV_RTS', div_100_func(col('REV_RTS')))
        A6_DF = A6_DF.withColumn('REV_IV_PAT', div_100_func(col('REV_IV_PAT')))
        A6_DF = A6_DF.withColumn('REV_IV_OBR', div_100_func(col('REV_IV_OBR')))
        A6_DF = A6_DF.withColumn('REV_IV_OBR', div_100_func(col('REV_GUAR')))
        A6_DF = A6_DF.withColumn('FAC_AUSE', div_100_func(col('FAC_AUSE')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia']
        print(drop_cols)
        A6_DF = A6_DF.drop(*drop_cols)
        A6_DF.printSchema()

        print('> A6_B78_JOIN')
        A6_B78_JOIN = A6_DF.join(B78_DF, (A6_DF['REG_PATRON'] == B78_DF['REG_PATRON_B78']) &
                                 (get_number(A6_DF['FOL_SUA']) == get_number(B78_DF['FOL_SUA_B78'])) &
                                 (A6_DF['FEC_TRAN'] == B78_DF['FEC_TRAN_B78']) &
                                 (A6_DF['BANCO'] == B78_DF['BANCO_B78']) &
                                 (A6_DF['CONS_DIA'] == B78_DF['CONS_DIA_B78']) &
                                 (get_number(A6_DF['GRUPO']) == get_number(B78_DF['GRUPO_B78'])))
        A6_B78_JOIN.printSchema()

        print('> A6_10_DF')
        A6_10_DF = A6_B78_JOIN.select(col('REG_PATRON'), col('FOL_SUA'),
                                      col('PER_PAGO'), col('FEC_TRAN'), col('CONS_DIA'),
                                      col('banco').alias('CVE_ENTIDAD_ORIGEN'),
                                      col('TIPO_REG'), col('RFC_PAT'), col('SUM_ACT_REC'),
                                      col('TAM_ARCH'), col('FEC_LIM_PAGO'), col('NUM_DIS'),
                                      col('VER_SUA'), col('TOT_DEP_IMSS'),
                                      col('TOT_DEP_IMSS_RCV'), col('TOT_DEP_INFO'), col('TOT_DEP_APO_AMO_INFO'),
                                      col('CHECKSUM'),
                                      col('REV_EYM_FIJA'),
                                      col('REV_EYM_EXC_PAT'), col('REV_EYM_EXC_OBR'), col('REV_EYM_DIN_PAT'),
                                      col('REV_EYM_DIN_OBR'),
                                      col('REV_GAS_MED_PAT'), col('REV_GAS_MED_OBR'), col('REV_RTS'),
                                      col('REV_IV_PAT'),
                                      col('REV_IV_OBR'), col('REV_GUAR'),
                                      col('FAC_EYM_FIJA'), col('FAC_EYM_EXC_PAT'),
                                      col('FAC_EYM_EXC_OBR'), col('FAC_EYM_DIN_PAT'), col('FAC_EYM_DIN_OBR'),
                                      col('FAC_GAS_MED_PAT'), col('FAC_GAS_MED_OBR'), col('FAC_RTS'),
                                      col('FAC_IV_PAT'), col('FAC_IV_OBR'), col('FAC_GUAR'),
                                      col('FAC_AUSE'), col('RES_OPER'), col('DIAG_1'),
                                      col('DIAG_2'), col('DIAG_3'),
                                      col('FEC_PAGO_B78').alias('FEC_PAGO'),
                                      col('CVE_PATRON'),
                                      col('CVE_MODALIDAD'), col('DIG_VERIFICADOR'))

        # 1 as TIPO_ORIG
        A6_10_DF = A6_10_DF.withColumn('TIPO_ORIG', when(lit(True), lit('1')))
        A6_10_DF.printSchema()

        A6_10_DF_TO_HIVE = A6_10_DF.select(col('REG_PATRON').alias('cve_reg_patronal'),
                                           col('FOL_SUA').alias('num_folio_sua'),
                                           col('PER_PAGO').alias('num_periodo_pago'),
                                           col('FEC_TRAN').alias('fec_transferencia'),
                                           col('CONS_DIA').alias('num_consecutivo_dia'),
                                           col('CVE_ENTIDAD_ORIGEN').alias('cve_entidad_origen'),
                                           col('TIPO_REG').alias('cve_tipo_registro'),
                                           col('RFC_PAT').alias('rfc_patron'),
                                           col('SUM_ACT_REC').alias('suma_tasas_recargos_act'),
                                           col('TAM_ARCH').alias('tamano_archivo'),
                                           col('FEC_LIM_PAGO').alias('fec_limite_pago'),
                                           col('NUM_DIS').alias('num_discos'),
                                           col('VER_SUA').alias('version_sua'),
                                           col('TOT_DEP_IMSS').alias('imp_tdep_cta_imss_cuatro'),
                                           col('TOT_DEP_IMSS_RCV').alias('imp_tdep_cta_imss_rcv'),
                                           col('TOT_DEP_INFO').alias('imp_tdep_cta_infonavit_ap_pat'),
                                           col('TOT_DEP_APO_AMO_INFO').alias('imp_tdep_cta_infonavit_am_cred'),
                                           col('CHECKSUM').alias('ref_checksum_sua'),
                                           col('REV_EYM_FIJA').alias('imp_cuota_fija'),
                                           col('REV_EYM_EXC_PAT').alias('imp_cuota_excedente_patronal'),
                                           col('REV_EYM_EXC_OBR').alias('imp_cuota_excedente_obrero'),
                                           col('REV_EYM_DIN_PAT').alias('imp_prest_dinero_patronal'),
                                           col('REV_EYM_DIN_OBR').alias('imp_prest_dinero_obrero'),
                                           col('REV_GAS_MED_PAT').alias('imp_gastos_med_pensionados_pat'),
                                           col('REV_GAS_MED_OBR').alias('imp_gastos_med_pensionados_obr'),
                                           col('REV_RTS').alias('imp_riesgos_de_trabajo'),
                                           col('REV_IV_PAT').alias('imp_inv_vida_patronal'),
                                           col('REV_IV_OBR').alias('imp_inv_vida_obrero'),
                                           col('REV_GUAR').alias('imp_guarderias_prest_sociales'),
                                           col('FAC_EYM_FIJA').alias('fac_reversion_cf'),
                                           col('FAC_EYM_EXC_PAT').alias('fac_reversion_exc_patronal'),
                                           col('FAC_EYM_EXC_OBR').alias('fac_reversion_exc_obrero'),
                                           col('FAC_EYM_DIN_PAT').alias('fac_reversion_pd_patronal'),
                                           col('FAC_EYM_DIN_OBR').alias('fac_reversion_pd_obrero'),
                                           col('FAC_GAS_MED_PAT').alias('fac_reversion_gmp_patronal'),
                                           col('FAC_GAS_MED_OBR').alias('fac_reversion_gmp_obrero'),
                                           col('FAC_RTS').alias('fac_reversion_rt'),
                                           col('FAC_IV_PAT').alias('fac_reversion_iv_patronal'),
                                           col('FAC_IV_OBR').alias('fac_reversion_iv_obrero'),
                                           col('FAC_GUAR').alias('fac_reversion_gps'),
                                           col('FAC_AUSE').alias('fac_ausentismo'),
                                           col('RES_OPER').alias('cve_resultado_operacion'),
                                           col('DIAG_1').alias('cve_diagnostico_1'),
                                           col('DIAG_2').alias('cve_diagnostico_2'),
                                           col('DIAG_3').alias('cve_diagnostico_3'),
                                           col('FEC_PAGO'),
                                           col('CVE_PATRON').alias('cve_patron'),
                                           col('CVE_MODALIDAD').alias('cve_modalidad'),
                                           col('DIG_VERIFICADOR').alias('dig_verificador'),
                                           col('TIPO_ORIG'))
        A6_10_DF_TO_HIVE = A6_10_DF_TO_HIVE.withColumn('fecha_carga', when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A6_10_DF_TO_HIVE = A6_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=A6_10_DF_TO_HIVE,
                                          dataframe_name='A6_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=A6_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_06validacion')
        A6_10_DF_TO_HIVE.unpersist()
        # A6_10_DF.unpersist()
        A6_B78_JOIN.unpersist()
        A6_DF.unpersist()

        """
        Paso 16: insert into SUT_SIPA_07TRANSAC_VENT_SUA
        select /*+ PARALLEL */     SUBSTR(RESTO,11,11) AS REG_PATRON, SUBSTR(RESTO,23,6) AS FOL_SUA, SUBSTR(RESTO,29,6) AS PER_PAGO, FEC_TRANSFERENCIA AS FEC_TRAN, BANCO AS CVE_ENTIDAD_ORIGEN, CONSDIA AS CONS_DIA
                    , TO_DATE(SUBSTR(RESTO,35,8),'YYYYMMDD') AS FEC_PAGO, TP_MOV AS TIPO_REGISTRO, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,8) AS PLA_SUC,  SUBSTR(RESTO,22,1) AS INF_PAT
                    , TO_DATE(SUBSTR(RESTO,43,8),'YYYYMMDD') AS FEC_VAL_RCV, TO_DATE(SUBSTR(RESTO,51,8),'YYYYMMDD') AS FEC_VAL_IMSS,  dm_admin.GET_NUMBER (SUBSTR(RESTO,59,12))/100 AS IMP_IMSS, dm_admin.GET_NUMBER (SUBSTR(RESTO,71,12))/100 AS IMP_RCV
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,83,12))/100 AS IMP_APO_PAT, dm_admin.GET_NUMBER (SUBSTR(RESTO,95,12))/100 AS IMP_AMOR_INFO, SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2
                    , SUBSTR(RESTO,346,3) AS DIAG_3, SUBSTR(SUBSTR(RESTO,11,11),1,8) AS CVE_PATRON, SUBSTR(SUBSTR(RESTO,11,11),9,2) AS CVE_MODALIDAD, SUBSTR(SUBSTR(RESTO,11,11),11,1) AS DIG_VERIFICADOR
        from dm_admin.T_carga_rsua
        where TP_MOV = '07' OR TP_MOV = '08';
                """
        SUT_SIPA_07_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                                      col('fec_transferencia'), col('banco'),
                                                      col('consdia')).filter((col('tp_mov') == '07') |
                                                                             (col('tp_mov') == '08'))

        print('> SUT_SIPA_07_DF fase 1')
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('REG_PATRON', substring(col('resto'), 1, 11))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('FOL_SUA', substring(col('resto'), 23, 6))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('PER_PAGO', substring(col('resto'), 29, 6))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CVE_ENTIDAD_ORIGEN', col('banco'))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CONS_DIA', col('consdia'))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('FEC_PAGO', substring(col('resto'), 35, 8))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('TIPO_REGISTRO', col('tp_mov'))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('ID_SERV', substring(col('resto'), 1, 2))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('PLA_SUC', substring(col('resto'), 3, 8))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('INF_PAT', substring(col('resto'), 22, 1))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('FEC_VAL_RCV', substring(col('resto'), 43, 8))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('FEC_VAL_IMSS', substring(col('resto'), 51, 8))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_IMSS', substring(col('resto'), 59, 12))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_APO_PAT', substring(col('resto'), 71, 12))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_RCV', substring(col('resto'), 83, 12))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_AMOR_INFO', substring(col('resto'), 95, 12))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CVE_PATRON', substring(col('resto'), 346, 3))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CVE_PATRON', substring(col('resto'), 1, 11))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CVE_MODALIDAD', substring(col('resto'), 1, 11))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('DIG_VERIFICADOR', substring(col('resto'), 1, 11))

        print('> SUT_SIPA_07_DF fase 2')
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_IMSS', get_number(col('IMP_IMSS')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_RCV', get_number(col('IMP_RCV')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_APO_PAT', get_number(col('IMP_APO_PAT')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_AMOR_INFO', get_number(col('IMP_AMOR_INFO')))

        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('CVE_PATRON', substring(col('CVE_PATRON'), 1, 8)) \
            .withColumn('CVE_MODALIDAD', substring(col('CVE_MODALIDAD'), 9, 2)) \
            .withColumn('DIG_VERIFICADOR', substring(col('DIG_VERIFICADOR'), 11, 1))

        print('> SUT_SIPA_07_DF fase 3')
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_IMSS', div_100_func(col('IMP_IMSS')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_RCV', div_100_func(col('IMP_RCV')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_APO_PAT', div_100_func(col('IMP_APO_PAT')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('IMP_AMOR_INFO', div_100_func(col('IMP_AMOR_INFO')))
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.withColumn('TIPO_REG', col('TIPO_REGISTRO'))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia']
        print(drop_cols)
        SUT_SIPA_07_DF = SUT_SIPA_07_DF.drop(*drop_cols)
        SUT_SIPA_07_DF.printSchema()

        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07_DF.select(col('REG_PATRON'), col('FOL_SUA'), col('PER_PAGO'),
                                                               col('FEC_TRAN'),
                                                               col('BANCO').alias('CVE_ENTIDAD_ORIGEN'),
                                                               col('CONS_DIA'),
                                                               col('FEC_PAGO'), col('TIPO_REGISTRO'),
                                                               col('ID_SERV'), col('PLA_SUC'), col('INF_PAT')
                                                               , col('FEC_VAL_RCV'), col('FEC_VAL_IMSS'),
                                                               col('IMP_IMSS')
                                                               , col('IMP_RCV'), col('IMP_APO_PAT'),
                                                               col('IMP_AMOR_INFO')
                                                               , col('RES_OPER'), col('DIAG_1'), col('DIAG_2'),
                                                               col('DIAG_3')
                                                               , col('CVE_PATRON'), col('CVE_MODALIDAD'),
                                                               col('DIG_VERIFICADOR'))

        SUT_SIPA_07TRANSAC_VENT_SUA_DF.printSchema()

        A7_10_DF_TO_HIVE = SUT_SIPA_07TRANSAC_VENT_SUA_DF.withColumn('fecha_carga',
                                                                     when(lit(True), lit(str(V_RSUA_FechaCarga))))
        A7_10_DF_TO_HIVE = A7_10_DF_TO_HIVE.withColumn('fecha_proceso', when(lit(True), lit(str(FEC_PROCESO))))
        pySparkTools().describe_dataframe(dataframe=A7_10_DF_TO_HIVE,
                                          dataframe_name='A7_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=A7_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_07transac_vent_sua')
        # SUT_SIPA_07TRANSAC_VENT_SUA_DF.unpersist()
        SUT_SIPA_07_DF.unpersist()
        A7_10_DF_TO_HIVE.unpersist()

        # Inserta SUT_SIPA_09SUMARIO_TOT
        """
        insert into SUT_SIPA_09SUMARIO_TOT
        select /*+ PARALLEL */    SUBSTR(RESTO,7,3) AS CVE_ENT_ORI, TO_DATE(SUBSTR(RESTO,15,8),'YYYYMMDD') AS FEC_TRAN, CONSDIA AS CONS_DIA, TO_DATE(SUBSTR(RESTO,23,8),'YYYYMMDD') AS FEC_PAGO, TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV
                    , SUBSTR(RESTO,3,2) AS ID_OPER, SUBSTR(RESTO,5,2) AS TPO_ENT_ORI,  SUBSTR(RESTO,10,2) AS TPO_ENT_DES, SUBSTR(RESTO,12,3) AS CVE_ENT_DES,   TO_DATE(SUBSTR(RESTO,31,8),'YYYYMMDD') AS FEC_VAL_RCV
                    , TO_DATE(SUBSTR(RESTO,39,8),'YYYYMMDD') AS FEC_VAL_IMSS, SUBSTR(RESTO,47,10) AS TOT_R06_FP, dm_admin.GET_NUMBER (SUBSTR(RESTO,57,14))/100 AS TOT_IMSS_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,71,14))/100 AS TOT_RCV_R06
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,85,14))/100 AS TOT_VIV_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,99,14))/100 AS TOT_ACV_R06, SUBSTR(RESTO,113,10) AS TOT_R07_FP, dm_admin.GET_NUMBER (SUBSTR(RESTO,123,14))/100 AS TOT_IMSS_R07
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,137,14))/100 AS TOT_RCV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,151,14))/100 AS TOT_VIV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,165,14))/100 AS TOT_ACV_R07, SUBSTR(RESTO,179,10) AS TOT_R08_FP
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,189,14))/100 AS TOT_IMSS_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,203,14))/100 AS TOT_RCV_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,217,14))/100 AS TOT_VIV_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,231,14))/100 AS TOT_ACV_R08
                    , SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2, SUBSTR(RESTO,346,3) AS DIAG_3
        from dm_admin.t_carga_rsua
        where TP_MOV = '09';
        """
        SUT_SIPA_09_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                                      col('fec_transferencia'), col('banco'),
                                                      col('consdia')).filter((col('tp_mov') == '09'))

        print('> SUT_SIPA_09_DF fase 1')
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('CVE_ENT_ORI', substring(col('resto'), 7, 3))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('CONS_DIA', col('consdia'))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TIPO_REG', col('tp_mov'))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('FEC_PAGO', substring(col('resto'), 23, 8))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('ID_SERV', substring(col('resto'), 1, 2))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('ID_OPER', substring(col('resto'), 3, 2))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TPO_ENT_ORI', substring(col('resto'), 5, 2))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TPO_ENT_DES', substring(col('resto'), 10, 2))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('CVE_ENT_DES', substring(col('resto'), 12, 3))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('FEC_VAL_RCV', substring(col('resto'), 31, 8))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('FEC_VAL_IMSS', substring(col('resto'), 39, 8))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_R06_FP', substring(col('resto'), 47, 10))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R06', substring(col('resto'), 57, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R06', substring(col('resto'), 71, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R06', substring(col('resto'), 85, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R06', substring(col('resto'), 99, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_R07_FP', substring(col('resto'), 113, 10))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R07', substring(col('resto'), 123, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R07', substring(col('resto'), 137, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R07', substring(col('resto'), 151, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R07', substring(col('resto'), 165, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_R08_FP', substring(col('resto'), 179, 10))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R08', substring(col('resto'), 189, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R08', substring(col('resto'), 203, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R08', substring(col('resto'), 217, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R08', substring(col('resto'), 231, 14))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))

        print('> SUT_SIPA_09_DF fase 2')
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R06', get_number(col('TOT_IMSS_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R06', get_number(col('TOT_RCV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R06', get_number(col('TOT_VIV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R06', get_number(col('TOT_ACV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R07', get_number(col('TOT_IMSS_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R07', get_number(col('TOT_RCV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R07', get_number(col('TOT_VIV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R07', get_number(col('TOT_ACV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R08', get_number(col('TOT_IMSS_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R08', get_number(col('TOT_RCV_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R08', get_number(col('TOT_VIV_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R08', get_number(col('TOT_ACV_R08')))

        print('> SUT_SIPA_09_DF fase 3')
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R06', div_100_func(col('TOT_IMSS_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R06', div_100_func(col('TOT_RCV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R06', div_100_func(col('TOT_VIV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R06', div_100_func(col('TOT_ACV_R06')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R07', div_100_func(col('TOT_IMSS_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R07', div_100_func(col('TOT_RCV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R07', div_100_func(col('TOT_VIV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R07', div_100_func(col('TOT_ACV_R07')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R08', div_100_func(col('TOT_IMSS_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_RCV_R08', div_100_func(col('TOT_RCV_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_VIV_R08', div_100_func(col('TOT_VIV_R08')))
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.withColumn('TOT_ACV_R08', div_100_func(col('TOT_ACV_R08')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'grupo', 'banco']
        print(drop_cols)
        SUT_SIPA_09_DF = SUT_SIPA_09_DF.drop(*drop_cols)
        SUT_SIPA_09_DF.printSchema()
        print('> Linea 2730 SUT_SIPA_09_DF')
        SUT_SIPA_09_DF.show(10, False)

        SUT_SIPA_09_DF_TO_HIVE = SUT_SIPA_09_DF.select(col('CVE_ENT_ORI').alias('cve_entidad_origen'),
                                                       col('FEC_TRAN').alias('fec_transferencia'),
                                                       col('CONS_DIA').alias('num_consecutivo_dia'),
                                                       col('FEC_PAGO'),
                                                       col('TIPO_REG').alias('cve_tipo_registro'),
                                                       col('ID_SERV').alias('cve_identif_servicio'),
                                                       col('ID_OPER').alias('cve_identif_operacion'),
                                                       col('TPO_ENT_ORI').alias('cve_tipo_entidad_origen'),
                                                       col('TPO_ENT_DES').alias('cve_tipo_entidad_destino'),
                                                       col('CVE_ENT_DES').alias('cve_entidad_destino'),
                                                       col('FEC_VAL_RCV').alias('fec_val_rcv'),
                                                       col('FEC_VAL_IMSS').alias('fec_val_4seg_imss_acv_vivienda'),
                                                       col('TOT_R06_FP').alias('tot_regs_06_fec_pago'),
                                                       col('TOT_IMSS_R06').alias('tot_4seg_imss_regs_06_fec_pago'),
                                                       col('TOT_RCV_R06').alias('tot_rcv_regs_06_fec_pago'),
                                                       col('TOT_VIV_R06').alias('tot_vivienda_regs_06_fec_pago'),
                                                       col('TOT_ACV_R06').alias('tot_acv_regs_06_fec_pago'),
                                                       col('TOT_R07_FP').alias('tot_regs_07_fec_pago'),
                                                       col('TOT_IMSS_R07').alias('tot_4seg_imss_regs_07_fec_pago'),
                                                       col('TOT_RCV_R07').alias('tot_rcv_regs_07_fec_pago'),
                                                       col('TOT_VIV_R07').alias('tot_vivienda_regs_07_fec_pago'),
                                                       col('TOT_ACV_R07').alias('tot_acv_regs_07_fec_pago'),
                                                       col('TOT_R08_FP').alias('tot_regs_08_fec_pago'),
                                                       col('TOT_IMSS_R08').alias('tot_4seg_imss_regs_08_fec_pago'),
                                                       col('TOT_RCV_R08').alias('tot_rcv_regs_08_fec_pago'),
                                                       col('TOT_VIV_R08').alias('tot_vivienda_regs_08_fec_pago'),
                                                       col('TOT_ACV_R08').alias('tot_acv_regs_08_fec_pago'),
                                                       col('RES_OPER').alias('cve_resultado_operacion'),
                                                       col('DIAG_1').alias('cve_diagnostico_1'),
                                                       col('DIAG_2').alias('cve_diagnostico_2'),
                                                       col('DIAG_3').alias('cve_diagnostico_3'))
        SUT_SIPA_09_DF_TO_HIVE = SUT_SIPA_09_DF_TO_HIVE.withColumn('fecha_carga',
                                                                   when(lit(True), lit(str(V_RSUA_FechaCarga))))
        SUT_SIPA_09_DF_TO_HIVE = SUT_SIPA_09_DF_TO_HIVE.withColumn('fecha_proceso',
                                                                   when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=SUT_SIPA_09_DF_TO_HIVE,
                                          dataframe_name='SUT_SIPA_09_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=SUT_SIPA_09_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_09sumario_tot')
        SUT_SIPA_09_DF_TO_HIVE.unpersist()
        # SUT_SIPA_09_DF.unpersist()

        # Inserta Inserta SUT_SIPA_10SUM_LOTE_PAGOS_ER
        """
        insert into SUT_SIPA_10SUM_LOTE_PAGOS_ER
select /*+ PARALLEL */   SUBSTR(RESTO,7,3) AS CVE_ENT_ORI, TO_DATE(SUBSTR(RESTO,15,8),'YYYYMMDD') AS FEC_TRAN, TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,2) AS ID_OPER, SUBSTR(RESTO,5,2) AS TPO_ENT_ORI
            , SUBSTR(RESTO,10,2) AS TPO_ENT_DES, SUBSTR(RESTO,12,3) AS CVE_ENT_DES, SUBSTR(RESTO,23,3) AS CONS_DIA, SUBSTR(RESTO,26,10) AS TOT_R06_ER, dm_admin.GET_NUMBER (SUBSTR(RESTO,36,14))/100 AS TOT_IMSS_R06
            , dm_admin.GET_NUMBER (SUBSTR(RESTO,50,14))/100 AS TOT_RCV_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,64,14))/100 AS TOT_VIV_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,78,14))/100 AS TOT_ACV_R06, SUBSTR(RESTO,92,10) AS TOT_R07_ER
            , dm_admin.GET_NUMBER (SUBSTR(RESTO,102,14))/100 AS TOT_IMSS_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,116,14))/100 AS TOT_RCV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,130,14))/100 AS TOT_VIV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,144,14))/100 AS TOT_ACV_R07
            , SUBSTR(RESTO,158,10) AS TOT_R08_ER, dm_admin.GET_NUMBER (SUBSTR(RESTO,168,14))/100 AS TOT_IMSS_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,182,14))/100 AS TOT_RCV_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,196,14))/100 AS TOT_VIV_R08
            , dm_admin.GET_NUMBER (SUBSTR(RESTO,210,14))/100 AS TOT_ACV_R08, SUBSTR(RESTO,224,7) AS SUM_FEC_PAGO, SUBSTR(RESTO,338,2) AS RES_OPER, SUBSTR(RESTO,340,3) AS DIAG_1, SUBSTR(RESTO,343,3) AS DIAG_2
            , SUBSTR(RESTO,346,3) AS DIAG_3
from dm_admin.t_carga_rsua
where TP_MOV = '10';
        """
        SUT_SIPA_10_DF = DM_ADMIN_T_CARGA_RSUA.select(
            col('resto'), col('tp_mov'), col('grupo'),
            col('fec_transferencia'), col('banco'),
            col('consdia')).filter((col('tp_mov') == '10'))

        print('> SUT_SIPA_10_DF fase 1')
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('CVE_ENT_ORI', substring(col('resto'), 7, 3))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TIPO_REG', col('tp_mov'))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('ID_SERV', substring(col('resto'), 1, 2))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('ID_OPER', substring(col('resto'), 3, 2))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TPO_ENT_ORI', substring(col('resto'), 5, 2))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TPO_ENT_DES', substring(col('resto'), 10, 2))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('CVE_ENT_DES', substring(col('resto'), 12, 3))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('CONS_DIA', substring(col('resto'), 23, 3))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_R06_ER', substring(col('resto'), 26, 10))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R06', substring(col('resto'), 36, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R06', substring(col('resto'), 50, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R06', substring(col('resto'), 64, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R06', substring(col('resto'), 78, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_R07_ER', substring(col('resto'), 92, 10))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R07', substring(col('resto'), 102, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R07', substring(col('resto'), 116, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R07', substring(col('resto'), 130, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R07', substring(col('resto'), 144, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_R08_ER', substring(col('resto'), 158, 10))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R08', substring(col('resto'), 168, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R08', substring(col('resto'), 182, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R08', substring(col('resto'), 196, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R08', substring(col('resto'), 210, 14))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('SUM_FEC_PAGO', substring(col('resto'), 224, 7))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))

        print('> SUT_SIPA_10_DF fase 2')
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R06', get_number(col('TOT_IMSS_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R06', get_number(col('TOT_RCV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R06', get_number(col('TOT_VIV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R06', get_number(col('TOT_ACV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R07', get_number(col('TOT_IMSS_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R07', get_number(col('TOT_RCV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R07', get_number(col('TOT_VIV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R07', get_number(col('TOT_ACV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R08', get_number(col('TOT_IMSS_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R08', get_number(col('TOT_RCV_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R08', get_number(col('TOT_VIV_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R08', get_number(col('TOT_ACV_R08')))
        print('> SUT_SIPA_10_DF fase 3')
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R06', div_100_func(col('TOT_IMSS_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R06', div_100_func(col('TOT_RCV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R06', div_100_func(col('TOT_VIV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R06', div_100_func(col('TOT_ACV_R06')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R07', div_100_func(col('TOT_IMSS_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R07', div_100_func(col('TOT_RCV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R07', div_100_func(col('TOT_VIV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R07', div_100_func(col('TOT_ACV_R07')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R08', div_100_func(col('TOT_IMSS_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_RCV_R08', div_100_func(col('TOT_RCV_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_VIV_R08', div_100_func(col('TOT_VIV_R08')))
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.withColumn('TOT_ACV_R08', div_100_func(col('TOT_ACV_R08')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'grupo', 'banco']
        # print(drop_cols)
        SUT_SIPA_10_DF = SUT_SIPA_10_DF.drop(*drop_cols)

        SUT_SIPA_10_DF.printSchema()

        SUT_SIPA_10_DF_TO_HIVE = SUT_SIPA_10_DF.select(
            col('CVE_ENT_ORI').alias('cve_entidad_origen'),
            col('FEC_TRAN').alias('fec_transferencia'),
            col('TIPO_REG').alias('cve_tipo_registro'),
            col('ID_SERV').alias('cve_identif_servicio'),
            col('ID_OPER').alias('cve_identif_operacion'),
            col('TPO_ENT_ORI').alias('cve_tipo_entidad_origen'),
            col('TPO_ENT_DES').alias('cve_tipo_entidad_destino'),
            col('CVE_ENT_DES').alias('cve_entidad_destino'),
            col('CONS_DIA').alias('num_consecutivo_dia'),
            col('TOT_R06_ER').alias('tot_regs_06_er'),
            col('TOT_IMSS_R06').alias('tot_4seg_imss_regs_06_er'),
            col('TOT_RCV_R06').alias('tot_rcv_regs_06_er'),
            col('TOT_VIV_R06').alias('tot_vivienda_regs_06_er'),
            col('TOT_ACV_R06').alias('tot_acv_regs_06_er'),
            col('TOT_R07_ER').alias('tot_regs_07_er'),
            col('TOT_IMSS_R07').alias('tot_4seg_imss_regs_07_er'),
            col('TOT_RCV_R07').alias('tot_rcv_regs_07_er'),
            col('TOT_VIV_R07').alias('tot_vivienda_regs_07_er'),
            col('TOT_ACV_R07').alias('tot_acv_regs_07_er'),
            col('TOT_R08_ER').alias('tot_regs_08_er'),
            col('TOT_IMSS_R08').alias('tot_4seg_imss_regs_08_er'),
            col('TOT_RCV_R08').alias('tot_rcv_regs_08_er'),
            col('TOT_VIV_R08').alias('tot_vivienda_regs_08_er'),
            col('TOT_ACV_R08').alias('tot_acv_regs_08_er'),
            col('SUM_FEC_PAGO').alias('num_sumarios_fec_pago'),
            col('RES_OPER').alias('cve_resultado_operacion'),
            col('DIAG_1').alias('cve_diagnostico_1'),
            col('DIAG_2').alias('cve_diagnostico_2'),
            col('DIAG_3').alias('cve_diagnostico_3'))

        SUT_SIPA_10_DF_TO_HIVE = SUT_SIPA_10_DF_TO_HIVE.withColumn('fecha_carga',
                                                                   when(lit(True), lit(str(V_RSUA_FechaCarga))))
        SUT_SIPA_10_DF_TO_HIVE = SUT_SIPA_10_DF_TO_HIVE.withColumn('fecha_proceso',
                                                                   when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=SUT_SIPA_10_DF_TO_HIVE,
                                          dataframe_name='SUT_SIPA_10_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=SUT_SIPA_10_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_10sum_lote_pagos_er')
        # SUT_SIPA_10_DF_TO_HIVE.unpersist()
        SUT_SIPA_10_DF.unpersist()

        # Inserta Inserta SUT_SIPA_11SUM_LOTE_NOT
        """
        insert into SUT_SIPA_11SUM_LOTE_NOT
        select /*+ PARALLEL */    TO_DATE(SUBSTR(RESTO,15,8),'YYYYMMDD') AS FEC_TRAN, SUBSTR(RESTO,7,3) AS CVE_ENT_ORI, SUBSTR(RESTO,12,3) AS CVE_ENT_DES, TP_MOV AS TIPO_REG, SUBSTR(RESTO,1,2) AS ID_SERV, SUBSTR(RESTO,3,2) AS ID_OPER
                    , SUBSTR(RESTO,5,2) AS TPO_ENT_ORI,  SUBSTR(RESTO,10,2) AS TPO_ENT_DES,   SUBSTR(RESTO,23,3) AS CONS_DIA, SUBSTR(RESTO,26,10) AS TOT_R06_LOTE, dm_admin.GET_NUMBER (SUBSTR(RESTO,36,14))/100 AS TOT_IMSS_R06
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,50,14))/100 AS TOT_RCV_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,64,14))/100 AS TOT_VIV_R06, dm_admin.GET_NUMBER (SUBSTR(RESTO,78,14))/100 AS TOT_ACV_R06, SUBSTR(RESTO,92,10) AS TOT_R07_LOTE
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,102,14))/100 AS TOT_IMSS_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,116,14))/100 AS TOT_RCV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,130,14))/100 AS TOT_VIV_R07, dm_admin.GET_NUMBER (SUBSTR(RESTO,144,14))/100 AS TOT_ACV_R07
                    , SUBSTR(RESTO,158,10) AS TOT_R08_LOTE, dm_admin.GET_NUMBER (SUBSTR(RESTO,168,14))/100 AS TOT_IMSS_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,182,14))/100 AS TOT_RCV_R08, dm_admin.GET_NUMBER (SUBSTR(RESTO,196,14))/100 AS TOT_VIV_R08
                    , dm_admin.GET_NUMBER (SUBSTR(RESTO,210,14))/100 AS TOT_ACV_R08, SUBSTR(RESTO,224,7) AS TOT_REG, SUBSTR(RESTO,231,7) AS TOT_LIQ, SUBSTR(RESTO,238,7) AS TOT_TV_CON_SUA, SUBSTR(RESTO,245,7) AS TOT_TV_PAPEL
                    , SUBSTR(RESTO,252,7) AS TOT_ENC_ER, SUBSTR(RESTO,259,7) AS TOT_SUM_LOTE, SUBSTR(RESTO,338,2) AS RES_OPER,  SUBSTR(RESTO,340,3) AS DIAG_1,  SUBSTR(RESTO,343,3) AS DIAG_2
                    ,  SUBSTR(RESTO,346,3) AS DIAG_3
        from dm_admin.t_carga_rsua
        where TP_MOV = '11';
        """
        SUT_SIPA_11SUM_DF = DM_ADMIN_T_CARGA_RSUA.select(col('resto'), col('tp_mov'), col('grupo'),
                                                         col('fec_transferencia'), col('banco'),
                                                         col('consdia')).filter((col('tp_mov') == '11'))

        print('> SUT_SIPA_11SUM_DF fase 1')
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('FEC_TRAN', col('fec_transferencia'))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('CVE_ENT_ORI', substring(col('resto'), 7, 3))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('CVE_ENT_DES', substring(col('resto'), 12, 3))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TIPO_REG', col('tp_mov'))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('ID_SERV', substring(col('resto'), 1, 2))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('ID_OPER', substring(col('resto'), 3, 2))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TPO_ENT_ORI', substring(col('resto'), 5, 2))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TPO_ENT_DES', substring(col('resto'), 10, 2))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('CONS_DIA', substring(col('resto'), 23, 3))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_R06_LOTE', substring(col('resto'), 26, 10))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R06', substring(col('resto'), 36, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R06', substring(col('resto'), 50, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R06', substring(col('resto'), 64, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R06', substring(col('resto'), 78, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_R07_LOTE', substring(col('resto'), 92, 10))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R07', substring(col('resto'), 102, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R07', substring(col('resto'), 116, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R07', substring(col('resto'), 130, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R07', substring(col('resto'), 144, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_R08_LOTE', substring(col('resto'), 158, 10))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R08', substring(col('resto'), 168, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R08', substring(col('resto'), 182, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R08', substring(col('resto'), 196, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R08', substring(col('resto'), 210, 14))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_REG', substring(col('resto'), 224, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_LIQ', substring(col('resto'), 231, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_TV_CON_SUA', substring(col('resto'), 238, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_TV_PAPEL', substring(col('resto'), 245, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ENC_ER', substring(col('resto'), 252, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_SUM_LOTE', substring(col('resto'), 259, 7))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('RES_OPER', substring(col('resto'), 338, 2))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('DIAG_1', substring(col('resto'), 340, 3))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('DIAG_2', substring(col('resto'), 343, 3))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('DIAG_3', substring(col('resto'), 346, 3))

        print('> SUT_SIPA_11SUM_DF fase 2')
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R06', get_number(col('TOT_IMSS_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R06', get_number(col('TOT_RCV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R06', get_number(col('TOT_VIV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R06', get_number(col('TOT_ACV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R07', get_number(col('TOT_IMSS_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R07', get_number(col('TOT_RCV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R07', get_number(col('TOT_VIV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R07', get_number(col('TOT_ACV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R08', get_number(col('TOT_IMSS_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R08', get_number(col('TOT_RCV_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R08', get_number(col('TOT_VIV_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R08', get_number(col('TOT_ACV_R08')))

        print('> SUT_SIPA_11SUM_DF fase 3')
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R06', div_100_func(col('TOT_IMSS_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R06', div_100_func(col('TOT_RCV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R06', div_100_func(col('TOT_VIV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R06', div_100_func(col('TOT_ACV_R06')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R07', div_100_func(col('TOT_IMSS_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R07', div_100_func(col('TOT_RCV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R07', div_100_func(col('TOT_VIV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R07', div_100_func(col('TOT_ACV_R07')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R08', div_100_func(col('TOT_IMSS_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_RCV_R08', div_100_func(col('TOT_RCV_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_VIV_R08', div_100_func(col('TOT_VIV_R08')))
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_ACV_R08', div_100_func(col('TOT_ACV_R08')))

        drop_cols = ['resto', 'tp_mov', 'fec_transferencia', 'consdia', 'grupo', 'banco']
        # print(drop_cols)
        SUT_SIPA_11SUM_DF = SUT_SIPA_11SUM_DF.drop(*drop_cols)

        SUT_SIPA_11SUM_DF.printSchema()

        SUT_SIPA_11SUM_DF_TO_HIVE = SUT_SIPA_11SUM_DF.select(
            col('FEC_TRAN').alias('fec_transferencia'),
            col('CVE_ENT_ORI').alias('cve_entidad_origen'),
            col('CVE_ENT_DES').alias('cve_entidad_destino'),
            col('TIPO_REG').alias('cve_tipo_registro'),
            col('ID_SERV').alias('cve_identif_servicio'),
            col('ID_OPER').alias('cve_identif_operacion'),
            col('TPO_ENT_ORI').alias('cve_tipo_entidad_origen'),
            col('TPO_ENT_DES').alias('cve_tipo_entidad_destino'),
            col('CONS_DIA').alias('num_consecutivo_dia'),
            col('TOT_R06_LOTE').alias('tot_regs_06_lote'),
            col('TOT_IMSS_R06').alias('tot_4seg_imss_regs_06'),
            col('TOT_RCV_R06').alias('tot_rcv_regs_06'),
            col('TOT_VIV_R06').alias('tot_vivienda_regs_06'),
            col('TOT_ACV_R06').alias('tot_acv_regs_06'),
            col('TOT_R07_LOTE').alias('tot_regs_07'),
            col('TOT_IMSS_R07').alias('tot_4seg_imss_regs_07'),
            col('TOT_RCV_R07').alias('tot_rcv_regs_07'),
            col('TOT_VIV_R07').alias('tot_vivienda_regs_07'),
            col('TOT_ACV_R07').alias('tot_acv_regs_07'),
            col('TOT_R08_LOTE').alias('tot_regs_08'),
            col('TOT_IMSS_R08').alias('tot_4seg_imss_regs_08'),
            col('TOT_RCV_R08').alias('tot_rcv_regs_08'),
            col('TOT_VIV_R08').alias('tot_vivienda_regs_08'),
            col('TOT_ACV_R08').alias('tot_acv_regs_08'),
            col('TOT_REG').alias('tot_registros'),
            col('TOT_LIQ').alias('tot_liquidaciones'),
            col('TOT_TV_CON_SUA').alias('tot_tvs_sua'),
            col('TOT_TV_PAPEL').alias('tot_tvs_papel'),
            col('TOT_ENC_ER').alias('tot_encabezados_ent_receptoras'),
            col('TOT_SUM_LOTE').alias('num_sumarios_lote'),
            col('RES_OPER').alias('cve_resultado_operacion'),
            col('DIAG_1').alias('cve_diagnostico_1'),
            col('DIAG_2').alias('cve_diagnostico_2'),
            col('DIAG_3').alias('cve_diagnostico_3'))

        SUT_SIPA_11SUM_DF_TO_HIVE = SUT_SIPA_11SUM_DF_TO_HIVE.withColumn('fecha_carga',
                                                                         when(lit(True),
                                                                              lit(str(V_RSUA_FechaCarga))))
        SUT_SIPA_11SUM_DF_TO_HIVE = SUT_SIPA_11SUM_DF_TO_HIVE.withColumn('fecha_proceso',
                                                                         when(lit(True), lit(str(FEC_PROCESO))))

        pySparkTools().describe_dataframe(dataframe=SUT_SIPA_11SUM_DF_TO_HIVE,
                                          dataframe_name='SUT_SIPA_11SUM_DF_TO_HIVE')
        # Insert dataframe to TE stage
        pySparkTools().dataframe_to_te(start_datetime_params=[self.start_datetime, self.start_datetime_proc],
                                       file_name=self.C2_NOMBRE_ARCHIVO_v,
                                       dataframe=SUT_SIPA_11SUM_DF_TO_HIVE,
                                       db_table_lt='lt_aficobranza.sut_sipa_11sum_lote_not')
        SUT_SIPA_11SUM_DF_TO_HIVE.unpersist()
        # SUT_SIPA_11SUM_DF.unpersist()

        # Paso 17: V_RSUA_AnioCarga
        """
        select  TO_CHAR(TO_DATE(SUBSTR(resto,15,8),'YYYYMMDD'),'yyyy') TRANS
        from DM_ADMIN.e_carga_rsua
        where tp_mov='00'
        """

        V_RSUA_AnioCarga_DF = DM_ADMIN_T_CARGA_RSUA.select(col('id'), col('resto'), col('tp_mov'), col('grupo'),
                                                           col('fec_transferencia'), col('banco'),
                                                           col('consdia')).filter((col('tp_mov') == '00'))

        print('> V_RSUA_AnioCarga fase 1')
        RESTO_TEMP = DM_ADMIN_E_CARGA_RSUA.filter((col('tp_mov') == '00')).select(
            col('resto'))
        FEC_TRAN = RESTO_TEMP.first()[0][14:22]
        FEC_TRAN = datetime.datetime(int(FEC_TRAN[:4]), int(FEC_TRAN[4:6]),
                                     int(FEC_TRAN[6:8])).date()
        TRANS = FEC_TRAN.strftime('%Y')
        print(f'> TRANS= {TRANS}')

        # Paso 18 V_RSUA_RegistrosCargadosTMP
        """
        SELECT SUM(TO_NUMBER(A.TOTAL_DE_REGISTROS)) as TOTAL_DE_REGISTROS 
        FROM (SELECT Cve_Tipo_Registro, COUNT(*)  AS TOTAL_DE_REGISTROS
            FROM SUT_SIPA_00ENCABEZADO_LOTE
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')   
            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
            UNION ALL
            SELECT Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_01ENCABEZADO_ENT
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')  
            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_02ENCABEZADO_PATRON
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')
            AND TIPO_ORIG = 1  
            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
            UNION ALL
            SELECT Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS 
            FROM SUT_SIPA_03DETALLE_TRABAJADOR
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            AND TIPO_ORIG = 1
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_04MOVTO_INCIDENCIA
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            AND TIPO_ORIG = 1
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS 
            FROM SUT_SIPA_05SUMARIO_PATRON
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            AND TIPO_ORIG = 1
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT Cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS 
            FROM SUT_SIPA_06VALIDACION
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            AND TIPO_ORIG = 1
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_07TRANSAC_VENT_SUA
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_09SUMARIO_TOT
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_10SUM_LOTE_PAGOS_ER
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
            UNION ALL
            SELECT cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
            FROM SUT_SIPA_11SUM_LOTE_NOT
            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro) A
            WHERE TO_NUMBER(A.Cve_Tipo_Registro) not in (4,8)
        """
        print('> SUT_SIPA_00ENCABEZADO_LOTE')
        SUT_SIPA_00ENCABEZADO_LOTE_18_DF = SUT_SIPA_00ENCABEZADO_LOTE_DF.filter(
            col('FEC_TRANSFERENCIA') == V_RSUA_FechaCarga)
        SUT_SIPA_00ENCABEZADO_LOTE_18_DF = SUT_SIPA_00ENCABEZADO_LOTE_18_DF.groupBy('FEC_TRANSFERENCIA',
                                                                                    "Cve_Tipo_Registro").count()
        SUT_SIPA_00ENCABEZADO_LOTE_18_DF = SUT_SIPA_00ENCABEZADO_LOTE_18_DF.withColumnRenamed('count',
                                                                                              'TOTAL_DE_REGISTROS')
        # SUT_SIPA_00ENCABEZADO_LOTE_DF.printSchema()
        # SUT_SIPA_00ENCABEZADO_LOTE_DF.show()

        print('> SUT_SIPA_01ENCABEZADO_ENT')
        SUT_SIPA_01ENCABEZADO_ENT_DF = SUT_SIPA_01ENCABEZADO_ENT_DF.filter(
            col('FEC_TRANSFERENCIA') == V_RSUA_FechaCarga)
        SUT_SIPA_01ENCABEZADO_ENT_DF = SUT_SIPA_01ENCABEZADO_ENT_DF.groupBy('FEC_TRANSFERENCIA',
                                                                            "Cve_Tipo_Registro").count()
        SUT_SIPA_01ENCABEZADO_ENT_DF = SUT_SIPA_01ENCABEZADO_ENT_DF.withColumnRenamed('count', 'TOTAL_DE_REGISTROS')
        SUT_SIPA_01ENCABEZADO_ENT_DF.printSchema()
        # SUT_SIPA_01ENCABEZADO_ENT_DF.show()

        print('> SUT_SIPA_02ENCABEZADO_PATRON')

        SUT_SIPA_02ENCABEZADO_PATRON = A2_10_DF.select(col('TIPO_REG'), col('TIPO_ORIG'), col('FEC_TRAN')).filter(
            col('TIPO_ORIG') == '1')
        SUT_SIPA_02ENCABEZADO_PATRON = SUT_SIPA_02ENCABEZADO_PATRON.select(col('TIPO_REG'), col('TIPO_ORIG'),
                                                                           col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_02ENCABEZADO_PATRON2 = SUT_SIPA_02ENCABEZADO_PATRON.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_02ENCABEZADO_PATRON2 = SUT_SIPA_02ENCABEZADO_PATRON2.withColumnRenamed('count', 'TOTAL_DE_REGISTROS')
        SUT_SIPA_02ENCABEZADO_PATRON2 = SUT_SIPA_02ENCABEZADO_PATRON2.withColumnRenamed('FEC_TRAN', 'FEC_TRANSFERENCIA')
        # SUT_SIPA_02ENCABEZADO_PATRON2.show(20, False)
        print('> SUT_SIPA_03DETALLE_TRABAJADOR')

        SUT_SIPA_03DETALLE_TRABAJADOR_DF = A3_10_DF.select(col('TIPO_REG'), col('TIPO_ORIG'), col('FEC_TRAN')).filter(
            col('TIPO_ORIG') == '1')
        SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.select(col('TIPO_REG'), col('TIPO_ORIG'),
                                                                                   col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.withColumnRenamed('count',
                                                                                              'TOTAL_DE_REGISTROS')
        SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.withColumnRenamed('FEC_TRAN',
                                                                                              'FEC_TRANSFERENCIA')
        # SUT_SIPA_03DETALLE_TRABAJADOR_DF.show(20, False)
        # print('> SUT_SIPA_04MOVTO_INCIDENCIA')

        # SUT_SIPA_04MOVTO_INCIDENCIA_DF = A4_10_DF.select(col('TIPO_REG'), col('TIPO_ORIG'), col('FEC_TRAN')).filter(
        #   col('TIPO_ORIG') == '1')
        # SUT_SIPA_04MOVTO_INCIDENCIA_DF = A4_10_DF.select(col('TIPO_REG'),col('FEC_TRAN')).filter(
        #     col('FEC_TRAN') == V_RSUA_FechaCarga)

        # SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.withColumnRenamed('count',
        #                                                                                     'TOTAL_DE_REGISTROS')
        # SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.withColumnRenamed('FEC_TRAN',
        #                                                                                    'FEC_TRANSFERENCIA')
        # SUT_SIPA_04MOVTO_INCIDENCIA_DF.show(20, False)
        print('> SUT_SIPA_04MOVTO_INCIDENCIA')
        SUT_SIPA_04MOVTO_INCIDENCIA_DF = A4_10_DF.select(col('TIPO_REG'), col('FEC_TRAN'))
        SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.select(col('TIPO_REG'), col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.withColumnRenamed('count',
                                                                                          'TOTAL_DE_REGISTROS')
        SUT_SIPA_04MOVTO_INCIDENCIA_DF = SUT_SIPA_04MOVTO_INCIDENCIA_DF.withColumnRenamed('FEC_TRAN',
                                                                                          'FEC_TRANSFERENCIA')
        # SUT_SIPA_04MOVTO_INCIDENCIA_DF.show(20, False

        print('> SIPA_05SUMARIO_PATRON')
        SIPA_05SUMARIO_PATRON_DF = A5_10_DF.select(col('TIPO_REG'), col('TIPO_ORIG'), col('FEC_TRAN')).filter(
            col('TIPO_ORIG') == '1')
        SIPA_05SUMARIO_PATRON_DF = SIPA_05SUMARIO_PATRON_DF.select(col('TIPO_REG'), col('TIPO_ORIG'),
                                                                   col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SIPA_05SUMARIO_PATRON_DF = SIPA_05SUMARIO_PATRON_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SIPA_05SUMARIO_PATRON_DF = SIPA_05SUMARIO_PATRON_DF.withColumnRenamed('count',
                                                                              'TOTAL_DE_REGISTROS')
        SIPA_05SUMARIO_PATRON_DF = SIPA_05SUMARIO_PATRON_DF.withColumnRenamed('FEC_TRAN',
                                                                              'FEC_TRANSFERENCIA')
        # SIPA_05SUMARIO_PATRON_DF.show(20, False)

        print('> SUT_SIPA_06VALIDACION')
        SUT_SIPA_06VALIDACION_DF = A6_10_DF.select(col('TIPO_REG'), col('TIPO_ORIG'), col('FEC_TRAN')).filter(
            col('TIPO_ORIG') == '1')
        SUT_SIPA_06VALIDACION_DF = SUT_SIPA_06VALIDACION_DF.select(col('TIPO_REG'), col('TIPO_ORIG'),
                                                                   col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_06VALIDACION_DF = SUT_SIPA_06VALIDACION_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_06VALIDACION_DF = SUT_SIPA_06VALIDACION_DF.withColumnRenamed('count',
                                                                              'TOTAL_DE_REGISTROS')
        SUT_SIPA_06VALIDACION_DF = SUT_SIPA_06VALIDACION_DF.withColumnRenamed('FEC_TRAN',
                                                                              'FEC_TRANSFERENCIA')
        # SUT_SIPA_06VALIDACION_DF.show(20, False)

        print('> SUT_SIPA_07TRANSAC_VENT_SUA')

        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07TRANSAC_VENT_SUA_DF.select(col('FEC_TRAN'),
                                                                               col('TIPO_REGISTRO')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07TRANSAC_VENT_SUA_DF.groupBy('FEC_TRAN', 'TIPO_REGISTRO').count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07TRANSAC_VENT_SUA_DF.withColumnRenamed('count',
                                                                                          'TOTAL_DE_REGISTROS')
        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07TRANSAC_VENT_SUA_DF.withColumnRenamed('FEC_TRAN',
                                                                                          'FEC_TRANSFERENCIA')
        SUT_SIPA_07TRANSAC_VENT_SUA_DF = SUT_SIPA_07TRANSAC_VENT_SUA_DF.withColumnRenamed('TIPO_REGISTRO',
                                                                                          'Cve_Tipo_Registro')
        # SUT_SIPA_07TRANSAC_VENT_SUA_DF.show(20, False)
        print('> SUT_SIPA_09SUMARIO_TOT')

        SUT_SIPA_09_TEMP_DF = SUT_SIPA_09_DF.select(col('TIPO_REG'),
                                                    col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_09_TEMP_DF = SUT_SIPA_09_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_09_TEMP_DF = SUT_SIPA_09_TEMP_DF.withColumnRenamed('count',
                                                                    'TOTAL_DE_REGISTROS')
        SUT_SIPA_09_TEMP_DF = SUT_SIPA_09_TEMP_DF.withColumnRenamed('FEC_TRAN',
                                                                    'FEC_TRANSFERENCIA')
        # SUT_SIPA_09_DF.show(20, False)
        print('> SUT_SIPA_10SUM_LOTE_PAGOS_ER')
        SUT_SIPA_10_TEMP_DF = SUT_SIPA_10_DF.select(col('TIPO_REG'),
                                                    col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_10_TEMP_DF = SUT_SIPA_10_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_10_TEMP_DF = SUT_SIPA_10_TEMP_DF.withColumnRenamed('count',
                                                                    'TOTAL_DE_REGISTROS')
        SUT_SIPA_10_TEMP_DF = SUT_SIPA_10_TEMP_DF.withColumnRenamed('FEC_TRAN',
                                                                    'FEC_TRANSFERENCIA')
        SUT_SIPA_10_TEMP_DF.printSchema()
        # SUT_SIPA_10_DF.show()

        print('> SUT_SIPA_11SUM_LOTE_NOT')

        SUT_SIPA_11SUM_TEMP_DF = SUT_SIPA_11SUM_DF.select(col('TIPO_REG'),
                                                          col('FEC_TRAN')).filter(
            col('FEC_TRAN') == V_RSUA_FechaCarga)
        # SUT_SIPA_02ENCABEZADO_PATRON.printSchema()

        SUT_SIPA_11SUM_TEMP_DF = SUT_SIPA_11SUM_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").count()
        # SUT_SIPA_02ENCABEZADO_PATRON2.printSchema()
        SUT_SIPA_11SUM_TEMP_DF = SUT_SIPA_11SUM_TEMP_DF.withColumnRenamed('count',
                                                                          'TOTAL_DE_REGISTROS')
        SUT_SIPA_11SUM_TEMP_DF = SUT_SIPA_11SUM_TEMP_DF.withColumnRenamed('FEC_TRAN',
                                                                          'FEC_TRANSFERENCIA')
        # SUT_SIPA_11SUM_DF.printSchema()
        # SUT_SIPA_11SUM_DF.show()
        print('>Paso 18 union all ')
        temp_DF = SUT_SIPA_00ENCABEZADO_LOTE_18_DF.unionAll(SUT_SIPA_01ENCABEZADO_ENT_DF).unionAll(
            SUT_SIPA_02ENCABEZADO_PATRON2).unionAll(SUT_SIPA_03DETALLE_TRABAJADOR_DF) \
            .unionAll(SIPA_05SUMARIO_PATRON_DF).unionAll(SUT_SIPA_06VALIDACION_DF).unionAll(
            SUT_SIPA_07TRANSAC_VENT_SUA_DF) \
            .unionAll(SUT_SIPA_09_TEMP_DF).unionAll(SUT_SIPA_10_TEMP_DF).unionAll(SUT_SIPA_11SUM_TEMP_DF)

        totales_DF = temp_DF.filter(temp_DF.Cve_Tipo_Registro != '08')
        # sum_DF.show()
        temp_DF = totales_DF.select(sum(totales_DF.TOTAL_DE_REGISTROS).alias("TOTAL_DE_REGISTROS"))

        TOTAL_DE_REGISTROS = temp_DF.collect()[0][0]

        print(f"Total registros {TOTAL_DE_REGISTROS}")

        # Paso 19: V_RSUA_NombreArchivoCifras.
        # select 'FECH'||substr('#COBRANZA.V_RSUA_NombreArchivo',5,10) from dual
        V_RSUA_NombreArchivoCifras = 'FECH' + V_RSUA_NombreArchivoSalida

        # Paso 20-21: Pass

        # Paso 21: Carga-Integracion
        # Carga de archivo
        if os_running != 'windows':
            try:
                # Validation 1
                txt_file_validation = False
                for dirOrFile in os.listdir(f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua'):
                    if dirOrFile.endswith(('.txt', '.TXT')):
                        txt_file_validation = True
                        break
                if not txt_file_validation:
                    try:
                        print(f'> Copying LA txt file to local LA...')
                        command = f'hdfs dfs -get hdfs://cnhcsepraphadoop-0001.imss.gob.mx:8020/bdaimss/la/' \
                                  f'la_aficobranza/SUT_CIFRAS_SUA/*.TXT ' \
                                  f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua'
                        print(f'> exec command: {command}')
                        subprocess.run([command], shell=True, check=True)
                    except subprocess.CalledProcessError:
                        raise
                else:
                    print(f'[ok] TXT file exist in {PATH_LOCAL_TEMP_LA}/sut_cifras_sua')
            except Exception as e:
                err = f'[error] {PATH_LOCAL_TEMP_LA}/sut_cifras_sua'
                print(f'{err}. {e}')
                # if falla enviar error:
                # SUBJECT=ERROR - RSUA
                # CONTENIDO: DM_ADMIN_T_CARGA_RSUA. {err}
                # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'


                err = f'HDFS get txt file. {e}'
                email_content = f"Error en el paso : Int. Carga cifras de validacion. \n\nDescripción:{err}"
                appTools().email_sender(email_content=email_content,
                                        to_email_list=TO_EMAIL_LIST,
                                        subject='ERROR - RSUA')
                print(f'[error] {err}')
                error_id = '1.0'
                error_description = err
                print('> Sending [error] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)

        # Validation 2
        txt_file_validation = False
        for dirOrFile in os.listdir(f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua'):
            if dirOrFile.endswith(('.txt', '.TXT')):
                txt_file_validation = True
                break

        if txt_file_validation:
            txt_file_name = ''
            try:
                for dirOrFile in os.listdir(f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua'):
                    if dirOrFile.endswith(('.txt', '.TXT')):
                        txt_file_name = dirOrFile
                        path_txt_file = f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua/{txt_file_name}'
                        break
                if not os.path.isfile(path_txt_file):
                    raise ValueError(f'TXT file does not exist in {PATH_LOCAL_TEMP_LA}/sut_cifras_sua')
            except Exception as e:
                err = f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua. {e}'
                print(f'[error] {err}')
                error_id = '1.0'
                error_description = err
                print('> Sending [error] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)
                txt_file_name = ''

            if txt_file_name:
                SUT_CIFRAS_SUA_DF = pySparkTools().dataframe_from_file(spark_obj=spark_obj,
                                                                       path=f'{PATH_LOCAL_TEMP_LA}/sut_cifras_sua/'
                                                                            f'{txt_file_name}',
                                                                       delimiter='^', header=False)
                if SUT_CIFRAS_SUA_DF:
                    new_colsNames = ['tipo_registro', 'casos', 'cuatro_ramos', 'act_recargos', 'rcv',
                                     'act_recargos_rcv', 'status']
                    SUT_CIFRAS_SUA_DF = pySparkTools().rename_col(dataframe=SUT_CIFRAS_SUA_DF,
                                                                  new_colsNames=new_colsNames)

                    # Caracteristicas del Dataframe
                    SUT_CIFRAS_SUA_DF_LEN_COLS = len(SUT_CIFRAS_SUA_DF.columns)
                    SUT_CIFRAS_SUA_DF_LEN_ROWS = SUT_CIFRAS_SUA_DF.count()
                    file_spec = os.stat(path_txt_file)
                    print(f'[ok] File <\033[93m{txt_file_name}\033[0m> loaded to dataframe '
                          f'SUT_CIFRAS_SUA_DF successfully!')
                    print(f'> Dataframe size: Rows= {SUT_CIFRAS_SUA_DF_LEN_ROWS}, Cols = {SUT_CIFRAS_SUA_DF_LEN_COLS}.')
                    print(f'> File size: {round(file_spec.st_size / 1024, 1)}KB')
                    SUT_CIFRAS_SUA_DF.printSchema()

                    SUT_CIFRAS_SUA_DF = SUT_CIFRAS_SUA_DF.withColumn('casos', replace_comma_func(col('casos')))
                    SUT_CIFRAS_SUA_DF = SUT_CIFRAS_SUA_DF.withColumn('cuatro_ramos',
                                                                     replace_comma_func(col('cuatro_ramos')))
                    SUT_CIFRAS_SUA_DF = SUT_CIFRAS_SUA_DF.withColumn('act_recargos',
                                                                     replace_comma_func(col('act_recargos')))
                    SUT_CIFRAS_SUA_DF = SUT_CIFRAS_SUA_DF.withColumn('rcv', replace_comma_func(col('rcv')))
                    SUT_CIFRAS_SUA_DF = SUT_CIFRAS_SUA_DF.withColumn('act_recargos_rcv',
                                                                     replace_comma_func(col('act_recargos_rcv')))
                    # SUT_CIFRAS_SUA_DF.show()
                else:
                    e = f'Failure on dataframe creation: SUT_CIFRAS_SUA_DF'
                    print(f'[error] {e}')
                    error_id = '2.2'
                    error_description = e
                    ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                          start_datetime=self.start_datetime,
                                                          start_datetime_proc=self.start_datetime_proc,
                                                          end_datetime_proc=appTools().get_datime_now(),
                                                          error_id=error_id,
                                                          error_description=error_description,
                                                          process_name=process_name)
                    appTools().error_logger(ctrl_cif=ctrl_cfrs)

                # Integracion
                """
                insert into	DM_ADMIN.SUT_CIFRAS_SUA 
                (   NOM_ARCHIVO,
                    FEC_PROCESO,
                    CIFRAS,
                    ESTATUS,
                    TIPO_REGISTRO,
                    CASOS,
                    CUATRO_RAMOS,
                    ACT_RECARGOS,
                    RCV,
                    ACT_RECARGOS_RCV) 
    
                select
                    NOM_ARCHIVO,
                    FEC_PROCESO,
                    CIFRAS,
                    ESTATUS,
                    TIPO_REGISTRO,
                    CASOS,
                    CUATRO_RAMOS,
                    ACT_RECARGOS,
                    RCV,
                    ACT_RECARGOS_RCV   
    
                FROM (	
                select 	DISTINCT
                    '#COBRANZA.V_RSUA_NombreArchivo' NOM_ARCHIVO,
                    TRUNC(sysdate) FEC_PROCESO,
                    C4_TIPO_REGISTRO || C1_CASOS || C7_CUATRO_RAMOS || C6_ACT_RECARGOS || C5_RCV || C2_ACT_RECARGOS_RCV || C3_ESTATUS CIFRAS,
                    C3_ESTATUS ESTATUS,
                    TRIM(C4_TIPO_REGISTRO) TIPO_REGISTRO,
                    REPLACE(C1_CASOS,',','') CASOS,
                    REPLACE(C7_CUATRO_RAMOS,',','') CUATRO_RAMOS,
                    REPLACE(C6_ACT_RECARGOS,',','') ACT_RECARGOS,
                    REPLACE(C5_RCV,',','') RCV,
                    REPLACE(C2_ACT_RECARGOS_RCV,',','') ACT_RECARGOS_RCV 
                from	DM_ADMIN.C$_0SUT_CIFRAS_SUA
                where		(1=1))    ODI_GET_FROM
                """
                print('Carga - Integracion fase 2')
                V_RSUA_NombreArchivo = txt_file_name.split('.')[0]
                select_DF = SUT_CIFRAS_SUA_DF.withColumn("NOM_ARCHIVO", lit(V_RSUA_NombreArchivo))
                select_DF = select_DF.withColumn("FEC_PROCESO", lit(FEC_PROCESO))
                select_DF = select_DF.withColumn('CIFRAS',
                                                 concat(col('tipo_registro'), col('casos'), col('cuatro_ramos'),
                                                        col('act_recargos'), col('rcv'), col('act_recargos_rcv'),
                                                        col('status')))
                select_DF = select_DF.withColumnRenamed('status', 'ESTATUS')

                select_DF = select_DF.withColumn('TIPO_REGISTRO', trim_func(col('tipo_registro')))
                select_DF = select_DF.withColumn('CASOS', replace_comma_func(col('casos')))
                select_DF = select_DF.withColumn('CUATRO_RAMOS', replace_comma_func(col('cuatro_ramos')))
                select_DF = select_DF.withColumn('ACT_RECARGOS', replace_comma_func(col('act_recargos')))
                select_DF = select_DF.withColumn('RCV', replace_comma_func(col('rcv')))
                select_DF = select_DF.withColumn('ACT_RECARGOS_RCV', replace_comma_func(col('act_recargos_rcv')))

                select_DF = select_DF.select(col('NOM_ARCHIVO'), col('FEC_PROCESO'), col('CIFRAS'),
                                             col('ESTATUS'), col('TIPO_REGISTRO'), col('CASOS'),
                                             col('CUATRO_RAMOS'), col('ACT_RECARGOS'), col('RCV'),
                                             col('ACT_RECARGOS_RCV'))

                SUT_CIFRAS_SUA_TEMP_DF = select_DF.distinct()
                # SUT_CIFRAS_SUA_TEMP_DF.show()

                # Paso 22: Obtiene archivo de comprobacion
                # OdiSqlUnload "-FILE=F:\DESCARGA_FTP\COBRANZA\RSUA\PROCESAR\CIFR_#COBRANZA.V_RSUA_NombreArchivoSalida.xls" "-DRIVER=oracle.jdbc.OracleDriver" "-URL=jdbc:oracle:thin:@172.16.8.218:1528:AFICOBRANZA" "-USER=DM_ADMIN" "-PASS=a7yaoEnnCJ0AUt1Y9Iem5v6Py" "-FILE_FORMAT=VARIABLE" "-XFIELD_SEP=09" "-ROW_SEP=\r\n" "-DATE_FORMAT=dd/mm/yyyy" "-CHARSET_ENCODING=ISO8859_1" "-XML_CHARSET_ENCODING=ISO-8859-1"

                print('> SUT_SIPA_00ENCABEZADO_LOTE')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(0) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(0) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                select_00 = SUT_SIPA_00ENCABEZADO_LOTE_18_DF.select(
                    col("Cve_Tipo_Registro").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_00 = select_00.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(0))))
                select_00 = select_00.withColumn('RCV_H', when(lit(True), lit(str(0))))
                select_00 = select_00.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_00 = select_00.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_00.show()

                print('> SUT_SIPA_01ENCABEZADO_ENT')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(0) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(0) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                select_01 = SUT_SIPA_01ENCABEZADO_ENT_DF.select(
                    col("Cve_Tipo_Registro").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_01 = select_01.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(0))))
                select_01 = select_01.withColumn('RCV_H', when(lit(True), lit(str(0))))
                select_01 = select_01.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_01 = select_01.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_01.show()

                print('> SUT_SIPA_02ENCABEZADO_PATRON')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(0) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(0) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                select_02 = SUT_SIPA_02ENCABEZADO_PATRON2.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_02 = select_02.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(0))))
                select_02 = select_02.withColumn('RCV_H', when(lit(True), lit(str(0))))
                select_02 = select_02.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_02 = select_02.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_02.show()

                print('> SUT_SIPA_03DETALLE_TRABAJADOR')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(IMP_CUOTA_FIJA_EYM + IMP_CUOTA_EXCEDENTE_EYM + IMP_PREST_DINERO_EYM + IMP_GASTOS_MEDICOS_PENSIONADOS + IMP_RIESGOS_TRABAJO + IMP_INVALIDEZ_VIDA + IMP_GUARDERIAS_PREST_SOCIALES) AS CUATRO_RAMOS_IMSS,
                # SUM(IMP_ACT_RECARGOS_4SEG_IMSS) AS ACT_Y_RECARGOS,
                # SUM(IMP_RETIRO + IMP_CES_VEJ_PAT + IMP_CES_VEJ_OBR) AS RCV,
                # SUM(IMP_ACT_RECARGOS_RETIRO + IMP_ACT_RECARGOS_CES_VEJ) AS ACT_Y_RECARGOS_RCV

                A3_10_DF = A3_10_DF.withColumn('IMP_EYM_FIJA', col('IMP_EYM_FIJA').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_EYM_EXCE', col('IMP_EYM_EXCE').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_EYM_DIN', col('IMP_EYM_DIN').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_EYM_PEN', col('IMP_EYM_PEN').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_RT', col('IMP_RT').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_IV', col('IMP_IV').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_GUAR', col('IMP_GUAR').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('IMP_ACTUALIZACION', col('IMP_ACTUALIZACION').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('TOT_RET', col('TOT_RET').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('TOT_CV_PAT', col('TOT_CV_PAT').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('TOT_CV_TRAB', col('TOT_CV_TRAB').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('TOT_ACT_REC_RET', col('TOT_ACT_REC_RET').cast('double'))
                A3_10_DF = A3_10_DF.withColumn('TOT_ACT_REC_CV', col('TOT_ACT_REC_CV').cast('double'))

                SUT_SIPA_03DETALLE_TRABAJADOR_DF = A3_10_DF.select(
                    (col('IMP_EYM_FIJA') + col('IMP_EYM_EXCE') +
                     col('IMP_EYM_DIN') + col('IMP_EYM_PEN') +
                     col('IMP_RT') + col('IMP_IV') +
                     col('IMP_GUAR')).alias('CUATRO_RAMOS_IMSS_H'),
                    (col('TOT_RET') + col('TOT_CV_PAT') + col('TOT_CV_TRAB')).alias('RCV_H'),
                    (col('TOT_ACT_REC_RET') + col('TOT_ACT_REC_CV')).alias('ACT_Y_RECARGOS_RCV_H'),
                    col('IMP_ACTUALIZACION').alias('ACT_Y_RECARGOS_H'),
                    col('TIPO_REG'), col('FEC_TRAN'), col('TIPO_ORIG')
                ).filter(col('TIPO_ORIG') == '1')
                SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.filter(
                    col('FEC_TRAN') == V_RSUA_FechaCarga)

                print(f'> V_RSUA_FechaCarga= {V_RSUA_FechaCarga}')
                print('> Antes del groupBy')
                SUT_SIPA_03DETALLE_TRABAJADOR_DF.show()
                CUATRO_RAMOS_IMSS = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("CUATRO_RAMOS_IMSS_H").alias('CUATRO_RAMOS_IMSS_H')

                print('> Despues del groupBy')
                CUATRO_RAMOS_IMSS.show()
                CUATRO_RAMOS_IMSS_TOTAL = CUATRO_RAMOS_IMSS.first()[2]

                RCV = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("RCV_H").alias('RCV_H')
                RCV_TOTAL = RCV.first()[2]

                ACT_Y_RECARGOS_RCV = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("ACT_Y_RECARGOS_RCV_H").alias('ACT_Y_RECARGOS_RCV_H')
                ACT_Y_RECARGOS_RCV_TOTAL = ACT_Y_RECARGOS_RCV.first()[2]

                ACT_Y_RECARGOS = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("ACT_Y_RECARGOS_H").alias('ACT_Y_RECARGOS_H')
                ACT_Y_RECARGOS_TOTAL = ACT_Y_RECARGOS.first()[2]

                SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF.groupBy('FEC_TRAN',
                                                                                            "TIPO_REG").count()
                SUT_SIPA_03DETALLE_TRABAJADOR_DF = SUT_SIPA_03DETALLE_TRABAJADOR_DF \
                    .withColumnRenamed('count', 'TOTAL_DE_REGISTROS')

                select_03 = SUT_SIPA_03DETALLE_TRABAJADOR_DF.select(col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                                                                    col("TOTAL_DE_REGISTROS").alias('CASOS_H'))
                select_03 = select_03.withColumn('CUATRO_RAMOS_IMSS_H',
                                                 when(lit(True), lit(str(CUATRO_RAMOS_IMSS_TOTAL))))
                select_03 = select_03.withColumn('RCV_H', when(lit(True), lit(str(RCV_TOTAL))))
                select_03 = select_03.withColumn('ACT_Y_RECARGOS_RCV_H',
                                                 when(lit(True), lit(str(ACT_Y_RECARGOS_RCV_TOTAL))))
                select_03 = select_03.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(ACT_Y_RECARGOS_TOTAL))))
                # select_03.show(1, False)

                print('> SUT_SIPA_04MOVTO_INCIDENCIA')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(0) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(0) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                select_04 = SUT_SIPA_04MOVTO_INCIDENCIA_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_04 = select_04.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(0))))
                select_04 = select_04.withColumn('RCV_H', when(lit(True), lit(str(0))))
                select_04 = select_04.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_04 = select_04.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_04.show()

                print('> SIPA_05SUMARIO_PATRON')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(IMP_CUOTA_FIJA_EYM + IMP_CUOTA_EXCEDENTE_EYM + IMP_PREST_DINERO_EYM + IMP_GASTOS_MEDICOS_PENSIONADOS + IMP_RIESGOS_TRABAJO + IMP_INVALIDEZ_VIDA + IMP_GUARDERIA_PREST_SOCIALES) AS CUATRO_RAMOS_IMSS,
                # SUM(IMP_ACT_4SEG_IMSS + IMP_RECARGOS_4SEG_IMSS) AS ACT_Y_RECARGOS,
                # SUM(IMP_RETIRO + IMP_CES_VEJ) AS RCV,
                # SUM(IMP_ACT_RETIRO_CES_VEJ + IMP_REC_RETIRO_CES_VEJ) AS ACT_Y_RECARGOS_RCV

                A5_10_DF = A5_10_DF.withColumn('IMP_EYM_CUOTA_FIJA_PAT', col('IMP_EYM_CUOTA_FIJA_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_EYM_EXCEDENTE_PAT', col('IMP_EYM_EXCEDENTE_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_EYM_DINERO_PAT', col('IMP_EYM_DINERO_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_GMP_PAT', col('IMP_GMP_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_RT_PAT', col('IMP_RT_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_IV_PAT', col('IMP_IV_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_GUAR_PAT', col('IMP_GUAR_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_ACTUALIZACION_PAT', col('IMP_ACTUALIZACION_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_REC_IMSS', col('IMP_REC_IMSS').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_RET_PAT', col('IMP_RET_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_CYV_PAT', col('IMP_CYV_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_ACT_REP_CYV_PAT', col('IMP_ACT_REP_CYV_PAT').cast('double'))
                A5_10_DF = A5_10_DF.withColumn('IMP_REC_RCV_PAT', col('IMP_REC_RCV_PAT').cast('double'))

                SIPA_05SUMARIO_PATRON = A5_10_DF.select(
                    (col('IMP_EYM_CUOTA_FIJA_PAT') + col('IMP_EYM_EXCEDENTE_PAT') +
                     col('IMP_EYM_DINERO_PAT') + col('IMP_GMP_PAT') +
                     col('IMP_RT_PAT') + col('IMP_IV_PAT') +
                     col('IMP_GUAR_PAT')).alias('CUATRO_RAMOS_IMSS_H'),
                    (col('IMP_ACTUALIZACION_PAT') + col('IMP_REC_IMSS')).alias('ACT_Y_RECARGOS_H'),
                    (col('IMP_RET_PAT') + col('IMP_CYV_PAT')).alias('RCV_H'),
                    (col('IMP_ACT_REP_CYV_PAT') + col('IMP_REC_RCV_PAT')).alias('ACT_Y_RECARGOS_RCV_H'),
                    col('TIPO_REG'), col('FEC_TRAN'), col('TIPO_ORIG')
                ).filter(col('TIPO_ORIG') == '1')
                SIPA_05SUMARIO_PATRON = SIPA_05SUMARIO_PATRON.filter(col('FEC_TRAN') == V_RSUA_FechaCarga)

                CUATRO_RAMOS_IMSS = SIPA_05SUMARIO_PATRON.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("CUATRO_RAMOS_IMSS_H").alias('CUATRO_RAMOS_IMSS_H')
                CUATRO_RAMOS_IMSS_TOTAL = CUATRO_RAMOS_IMSS.first()[2]

                RCV = SIPA_05SUMARIO_PATRON.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("RCV_H").alias('RCV_H')
                RCV_TOTAL = RCV.first()[2]

                ACT_Y_RECARGOS_RCV = SIPA_05SUMARIO_PATRON.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("ACT_Y_RECARGOS_RCV_H").alias('ACT_Y_RECARGOS_RCV_H')
                ACT_Y_RECARGOS_RCV_TOTAL = ACT_Y_RECARGOS_RCV.first()[2]

                ACT_Y_RECARGOS = SIPA_05SUMARIO_PATRON.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("ACT_Y_RECARGOS_H").alias('ACT_Y_RECARGOS_H')
                ACT_Y_RECARGOS_TOTAL = ACT_Y_RECARGOS.first()[2]

                select_05 = SIPA_05SUMARIO_PATRON_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_05 = select_05.withColumn('CUATRO_RAMOS_IMSS_H',
                                                 when(lit(True), lit(str(CUATRO_RAMOS_IMSS_TOTAL))))
                select_05 = select_05.withColumn('RCV_H', when(lit(True), lit(str(RCV_TOTAL))))
                select_05 = select_05.withColumn('ACT_Y_RECARGOS_RCV_H',
                                                 when(lit(True), lit(str(ACT_Y_RECARGOS_RCV_TOTAL))))
                select_05 = select_05.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(ACT_Y_RECARGOS_TOTAL))))
                # select_05.show()

                print('> SUT_SIPA_06VALIDACION')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(IMP_TDEP_CTA_IMSS_CUATRO) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(IMP_TDEP_CTA_IMSS_RCV) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                A6_10_DF = A6_10_DF.withColumn('TOT_DEP_IMSS', col('TOT_DEP_IMSS').cast('double'))
                A6_10_DF = A6_10_DF.withColumn('TOT_DEP_IMSS_RCV', col('TOT_DEP_IMSS_RCV').cast('double'))

                SUT_SIPA_06VALIDACION = A6_10_DF.select(
                    (col('TOT_DEP_IMSS')).alias('CUATRO_RAMOS_IMSS_H'),
                    (col('TOT_DEP_IMSS_RCV')).alias('RCV_H'),
                    col('TIPO_REG'), col('FEC_TRAN'), col('TIPO_ORIG')
                ).filter(col('TIPO_ORIG') == '1')
                SUT_SIPA_06VALIDACION = SUT_SIPA_06VALIDACION.filter(col('FEC_TRAN') == V_RSUA_FechaCarga)

                CUATRO_RAMOS_IMSS = SUT_SIPA_06VALIDACION.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("CUATRO_RAMOS_IMSS_H").alias('CUATRO_RAMOS_IMSS_H')
                CUATRO_RAMOS_IMSS_TOTAL = CUATRO_RAMOS_IMSS.first()[2]

                RCV = SUT_SIPA_06VALIDACION.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("RCV_H").alias('RCV_H')
                RCV_TOTAL = RCV.first()[2]

                select_06 = SUT_SIPA_06VALIDACION_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_06 = select_06.withColumn('CUATRO_RAMOS_IMSS_H',
                                                 when(lit(True), lit(str(CUATRO_RAMOS_IMSS_TOTAL))))
                select_06 = select_06.withColumn('RCV_H', when(lit(True), lit(str(RCV_TOTAL))))
                select_06 = select_06.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_06 = select_06.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_06.show()

                print('> SUT_SIPA_07TRANSAC_VENT_SUA')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(IMP_4SEG_IMSS) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(IMP_RETIRO_CES_VEJ) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                A7_10_DF = SUT_SIPA_07_DF.withColumn('IMP_IMSS', col('IMP_IMSS').cast('double'))
                A7_10_DF = A7_10_DF.withColumn('IMP_RCV', col('IMP_RCV').cast('double'))

                SUT_SIPA_07TRANSAC_VENT_SUA = A7_10_DF.select(
                    (col('IMP_IMSS')).alias('CUATRO_RAMOS_IMSS_H'),
                    (col('IMP_RCV')).alias('RCV_H'),
                    col('TIPO_REG'), col('FEC_TRAN')
                ).filter(col('FEC_TRAN') == V_RSUA_FechaCarga)

                CUATRO_RAMOS_IMSS = SUT_SIPA_07TRANSAC_VENT_SUA.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("CUATRO_RAMOS_IMSS_H").alias('CUATRO_RAMOS_IMSS_H')
                CUATRO_RAMOS_IMSS_TOTAL = CUATRO_RAMOS_IMSS.first()[2]

                RCV = SUT_SIPA_07TRANSAC_VENT_SUA.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("RCV_H").alias('RCV_H')
                RCV_TOTAL = RCV.first()[2]

                select_07 = SUT_SIPA_07TRANSAC_VENT_SUA_DF.select(
                    col("Cve_Tipo_Registro").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_07 = select_07.withColumn('CUATRO_RAMOS_IMSS_H',
                                                 when(lit(True), lit(str(CUATRO_RAMOS_IMSS_TOTAL))))
                select_07 = select_07.withColumn('RCV_H', when(lit(True), lit(str(RCV_TOTAL))))
                select_07 = select_07.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_07 = select_07.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_07.show()

                # ***035308 falta
                # SELECT
                #    '08' CVE_TIPO_REGISTRO,
                #    0 AS CASOS,
                #    0 AS CUATRO_RAMOS_IMSS,
                #    0 AS ACT_Y_RECARGOS,
                #    0 AS RCV,
                #    0 AS ACT_Y_RECARGOS_RCV
                # FROM DUAL

                print('> SUT_SIPA_09SUMARIO_TOT')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(Tot_4Seg_Imss_Regs_07_Fec_Pago) + SUM(Tot_4Seg_Imss_Regs_08_Fec_Pago) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(Tot_Rcv_Regs_07_Fec_Pago) + SUM(Tot_Rcv_Regs_08_Fec_Pago) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                A9_10_DF = SUT_SIPA_09_DF.withColumn('TOT_IMSS_R07', col('TOT_IMSS_R07').cast('double'))
                A9_10_DF = A9_10_DF.withColumn('TOT_IMSS_R08', col('TOT_IMSS_R08').cast('double'))
                A9_10_DF = A9_10_DF.withColumn('TOT_RCV_R07', col('TOT_RCV_R07').cast('double'))
                A9_10_DF = A9_10_DF.withColumn('TOT_RCV_R08', col('TOT_RCV_R08').cast('double'))

                print('> Antes del select  A9_10_DF')
                print(f'> V_RSUA_FechaCarga= {V_RSUA_FechaCarga}')
                A9_10_DF.show(10, False)
                A9_10_DF = A9_10_DF.select(
                    col('TOT_IMSS_R07'),
                    col('TOT_IMSS_R08'),
                    col('TOT_RCV_R07'),
                    col('TOT_RCV_R08'),
                    col('TIPO_REG'), col('FEC_TRAN')
                ).filter(col('FEC_TRAN') == V_RSUA_FechaCarga)
                print('> Despues del select  A9_10_DF')
                A9_10_DF.show(10, False)

                aux1 = A9_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R07").alias('TOT_IMSS_R07')
                aux1_tot = aux1.first()[2]

                aux2 = A9_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R08").alias('TOT_IMSS_R08')
                aux2_tot = aux2.first()[2]

                aux3 = A9_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R07").alias('TOT_RCV_R07')
                aux3_tot = aux3.first()[2]

                aux4 = A9_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R08").alias('TOT_RCV_R08')
                aux4_tot = aux4.first()[2]

                select_09 = SUT_SIPA_09_TEMP_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_09 = select_09.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(aux1_tot + aux2_tot))))
                select_09 = select_09.withColumn('RCV_H', when(lit(True), lit(str(aux3_tot + aux4_tot))))
                select_09 = select_09.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_09 = select_09.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_09.show()

                print('> SUT_SIPA_10SUM_LOTE_PAGOS_ER')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(Tot_4Seg_Imss_Regs_07_Er) + SUM(Tot_4Seg_Imss_Regs_08_Er) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(Tot_Rcv_Regs_07_Er) + SUM(Tot_Rcv_Regs_08_Er) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                A10_10_DF = SUT_SIPA_10_DF.withColumn('TOT_IMSS_R07', col('TOT_IMSS_R07').cast('double'))
                A10_10_DF = A10_10_DF.withColumn('TOT_IMSS_R08', col('TOT_IMSS_R08').cast('double'))
                A10_10_DF = A10_10_DF.withColumn('TOT_RCV_R07', col('TOT_RCV_R07').cast('double'))
                A10_10_DF = A10_10_DF.withColumn('TOT_RCV_R08', col('TOT_RCV_R08').cast('double'))

                A10_10_DF = A10_10_DF.select(
                    col('TOT_IMSS_R07'),
                    col('TOT_IMSS_R08'),
                    col('TOT_RCV_R07'),
                    col('TOT_RCV_R08'),
                    col('TIPO_REG'), col('FEC_TRAN')
                ).filter(col('FEC_TRAN') == V_RSUA_FechaCarga)

                aux1 = A10_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R07").alias('TOT_IMSS_R07')
                aux1_tot = aux1.first()[2]

                aux2 = A10_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R08").alias('TOT_IMSS_R08')
                aux2_tot = aux2.first()[2]

                aux3 = A10_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R07").alias('TOT_RCV_R07')
                aux3_tot = aux3.first()[2]

                aux4 = A10_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R08").alias('TOT_RCV_R08')
                aux4_tot = aux4.first()[2]

                select_10 = SUT_SIPA_10_TEMP_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_10 = select_10.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(aux1_tot + aux2_tot))))
                select_10 = select_10.withColumn('RCV_H', when(lit(True), lit(str(aux3_tot + aux4_tot))))
                select_10 = select_10.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_10 = select_10.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                # select_10.show()

                print('> SUT_SIPA_11SUM_LOTE_NOT')
                # Cve_Tipo_Registro,
                # COUNT(1) AS CASOS,
                # SUM(Tot_4Seg_Imss_Regs_07) + SUM(Tot_4Seg_Imss_Regs_08) AS CUATRO_RAMOS_IMSS,
                # SUM(0) AS ACT_Y_RECARGOS,
                # SUM(Tot_Rcv_Regs_07) + SUM(Tot_Rcv_Regs_08) AS RCV,
                # SUM(0) AS ACT_Y_RECARGOS_RCV

                A11_10_DF = SUT_SIPA_11SUM_DF.withColumn('TOT_IMSS_R07', col('TOT_IMSS_R07').cast('double'))
                A11_10_DF = A11_10_DF.withColumn('TOT_IMSS_R08', col('TOT_IMSS_R08').cast('double'))
                A11_10_DF = A11_10_DF.withColumn('TOT_RCV_R07', col('TOT_RCV_R07').cast('double'))
                A11_10_DF = A11_10_DF.withColumn('TOT_RCV_R08', col('TOT_RCV_R08').cast('double'))

                A11_10_DF = A11_10_DF.select(
                    col('TOT_IMSS_R07'),
                    col('TOT_IMSS_R08'),
                    col('TOT_RCV_R07'),
                    col('TOT_RCV_R08'),
                    col('TIPO_REG'), col('FEC_TRAN')
                ).filter(col('FEC_TRAN') == V_RSUA_FechaCarga)

                aux1 = A11_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R07").alias('TOT_IMSS_R07')
                aux1_tot = aux1.first()[2]

                aux2 = A11_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_IMSS_R08").alias('TOT_IMSS_R08')
                aux2_tot = aux2.first()[2]

                aux3 = A11_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R07").alias('TOT_RCV_R07')
                aux3_tot = aux3.first()[2]

                aux4 = A11_10_DF.groupBy('FEC_TRAN', "TIPO_REG") \
                    .sum("TOT_RCV_R08").alias('TOT_RCV_R08')
                aux4_tot = aux4.first()[2]

                select_11 = SUT_SIPA_11SUM_TEMP_DF.select(
                    col("TIPO_REG").alias('CVE_TIPO_REGISTRO'),
                    col("TOTAL_DE_REGISTROS").alias('CASOS_H')
                )
                select_11 = select_11.withColumn('CUATRO_RAMOS_IMSS_H', when(lit(True), lit(str(aux1_tot + aux2_tot))))
                select_11 = select_11.withColumn('RCV_H', when(lit(True), lit(str(aux3_tot + aux4_tot))))
                select_11 = select_11.withColumn('ACT_Y_RECARGOS_RCV_H', when(lit(True), lit(str(0))))
                select_11 = select_11.withColumn('ACT_Y_RECARGOS_H', when(lit(True), lit(str(0))))
                select_11.show()

                print('> Paso Union ')
                H_UNION_DF = select_00.unionAll(select_01).unionAll(select_02).unionAll(select_03) \
                    .unionAll(select_04).unionAll(select_05).unionAll(select_06).unionAll(select_07) \
                    .unionAll(select_09).unionAll(select_10).unionAll(select_11)

                # H_UNION_DF.show(3, False)

                print('> SUT_CIFRAS_SUA fase 1')
                # REPLACE(TIPO_REGISTRO,'0353','') CLAVE,
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA_DF.withColumn('CLAVE_SUT_CIFRAS_SUA',
                                                              special_func_replace_0353(col('tipo_registro')))
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA.withColumn('casos', col('casos'))
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA.withColumn('cuatro_ramos', col('cuatro_ramos'))
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA.withColumn('act_recargos', col('act_recargos'))
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA.withColumn('rcv', col('rcv'))
                SUT_CIFRAS_SUA = SUT_CIFRAS_SUA.withColumn('act_recargos_rcv', col('act_recargos_rcv'))

                # SUT_CIFRAS_SUA.show(5, False)

                print('> H_UNION_CIFRAS_JOIN')
                H_UNION_CIFRAS_JOIN = H_UNION_DF.join(SUT_CIFRAS_SUA, (
                        H_UNION_DF['CVE_TIPO_REGISTRO'] == SUT_CIFRAS_SUA['CLAVE_SUT_CIFRAS_SUA']))
                # H_UNION_CIFRAS_JOIN.orderBy(col("CLAVE_SUT_CIFRAS_SUA").asc())

                # H_UNION_CIFRAS_JOIN.show(3, False)

                L_DF = H_UNION_CIFRAS_JOIN.select(col('CVE_TIPO_REGISTRO'), col('CASOS_H'), col('casos'),
                                                  col('CUATRO_RAMOS_IMSS_H'),
                                                  col('cuatro_ramos'), col('ACT_Y_RECARGOS_H'),
                                                  col('act_recargos'),
                                                  col('RCV_H'),
                                                  col('rcv'), col('ACT_Y_RECARGOS_RCV_H'),
                                                  col('act_recargos_rcv'))

                # print('> Paso Union L DUAL')
                # L_DUAL_UNION_DF = L_DF.unionAll(DUAL_DF)

                L_DF = L_DF.select(
                    col('CVE_TIPO_REGISTRO').alias('A'), col('CASOS_H').alias('B'), col('casos').alias('C'),
                    col('CUATRO_RAMOS_IMSS_H').alias('D'),
                    col('cuatro_ramos').alias('E'), col('ACT_Y_RECARGOS_H').alias('F'),
                    col('act_recargos').alias('G'),
                    col('RCV_H').alias('H'),
                    col('rcv').alias('I'), col('ACT_Y_RECARGOS_RCV_H').alias('J'),
                    col('act_recargos_rcv').alias('K'))

                L_DF.show(3, truncate=False)

                # FINAL_DF.write.csv("-FILE=F:\DESCARGA_FTP\COBRANZA\RSUA\PROCESAR\CIFR_#COBRANZA.V_RSUA_NombreArchivoSalida.xls")
                LPATH_SALIDA=f'{PATH_LOCAL_TEMP_LA}/{V_RSUA_NombreArchivoSalida}_SALIDA'
                #L_DF.write.csv(LPATH_SALIDA)
                L_DF.coalesce(1).write.format('csv').option('header', 'false') \
                    .mode('overwrite').option('sep', '|').save(f'{LPATH_SALIDA}')

                for dirOrFile in os.listdir(f'{LPATH_SALIDA}'):
                    if dirOrFile.endswith(('.csv', '.CSV')):
                        csv_file_path = f'{LPATH_SALIDA}/{dirOrFile}'




                # Paso 23 V_RSUA_ValidaTotalReg
                """
                select count(1) from (
                SELECT TO_NUMBER(A.Cve_Tipo_Registro) as Cve_Tipo_Registro, TO_NUMBER(A.TOTAL_DE_REGISTROS) as TOTAL_DE_REGISTROS
                FROM (SELECT 'ENCABEZADO LOTE', Cve_Tipo_Registro, COUNT(*)  AS TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_00ENCABEZADO_LOTE
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')   
                            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
                            UNION ALL
                            SELECT 'ENCABEZADO ENTIDAD', Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_01ENCABEZADO_ENT
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')  
                            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
                            UNION ALL
                            SELECT 'ENCABEZADO PATRON', cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_02ENCABEZADO_PATRON
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY')
                            AND TIPO_ORIG = 1  
                            GROUP BY FEC_TRANSFERENCIA, cve_Tipo_Registro
                            UNION ALL
                            SELECT 'DETALLE_TRABAJADOR', Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS 
                            FROM SUT_SIPA_03DETALLE_TRABAJADOR
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            AND TIPO_ORIG = 1
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'MOVIMIENTOS INCIDENCIAS', Cve_Tipo_Registro, COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_04MOVTO_INCIDENCIA
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            AND TIPO_ORIG = 1
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO PATRON',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS 
                            FROM SUT_SIPA_05SUMARIO_PATRON
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            AND TIPO_ORIG = 1
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'VALIDACION',Cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS 
                            FROM SUT_SIPA_06VALIDACION
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            AND TIPO_ORIG = 1
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'TRANSAC VENTANILLA SUA', cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_07TRANSAC_VENT_SUA
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO_TOTALIZACION',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_09SUMARIO_TOT
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO LOTE PAGOS ENTIDAD RECEPTORA',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_10SUM_LOTE_PAGOS_ER
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO LOTE NOTIFICACION',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS
                            FROM SUT_SIPA_11SUM_LOTE_NOT
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro) A
                WHERE TO_NUMBER(A.Cve_Tipo_Registro) not in (4,8)          
                MINUS
                SELECT to_number(SUBSTR(REGEXP_SUBSTR(CIFRAS,'[^  ]+',1,1),5,2)) CVE_TIPO_REGISTRO,
                             TO_NUMBER(REPLACE(REGEXP_SUBSTR(CIFRAS,'[^  ]+',2,2),',','')) TOTAL_DE_REGISTROS
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',3,3) IMPORTE_CUOTAS_IMSS,
                --            REGEXP_SUBSTR(CIFRAS,'[^  ]+',4,4) IMPORTE_RCV,
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',5,5) IMPORTE_RCV
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',6,6), 
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',7,7)
                FROM DM_ADMIN.SUT_CIFRAS_SUA
                WHERE SUBSTR(REGEXP_SUBSTR(CIFRAS,'[^  ]+',1,1),5,2) not in (4,8)
                AND NOM_ARCHIVO = '#COBRANZA.V_RSUA_NombreArchivo'  
                ORDER BY 1)
                        """
                print('> V_RSUA_ValidaTotalReg fase 1')
                select1_DF = totales_DF.select(col('Cve_Tipo_Registro'), col('TOTAL_DE_REGISTROS'))
                # select1_DF.show()
                print('> V_RSUA_ValidaTotalReg fase 2')

                select2_DF = SUT_CIFRAS_SUA_DF.withColumn('Cve_Tipo_Registro', substring(col('tipo_registro'), 5, 2))
                select2_DF = select2_DF.select(col('Cve_Tipo_Registro'), col('casos')).filter(
                    "Cve_Tipo_Registro NOT IN ('04','08')")

                select2_DF = select2_DF.withColumnRenamed('casos', 'TOTAL_DE_REGISTROS')
                # select2_DF.show()
                print('> V_RSUA_ValidaTotalReg fase 3')
                minus_DF = select1_DF.subtract(select2_DF)
                # minus_DF.show()
                rows = minus_DF.count()
                print(f' count minus: {rows}')

                err = f'[error] {PATH_LOCAL_TEMP_LA}/sut_cifras_sua'
                #print(f'{err}')
                # if falla enviar error:
                # SUBJECT=ERROR - RSUA - Validar Cifras Control
                # CONTENIDO:
                email_content = f'Buen día, Se proceso el archivo #V_RSUA_NombreArchivo, ' \
                                'pero no cuadró la revision de cifras en cuanto a totales de registros, favor de validar. ' \
                                'Saludos'
                # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

                # to_email_list = 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

                # Adjuntar archivo
                if rows !=0:
                 print('correo paso 23')
                 email_content = f'Buen día, Se proceso el archivo {V_RSUA_NombreArchivoSalida}, ' \
                                'pero no cuadró la revision de cifras en cuanto a totales de registros, favor de validar. ' \
                                'Saludos'
                 appTools().email_sender(email_content=email_content,
                                        to_email_list=TO_EMAIL_LIST,
                                        subject='ERROR - RSUA - Validar Cifras de control')

                # Paso 24: V_RSUA_ValidaTotalReg -> pass

                # Paso 25 V_RSUA_ValidaTotalImp
                """
                SELECT COUNT (1) FROM (            
                SELECT TO_NUMBER(A.CVE_TIPO_REGISTRO), TO_NUMBER(A.IMPORTE_CUOTAS_IMSS), TO_NUMBER(A.IMPORTE_RCV)            
                FROM (SELECT 'SUMARIO_TOTALIZACION',cve_Tipo_Registro,COUNT(*), 
                            SUM(Tot_4Seg_Imss_Regs_07_Fec_Pago) +
                            SUM(Tot_4Seg_Imss_Regs_08_Fec_Pago) AS IMPORTE_CUOTAS_IMSS,
                            SUM(Tot_Rcv_Regs_07_Fec_Pago) +
                            SUM(Tot_Rcv_Regs_08_Fec_Pago) AS IMPORTE_RCV, FEC_TRANSFERENCIA
                            FROM SUT_SIPA_09SUMARIO_TOT
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO LOTE PAGOS ENTIDAD RECEPTORA',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS,
                            SUM(Tot_4Seg_Imss_Regs_07_Er) +
                            SUM(Tot_4Seg_Imss_Regs_08_Er) AS IMPORTE_CUOTAS_IMSS,
                            SUM(Tot_Rcv_Regs_07_Er) +
                            SUM(Tot_Rcv_Regs_08_Er) AS IMPORTE_RCV, FEC_TRANSFERENCIA
                            FROM SUT_SIPA_10SUM_LOTE_PAGOS_ER
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro
                            UNION ALL
                            SELECT 'SUMARIO LOTE NOTIFICACION',cve_Tipo_Registro,COUNT(*) TOTAL_DE_REGISTROS,
                            SUM(Tot_4Seg_Imss_Regs_07) +
                            SUM(Tot_4Seg_Imss_Regs_08) AS IMPORTE_CUOTAS_IMSS,
                            SUM(Tot_Rcv_Regs_07) +
                            SUM(Tot_Rcv_Regs_08) AS IMPORTE_RCV, FEC_TRANSFERENCIA
                            FROM SUT_SIPA_11SUM_LOTE_NOT
                            WHERE FEC_TRANSFERENCIA =  TO_DATE('#COBRANZA.V_RSUA_FechaCarga', 'DD/MM/YYYY') 
                            GROUP BY FEC_TRANSFERENCIA, Cve_Tipo_Registro) A
                MINUS       
                SELECT TO_NUMBER(SUBSTR(REGEXP_SUBSTR(CIFRAS,'[^  ]+',1,1),5,2)) CVE_TIPO_REGISTRO,
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',2,2) TOTAL_DE_REGISTROS,
                            TO_NUMBER(REPLACE(REGEXP_SUBSTR(CIFRAS,'[^  ]+',3,3),',','')) IMPORTE_CUOTAS_IMSS,
                --            REGEXP_SUBSTR(CIFRAS,'[^  ]+',4,4) IMPORTE_RCV,
                            TO_NUMBER(REPLACE(REGEXP_SUBSTR(CIFRAS,'[^  ]+',5,5),',','')) IMPORTE_RCV
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',6,6), 
                            --REGEXP_SUBSTR(CIFRAS,'[^  ]+',7,7)
                FROM DM_ADMIN.SUT_CIFRAS_SUA
                WHERE SUBSTR(REGEXP_SUBSTR(CIFRAS,'[^  ]+',1,1),5,2) IN  (9,10,11)
                AND NOM_ARCHIVO = '#COBRANZA.V_RSUA_NombreArchivo'  
                ORDER BY 1)
                """
                print('> V_RSUA_ValidaTotalImp fase 1')
                SUT_SIPA_09_TEMP_DF = SUT_SIPA_09_DF.select(col('TIPO_REG'),
                                                            (col('TOT_IMSS_R07') + col('TOT_IMSS_R08')).alias(
                                                                'SUM_IMPORTE_CUOTAS_IMSS')
                                                            , (col('TOT_RCV_R07') + col('TOT_RCV_R08')).alias(
                        'SUM_IMPORTE_RCV'), col('FEC_TRAN')).filter(
                    col('FEC_TRAN') == V_RSUA_FechaCarga)

                SUT_SIPA_09_TEMP_01_DF = SUT_SIPA_09_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum(
                    "SUM_IMPORTE_CUOTAS_IMSS").alias('IMPORTE_CUOTAS_IMSS')

                TOTAL_01 = SUT_SIPA_09_TEMP_01_DF.first()[2]
                TOTAL_01 = round(TOTAL_01, 2)
                TOTAL_01 = str(TOTAL_01)
                print(f'total01:{TOTAL_01}')

                SUT_SIPA_09_TEMP_02_DF = SUT_SIPA_09_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum("SUM_IMPORTE_RCV")
                TOTAL_02 = SUT_SIPA_09_TEMP_02_DF.first()[2]
                TOTAL_02 = round(TOTAL_02, 2)
                TOTAL_02 = str(TOTAL_02)
                print(f'total02:{TOTAL_02}')

                select_01_df = SUT_SIPA_09_TEMP_01_DF.select(col("TIPO_REG"))
                select_01_df = select_01_df.withColumn('IMPORTE_CUOTAS_IMSS', lit(TOTAL_01))
                select_01_df = select_01_df.withColumn('IMPORTE_RCV', lit(TOTAL_02))
                select_01_df = select_01_df.withColumnRenamed('CVE_TIPO_REGISTRO', 'TIPO_REG')
                # select_01_df.show()
                print('> V_RSUA_ValidaTotalImp fase 2')

                SUT_SIPA_10_TEMP_DF = SUT_SIPA_10_DF.select(col('TIPO_REG'),
                                                            (col('TOT_IMSS_R07') + col('TOT_IMSS_R08')).alias(
                                                                'SUM_IMPORTE_CUOTAS_IMSS')
                                                            , (col('TOT_RCV_R07') + col('TOT_RCV_R08')).alias(
                        'SUM_IMPORTE_RCV'), col('FEC_TRAN')).filter(
                    col('FEC_TRAN') == V_RSUA_FechaCarga)

                SUT_SIPA_10_TEMP_01_DF = SUT_SIPA_10_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum(
                    "SUM_IMPORTE_CUOTAS_IMSS").alias('IMPORTE_CUOTAS_IMSS')

                TOTAL_01 = SUT_SIPA_10_TEMP_01_DF.first()[2]
                TOTAL_01 = round(TOTAL_01, 2)
                TOTAL_01 = str(TOTAL_01)
                print(f'total01: {TOTAL_01}')

                SUT_SIPA_10_TEMP_02_DF = SUT_SIPA_10_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum("SUM_IMPORTE_RCV")
                TOTAL_02 = SUT_SIPA_10_TEMP_02_DF.first()[2]
                TOTAL_02 = round(TOTAL_02, 2)
                TOTAL_02 = str(TOTAL_02)
                print(f'total02: {TOTAL_02}')

                select_02_df = SUT_SIPA_10_TEMP_01_DF.select(col("TIPO_REG"))
                select_02_df = select_02_df.withColumn('IMPORTE_CUOTAS_IMSS', lit(TOTAL_01))
                select_02_df = select_02_df.withColumn('IMPORTE_RCV', lit(TOTAL_02))
                select_02_df = select_02_df.withColumnRenamed('CVE_TIPO_REGISTRO', 'TIPO_REG')
                # select_02_df.show()

                print('> V_RSUA_ValidaTotalImp fase 3')
                SUT_SIPA_11SUM_TEMP_DF = SUT_SIPA_11SUM_DF.select(col('TIPO_REG'),
                                                                  (col('TOT_IMSS_R07') + col('TOT_IMSS_R08')).alias(
                                                                      'SUM_IMPORTE_CUOTAS_IMSS')
                                                                  ,
                                                                  (col('TOT_RCV_R07') + col('TOT_RCV_R08')).alias(
                                                                      'SUM_IMPORTE_RCV'),
                                                                  col('FEC_TRAN')).filter(
                    col('FEC_TRAN') == V_RSUA_FechaCarga)
                # SUT_SIPA_11SUM_TEMP_DF.show()

                SUT_SIPA_11SUM_TEMP_01_DF = SUT_SIPA_11SUM_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum(
                    "SUM_IMPORTE_CUOTAS_IMSS").alias('IMPORTE_CUOTAS_IMSS')

                TOTAL_01 = SUT_SIPA_11SUM_TEMP_01_DF.first()[2]
                TOTAL_01 = round(TOTAL_01, 2)
                TOTAL_01 = str(TOTAL_01)
                print(f'total01:{TOTAL_01}')

                SUT_SIPA_11SUM_TEMP_02_DF = SUT_SIPA_11SUM_TEMP_DF.groupBy('FEC_TRAN', "TIPO_REG").sum(
                    "SUM_IMPORTE_RCV")
                TOTAL_02 = SUT_SIPA_11SUM_TEMP_02_DF.first()[2]
                TOTAL_02 = round(TOTAL_02, 2)
                TOTAL_02 = str(TOTAL_02)
                print(f'total02:{TOTAL_02}')

                select_03_df = SUT_SIPA_11SUM_TEMP_01_DF.select(col("TIPO_REG"))
                select_03_df = select_03_df.withColumn('IMPORTE_CUOTAS_IMSS', lit(TOTAL_01))
                select_03_df = select_03_df.withColumn('IMPORTE_RCV', lit(TOTAL_02))
                # select_03_df.show()

                print('> V_RSUA_ValidaTotalImp fase 4')
                temp_DF = select_01_df.unionAll(select_02_df).unionAll(select_03_df)
                temp_DF.withColumnRenamed('TIPO_REG', 'Cve_Tipo_Registro')
                # temp_DF.show()

                print('> V_RSUA_ValidaTotalImp fase 5')
                select2_DF = SUT_CIFRAS_SUA_DF.withColumn('Cve_Tipo_Registro', substring(col('tipo_registro'), 5, 2))
                select2_DF = select2_DF.select(col('Cve_Tipo_Registro'), col('cuatro_ramos'), col('rcv')).filter(
                    "Cve_Tipo_Registro IN ('09','10','11')")

                select2_DF = select2_DF.withColumnRenamed('cuatro_ramos', 'IMPORTE_CUOTAS_IMSS')
                select2_DF = select2_DF.withColumnRenamed('rcv', 'IMPORTE_RCV')
                # select2_DF.show()

                print('> V_RSUA_ValidaTotalImp fase 6')
                minus_DF = temp_DF.subtract(select2_DF)
                # minus_DF.show()
                minus_rows = minus_DF.count()

                print(f'count minus: {minus_rows}')
                # SUT_CIFRAS_SUA_TEMP_DF = SUT_CIFRAS_SUA_TEMP_DF\
                #    .filter(col('CVE_TIPO_REGISTRO') != '04' & col('CVE_TIPO_REGISTRO') != '08')

                if minus_rows != 0:
                    err = f'[error] {PATH_LOCAL_TEMP_LA}/sut_cifras_sua'
                    #print(f'{err}')
                    # if falla enviar error:
                    # SUBJECT=ERROR - RSUA - Validar Cifras Control
                    # CONTENIDO:

                    # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'

                    # to_email_list = 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'
                    email_content = f'Buen día, Se procesó el archivo {V_RSUA_NombreArchivoSalida}, pero no cuadró la ' \
                                    f'revision de cifras, favor de validar. ' \
                                    f'Se adjunta archivo de cifras como evidencia. Saludos'
                    appTools().email_sender(email_content=email_content,
                                            to_email_list=TO_EMAIL_LIST,
                                            subject='ERROR - RSUA - Validar Cifras de control',file=csv_file_path)
                else:
                    err = f'[error] {PATH_LOCAL_TEMP_LA}/sut_cifras_sua'
                    print(f'{err}')
                    # if falla enviar error:
                    # SUBJECT=ERROR - RSUA - Validar Cifras Control
                    # CONTENIDO:
                    email_content = f'Buen día, El proceso RSUA #V_RSUA_FechaCarga se ejecutó correctamente para la ' \
                                    f'carga del archivo #V_RSUA_NombreArchivo. Se adjunta archivo de validación.'
                    # TO: 'susana.apaseo@imss.gob.mx, brenda.corona@imss.gob.mx, guillermo.acosta@imss.gob.mx'
                    print(f'archivo salida{LPATH_SALIDA}')
                    email_content = f'Buen día, El proceso RSUA {V_RSUA_FechaCarga} se ejecutó correctamente para la ' \
                                    f'carga del archivo {V_RSUA_NombreArchivoSalida}. Se adjunta archivo de validación.'
                    appTools().email_sender(email_content=email_content,
                                        to_email_list=TO_EMAIL_LIST,
                                        subject='ERROR - RSUA - Validar Cifras de control',
                                        file=csv_file_path)
            else:
                print(f'> Non-compatible TXT files')

        # Paso 28
        """
        INSERT INTO D_BITACORA_DMART_DSTD (FEC_ARCHIVO, FEC_CARGA_DM, CVE_TABLADM, CVE_CARGA, NOM_ARCHIVO, REG_TOTALES, 
        CVE_ESTATUSCARGA, CVE_ORIGEN, TIPO_CARGA) 
        VALUES (TO_DATE(to_date('#COBRANZA.V_RSUA_FechaCarga'),'yyyy/mm/dd'), TRUNC(SYSDATE), 
        COBRANZA.V_RSUA_NombreProceso, COBRANZA.V_RSUA_TipoEjecucion, COBRANZA.V_RSUA_NombreArchivo', 
        COBRANZA.V_RSUA_RegistrosCargadosTMP, COBRANZA.V_RSUA_TipoEjecucion, 1, '#COBRANZA.V_RSUA_PeriodoEjecucion');
        """

        """
        1 fec_archivo = V_RSUA_FechaCarga
        2 fec_carga_dm = SYSDATE
        3 cve_tabladm = V_RSUA_NombreProceso = 20
        4 cve_carga	= V_RSUA_TipoEjecucion
        5 nom_archivo = V_RSUA_NombreArchivo
        6 reg_totales = V_RSUA_RegistrosCargadosTMP
        7 reg_insertados = V_RSUA_RegistrosCargadosTMP
        8 reg_actualizados = 0
        9 reg_borrados = 0
        10 reg_descartados = 0
        11 cve_estatuscarga	= 1
        12 cve_origen = 1
        13 tipo_carga = V_RSUA_PeriodoEjecucion
        14 reg_ignorados Null
        15 futuro1 Null
        16 futuro2 Null
        17 futuro3 Null
        18 futuro4 Null
        19 futuro5 Null
        20 futuro6 Null
        21 futuro7 Null
        """
        SYS_DATE = dt_now.strftime('%Y-%m-%d %H:%M:%S.%f')
        V_RSUA_NombreProceso = '20'

        """
        SELECT count(1)  from SUT_SIPA_00ENCABEZADO_LOTE
        where fec_transferencia = TO_DATE('#V_RSUA_FechaCarga','dd/mm/yyyy')
        """

        table_name = 'te_aficobranza.d_bitacora_dmart_dstd'
        insert_data_query = f"insert into {table_name} values ('{V_RSUA_FechaCarga}', " \
                            f"'{SYS_DATE}', {V_RSUA_NombreProceso}, '{V_RSUA_TipoEjecucion}', " \
                            f"'{V_RSUA_NombreArchivoSalida}', '{V_RSUA_RegistrosCargados}', " \
                            f"'{V_RSUA_RegistrosCargados}', '0', '0', " \
                            f"'0', '1', '1', '{V_RSUA_TipoEjecucion}', '', '', '', '', " \
                            f"'', '', '', '')"
        try:
            print(f'> Query to Hive: {insert_data_query}')
            status_query_hive = Hive().exec_query(insert_data_query)
            if not status_query_hive:
                error_id = '3.1'
                error_description = f'Hive query: {insert_data_query}'
            else:
                error_id = None
        except Exception as e:
            error_id, error_description = appTools().get_error(error=e)
        finally:
            if error_id:
                print('> Sending [error] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id=error_id,
                                                      error_description=error_description,
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)
            else:
                print('> Sending [ok] status to cifras_control...')
                ctrl_cfrs = appTools().cifras_control(db_table_name=table_name,
                                                      start_datetime=self.start_datetime,
                                                      start_datetime_proc=self.start_datetime_proc,
                                                      end_datetime_proc=appTools().get_datime_now(),
                                                      error_id='0',
                                                      error_description='',
                                                      process_name=process_name)
                appTools().error_logger(ctrl_cif=ctrl_cfrs)

        elapsed_seconds = round(time.time() - self.start_datetime_etl, 3)
        print(f'[ok] appEtl {appTools().get_proc_etl_name()} for \033[93m{self.C2_NOMBRE_ARCHIVO_v}\033[0m '
              f'process executed on {elapsed_seconds}sec.')
