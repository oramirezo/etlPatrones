import os
import subprocess
import sys
import time
from core.appTools import appTools
from core.appEtl import etl
from core.systemPerformance import systemPerformance
from core.constants import SYSTEM_PERFORMANCE_MONITOR_CONTROL, SYSTEM_RUNTIME_PERIOD_H, SYSTEM_PERFORMANCE_PERIOD

if __name__ == '__main__':
    start_datetime_etl = time.time()
    start_datetime = appTools().get_datime_now()
    print(start_datetime)

    os_running = appTools().get_os_system()[0]
    if os_running == 'windows':
        from core.constants import PATH_LOCAL_TEMP_LA_WIN as PATH_LOCAL_TEMP_LA
    else:
        from core.constants import PATH_LOCAL_TEMP_LA

    process_name = appTools().get_proc_etl_name()
    print(f'> Starting process \033[95m{process_name}\033[0m...')
    start_datetime_proc = appTools().get_datime_now()

    if os_running != 'windows':
        try:
            print('> Ingesting files from LA stage...')
            command = f'chmod +x py-extract-la.sh && ./py-extract-la.sh'
            print(f'> exec command: {command}')
            subprocess.run([command], shell=True, check=True)
        except subprocess.CalledProcessError as e:
            e = f' Ingesting files from LA stage. {e}'
            print(f'[error] {e}')

    # Paso 0: ORACLE_AFICOBRANZA -> Pass

    # Paso 1: Proc. Lee archivos planos
    C1_PROYECTO_v = 'RSUA'
    C3_ESTATUS_v = '0'
    C2_NOMBRE_ARCHIVO_LIST = []
    try:
        local_path_content = os.listdir(PATH_LOCAL_TEMP_LA)
        for dirOrFile in local_path_content:
            if dirOrFile.endswith(('.csv', '.CSV')):
                C2_NOMBRE_ARCHIVO_LIST.append(dirOrFile)
    except Exception as e:
        print(f'[error] File .csv validation. {e}')
        C2_NOMBRE_ARCHIVO_LIST = []
    if not C2_NOMBRE_ARCHIVO_LIST:
        e = f'Any compatible file: {PATH_LOCAL_TEMP_LA}'
        print(f'[error] {e}')
        error_id = '3.2'
        error_description = e
        ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                              start_datetime=start_datetime,
                                              start_datetime_proc=start_datetime_proc,
                                              end_datetime_proc=appTools().get_datime_now(),
                                              error_id=error_id,
                                              error_description=error_description,
                                              process_name=process_name)
        appTools().error_logger(ctrl_cif=ctrl_cfrs)
        sys.exit()
    else:
        etl_processing = list()
        # Ejecucion Threading
        for C2_NOMBRE_ARCHIVO_v in C2_NOMBRE_ARCHIVO_LIST:
            etl_thread = etl(start_datetime=start_datetime,
                             start_datetime_etl=start_datetime_etl,
                             start_datetime_proc=start_datetime_proc,
                             C1_PROYECTO_v=C1_PROYECTO_v,
                             C2_NOMBRE_ARCHIVO_v=C2_NOMBRE_ARCHIVO_v,
                             C3_ESTATUS_v=C3_ESTATUS_v)
            etl_thread.start()
            etl_processing.append(etl_thread)
            time.sleep(0.5)

        runtime_error = False
        process_executing = appTools().count_process_executing(threads_processing=etl_processing)
        while process_executing >= 1:
            if SYSTEM_PERFORMANCE_MONITOR_CONTROL:
                systemPerformance().run()
            print(f'\033[96m> Running <{process_executing}> ETL process...\033[0m')

            current_time_exec_hour = round(((time.time() - start_datetime_etl) / 3600), 1)
            if current_time_exec_hour >= SYSTEM_RUNTIME_PERIOD_H:
                process_executing = appTools().count_process_executing(threads_processing=etl_processing)
                err = f'Timeout execution. Pending <{process_executing}> thread process. Current time: {current_time_exec_hour}H'
                print(f'[error] {err}')
                runtime_error = True
                break
            else:
                time.sleep(SYSTEM_PERFORMANCE_PERIOD)
                process_executing = appTools().count_process_executing(threads_processing=etl_processing)

        if runtime_error:
            ctrl_cfrs = appTools().cifras_control(db_table_name='lt_aficobranza.e_carga_rsua',
                                                  start_datetime=start_datetime,
                                                  start_datetime_proc=start_datetime_proc,
                                                  end_datetime_proc=appTools().get_datime_now(),
                                                  error_id='1.0',
                                                  error_description=err,
                                                  process_name=process_name)
            appTools().error_logger(ctrl_cif=ctrl_cfrs)
            sys.exit()
        else:
            elapsed_seconds = round(time.time() - start_datetime_etl, 3)
            print(f'[ok] \033[95m{process_name}\033[0m process executed on {elapsed_seconds}sec.')

        # Eliminar erchivos temporales LOCAL y HDFS
        """print(f'> Removing TEMP files...')
        try:
            print(f'> Removing temp TXT file {path_txt_file}')
            command = f'rm {path_txt_file}'
            print(f'> exec command: {command}')
            subprocess.run([command], shell=True, check=True)
            print(f'[ok] File {path_txt_file} is deleted')
        except subprocess.CalledProcessError as e:
            print(f'[error] Removing TXT file {txt_file_name}. {e}')"""
