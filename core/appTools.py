import datetime
import smtplib
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
import uuid
import platform
import unidecode
import __main__
from core.storageController import MySQL, Hive
from core.constants import ERROR_TYPE, CIFRAS_ERROR_LOG
from subprocess import Popen, PIPE


class appTools:
    def get_kerberos_ticket(self):
        try:
            kinit_args = [
                '/usr/bin/kinit',
                '-kt',
                '/etc/security/keytabs/apli_usr_nifi.keytab',
                'apli_usr_nifi@IMSS.GOB.MX']
            subp = Popen(kinit_args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            subp.wait()
        except Exception as e:
            print(f'[error] get_kerberos_ticket.')
            if not e.args:
                e.args = ('',)
            e.args = e.args + ('[3.2]',)
            raise

    def format_file_name(self, v_file_name):
        return unidecode.unidecode(v_file_name.lower().replace(' ', '_'))

    def get_os_system(self):
        try:
            release_version = str(platform.release())
            os_name = str(platform.system()).lower()
        except Exception as e:
            print(f'[error] get_os_system. {e}')
            release_version = 'unknown'
            os_name = 'unknown'
        return [os_name, release_version]

    def count_process_executing(self, threads_processing):
        proc_exec = 0
        for tp in threads_processing:
            if tp.is_alive():
                proc_exec += 1
        return proc_exec

    def id_generator(self):
        return str(uuid.uuid4().hex)

    def cifras_control(self, db_table_name, start_datetime,
                       start_datetime_proc, end_datetime_proc, error_id,
                       error_description, process_name):
        if CIFRAS_ERROR_LOG:
            try:
                db_table_name = db_table_name.split('.')
                db_name = db_table_name[0]
                table_name = db_table_name[1]

                stage = db_name[0:2]
                allowed_stage = ['lt', 'te']

                if not stage in allowed_stage:
                    raise ValueError(f'{stage} is not supported')

                if error_description:
                    des_error = f'{ERROR_TYPE[error_id]}-{error_description}'
                    des_error = des_error[0:149]
                else:
                    des_error = ''

                special_case_nom_des_proceso = ['sut_sipa_00encabezado_lote', 'sut_sipa_01encabezado_ent']
                if table_name in special_case_nom_des_proceso:
                    process_name = process_name.replace('lt', 'te')
                nom_proceso = f'{process_name}-{table_name}'
                des_proceso = nom_proceso

                ctrl_cfrs = {
                    'id_proceso': 3,
                    'nom_grupo': self.id_generator(),
                    'nom_tabla': table_name,
                    'nom_fuente': 'cobranza',
                    'nom_proceso': nom_proceso,
                    'des_proceso': des_proceso,
                    'des_capa': stage,
                    'nom_usuario': str(os.getlogin()),
                    'fec_inicio': start_datetime,
                    'fec_fin': '',
                    'fec_proceso_inicio': start_datetime_proc,
                    'fec_proceso_fin': end_datetime_proc,
                    'num_registros': '',
                    'num_error': int(error_id[0]),
                    'des_error': des_error,
                    'num_advertencia': 0,
                    'des_advertencia': '',
                    'des_estatus': ''}
                if error_description:
                    ctrl_cfrs['des_estatus'] = 'ERROR'
                else:
                    ctrl_cfrs['des_estatus'] = 'OK'
                ctrl_cfrs['num_registros'] = Hive().get_registers(db=db_name, table=table_name)
            except Exception as e:
                print(f'[error] cifras_control. {e}')
                ctrl_cfrs = None
            return ctrl_cfrs

    def error_logger(self, ctrl_cif):
        if CIFRAS_ERROR_LOG and ctrl_cif:
            try:
                table_name = 'bd_control.ctrl_cfrs'
                ctrl_cif['fec_fin'] = self.get_datime_now()
                print(f"> fec_fin: {ctrl_cif['fec_fin']}")
                params_name = list(dict(ctrl_cif).keys())
                params_value = [ctrl_cif[k] for k in params_name]
                # print(f'params_value: {params_value}')
                MySQL().insert_data(table_name=table_name,
                                    params_name=params_name,
                                    params_value=params_value)
            except Exception as e:
                print(f'[error] error_logger. {e}')
                # raise

    def get_datime_now(self):
        return datetime.datetime.now()

    def get_error(self, error):
        try:
            error_id = self.extract_string(string=error,
                                           start_char='[', end_char=']')
            error_description = self.extract_string(string=error,
                                                    start_char='(', end_char=',')
            return error_id, error_description
        except Exception as e:
            return '0', e

    def extract_string(self, string, start_char, end_char):
        str_ = str(string).split(start_char)[1].split(end_char)[0]
        str_ = str_.replace("'", '')
        return str_

    def get_proc_etl_name(self):
        try:
            complete_name = str(__main__.__file__)
            complete_name = str(complete_name).replace(chr(92), '/')
            only_name = complete_name.split('/')[-1]
            return only_name.split('.')[0]
        except Exception as e:
            print(f'[error] get_proc_etl_name. {e}')
            return ''

    def email_sender(self, email_content, to_email_list, subject='', file='None'):
        smtp_server = '172.16.23.17'
        smtp_port = 25
        # smtp_server = 'smtp.gmail.com'
        # smtp_port = 587
        from_email = 'datamart.sindo@imss.gob.mx'
        # from_email = 'jjsaldanalopez@gmail.com'

        msg = MIMEMultipart()
        msg['Subject'] = subject
        msg['From'] = from_email
        msg['To'] = to_email_list
        msg.attach(MIMEText(email_content, 'plain'))
        print(f'file type {type(file)}')
        if file != 'None':
            print(f'file a enviar{file}')
            attachmentPath = file
            try:
                with open(attachmentPath, "rb") as attachment:
                    p = MIMEApplication(attachment.read())
                    p.add_header('Content-Disposition', "attachment; filename= %s" % attachmentPath.split("/")[-1])
                    msg.attach(p)
            except Exception as e:
                print(str(e))
        try:
            with smtplib.SMTP(smtp_server, smtp_port, timeout=30.5) as server:
                server.send_message(msg)
                print(f'[ok] Successfully sent email to: {to_email_list}')
                server.quit()
        except Exception as e:
            print(f'[error] email_sender. {e}')
            try:
                server.close()
            except Exception as e:
                print(e)


if __name__ == '__main__':
    file_path = ''
    email_content = "Aqui va el contenido del mail. \n otro renglon"
    appTools().email_sender(email_content=email_content,
                            to_email_list='jjsaldanalopez@hotmail.com,jose.saldana@people-media.com.mx',
                            subject='Tema del mail, ejemplo: Error insercion tabla TE afiliacion.',
                            file='C:/proyectos/imss/la/RSUA2023-05-24.csv/part-00000-c961376d-f968-4158-ad02-46c92ed74453-c000.csv')
    print('mando correo')
