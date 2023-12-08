# -*- encoding=utf8 -*-
from datetime import time, datetime
from time import sleep

import pymysql.cursors

from log_utils import logger


def get_doris_info(connection_info):

    try:
        conn = pymysql.connect(host=connection_info['host'],
                               port=connection_info['port'],
                               user=connection_info['user'],
                               password=connection_info['password'],
                              # database="information_schema",
                               charset='utf8')
        mycursor = conn.cursor()
        mycursor.execute("show databases")
        result = mycursor.fetchall()
        skip_db = ['__internal_schema','information_schema']
        source_table_info = []
        for data in result:
            db_name = data[0]
            is_in_skip_db = any(num == db_name for num in skip_db)
            if not is_in_skip_db:
                mycursor.execute("use {}".format(db_name))
                mycursor.execute("show data;")
                show_data_list = mycursor.fetchall()
                for table_info in show_data_list:
                    #print(table_info)
                    table_name = table_info[0]
                    data_size = table_info[1]
                    skip_special_row = ['Total', 'Quota','Left','Transaction Quota']
                    is_in_special_row = any(item == table_name for item in skip_special_row)
                    if not is_in_special_row:
                        mycursor.execute("show create table {}".format(table_info[0]))
                        ddl_detail = mycursor.fetchall()
                        for ddl_temp in ddl_detail:
                           ddl =  ddl_temp[1]
                           source_table_info.append({"db_name":db_name,"table_name":table_name,"data_size":data_size,"ddl":ddl})
        print(source_table_info)
        mycursor.close()
        conn.close()
        return source_table_info
    except Exception as e:
        logger.exception(e)
        raise Exception("get_doris_info error")




def create_table(connection_info,table_info):

    try:
        conn = pymysql.connect(host=connection_info['host'],
                               port=connection_info['port'],
                               user=connection_info['user'],
                               password=connection_info['password'],
                              # database="information_schema",
                               charset='utf8')
        mycursor = conn.cursor()
        mycursor.execute("CREATE DATABASE IF NOT EXISTS {}".format(table_info["db_name"]))
        mycursor.execute("use  {}".format(table_info["db_name"]))
        ddl = str(table_info["ddl"]).replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ')
        #print(ddl)
        mycursor.execute(ddl)
        mycursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.exception(e)
        return False


def export_oss(connection_info,table_info,s3_info):

    try:
        conn = pymysql.connect(host=connection_info['host'],
                               port=connection_info['port'],
                               user=connection_info['user'],
                               password=connection_info['password'],
                               charset='utf8')
        mycursor = conn.cursor()
        mycursor.execute("use  {}".format(table_info["db_name"]))
        # default parallelism = 1,when datasize > 1GB ,parallelism = 12
        parallelism = 1
        current_time = str(datetime.now()).replace("-","").replace(" ","").replace(".","").replace(":","")
        label = "export"+table_info["table_name"] + current_time
        if str(table_info["data_size"]).__contains__("GB"):
            parallelism = 12
        export_sql = '''
        EXPORT TABLE {} TO "{}/{}/{}/" 
            PROPERTIES (
              "format"="csv",
              "label" = "{}",
              "column_separator"="\\x07", 
              "timeout" = "86400",
              "max_file_size" = "215MB",
              "parallelism" = "{}"
            ) WITH s3 (
                "AWS_ENDPOINT" = "{}",
                "AWS_ACCESS_KEY" = "{}",
                "AWS_SECRET_KEY" = "{}",
                "AWS_REGION" = "{}"
            );
        '''.format(table_info["table_name"],s3_info['s3_path_prex'],table_info["table_name"],current_time, label ,parallelism,s3_info["AWS_ENDPOINT"],s3_info["AWS_ACCESS_KEY"],s3_info["AWS_SECRET_KEY"],s3_info["AWS_REGION"])
        mycursor.execute(export_sql)
        mycursor.close()
        conn.close()
        cmd = 'SHOW EXPORT FROM {} WHERE LABEL = "{}";'.format(table_info["db_name"],label)
        res = {"current_time":current_time,"status" :get_status(connection_info, table_info,cmd)}
        return res
    except Exception as e:
        logger.exception(e)
        return False


def s3_load(connection_info, table_info, s3_info,current_time):
        try:
            conn = pymysql.connect(host=connection_info['host'],
                                   port=connection_info['port'],
                                   user=connection_info['user'],
                                   password=connection_info['password'],
                                   charset='utf8')
            mycursor = conn.cursor()
            mycursor.execute("use  {}".format(table_info["db_name"]))
            # default parallelism = 1,when datasize > 1GB ,parallelism = 12
            parallelism = 1
            label = "s3_load"+table_info["table_name"] + str(datetime.now()).replace("-","").replace(" ","").replace(".","").replace(":","")
            if str(table_info["data_size"]).__contains__("GB"):
                parallelism = 12
            load_sql = '''
                LOAD LABEL {}
                (
                    DATA INFILE("{}/{}/{}/*")
                    INTO TABLE {}
                    COLUMNS TERMINATED BY "\\x07"
                )
                WITH S3
                (
                    "AWS_ENDPOINT" = "{}",
                    "AWS_ACCESS_KEY" = "{}",
                    "AWS_SECRET_KEY"="{}",
                    "AWS_REGION" = "{}"
                )
                PROPERTIES
                (
                    "timeout" = "14400",
                    "exec_mem_limit" = "12884901888",
                    "load_parallelism" = "{}"
                );
            '''.format(label,
                       s3_info['s3_path_prex'], table_info["table_name"], current_time,table_info["table_name"], s3_info["AWS_ENDPOINT"],
                       s3_info["AWS_ACCESS_KEY"], s3_info["AWS_SECRET_KEY"], s3_info["AWS_REGION"],parallelism)
            print(load_sql)
            mycursor.execute(load_sql)
            mycursor.close()
            conn.close()
            cmd = 'show load where label="{}";'.format(label)
            return get_status(connection_info, table_info, cmd)
        except Exception as e:
            logger.exception(e)
            return False



def get_status(connection_info, table_info,cmd):
    try:

        conn = pymysql.connect(host=connection_info['host'],
                               port=connection_info['port'],
                               user=connection_info['user'],
                               password=connection_info['password'],
                               read_timeout = 3600,
                               charset='utf8')
        mycursor = conn.cursor()
        mycursor.execute("use  {}".format(table_info["db_name"]))
        status = False
        while True:
           mycursor.execute(cmd)
           exprot_status = mycursor.fetchall()
           state = str(exprot_status[0][2])
           print(state)
           if  state == "FINISHED":
               status = True
               break
           if state == "CANCELLED":
               status = False
               break
           else:
               sleep(10)
        mycursor.close()
        conn.close()
        return status
    except Exception as e:
        logger.exception(e)
        return False
