# -*- encoding=utf8 -*-
import os
from datetime import datetime
from time import sleep

import config
import utils
from log_utils import logger


"""
-------------------------------------------------
@author: "wanglei@flywheels.com"
@file: executor.py
@time: 2023-12-08
-------------------------------------------------
"""

class executor():
    def __init__(self):
        return super().__init__()

    def run(self,task_info):

        try:

            task_flag = "{}.{} ".format(task_info["db_name"], task_info["table_name"])
            cmd = "cat {}/success_list.txt |grep '{}' |wc -l".format(config.success_list_path, task_flag)
            print(cmd)
            is_transfar = os.popen(cmd).read().replace("\n", "").replace(" ","")
            if "0" == is_transfar:
                logger.info("task :{} start".format(task_info))
                # step 1： export to oss
                task_status = "Fail"
                export_result = utils.export_oss(config.source_doris_info,task_info,config.s3_info)
                if export_result["status"] == True:
                    # step 2：create database and table on selectdb cloud
                   create_result = utils.create_table(config.target_sdb_info,task_info)
                   if create_result:
                      #step 3 : load data to SelectDB cloud by s3
                      s3_load_result = utils.s3_load(config.target_sdb_info,task_info,config.s3_info,export_result["current_time"])
                      if s3_load_result:
                          task_status = "Success"
                          f = open("{}/success_list.txt".format(config.success_list_path), "a")
                          f.write("{} success done".format(task_flag))
                          f.close()
            else:
                logger.info("skip already completed  task :{} ".format(task_info))
            logger.info("run {} task status:{} ".format(task_status, task_info))
        except Exception as e:
            logger.exception(e)
            logger.error("task run error:{} ".format(task_info))
            #raise Exception("boot_strap start error")
