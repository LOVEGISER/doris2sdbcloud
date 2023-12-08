import copy
import json
import time
from concurrent.futures import ThreadPoolExecutor

import config
import utils
from config import process_number
from executor import executor
from log_utils import logger



threadPool = ThreadPoolExecutor(process_number)

#one process -> one taskset
# 1.connect to doris by mysql_jdbc,return [{database_name:"",table_name:"",doris_ddl:""}]
# 2.multitask sync run [export,stage load]
# 3.record result to success_list.txt,when task fail retry
def process_backend(task_info):
        try:
            logger.info("task {} submit.....".format(task_info))
            executor_object = executor()
            executor_object.run(task_info)
        except Exception as e:
            logger.exception(e)
            raise Exception("task submit error")

#nohup python start_up.py > /dev/null 2>&1 &
if __name__ == '__main__':
    logger.info("start_up.py start")
    try:
        source_table_info = utils.get_doris_info(config.source_doris_info)
        for task_info in source_table_info:
             threadPool.submit(process_backend, task_info)
    except Exception as e:
        logger.exception(e)
        raise Exception("boot_strap start error")

