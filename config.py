# -*- encoding=utf8 -*-

"""
-------------------------------------------------
@author: "wanglei@flywheels.com"
@file: config.py
@time: 2023-12-08
@desc:  Server Config
-------------------------------------------------
"""
#并发度
process_number = 2
#这里是配置当前绝对目录
success_list_path = "/root/doris-selectdbcloud"
#doris数据源配置
source_doris_info = {"host":"127.0.0.1","port":9030, "user":"root","password":""}
#SelecDB Cloud目标端配置
target_sdb_info = {"host":"172.21.16.x","port":29402, "user":"admin","password":"xx"}
#依赖s3地址，注意：s3 region需要和SelectDB Cloud在同一个Region
s3_info = {"s3_path_prex":"s3://wanglei-bj-1316291683/export_test/","AWS_ENDPOINT":"cos.ap-beijing.myqcloud.com", "AWS_ACCESS_KEY":"xxx","AWS_SECRET_KEY":"xxx","AWS_REGION":"ap-beijing"}