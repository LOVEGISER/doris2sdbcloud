1. 使用说明
   1. 需要python3 环境
   2. 修改config.py的配置文件
   3. 执行python3 start_up.py启动服务即可
2. 流程： 
   4. export s3 ;
   5. create table on selectdb cloud ;
   6. load data to selectdb cloud by s3 load
3. 功能
   1. 支持多任务并行
   2. 支持错误重传
   3. 暂时不支持分区迁移
