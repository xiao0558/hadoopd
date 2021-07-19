源文件
touch /tmp/2.txt
echo 1,路口1,51,31,71,41,2021-06-24 01:00:00>>/tmp/2.txt
echo 2,路口3,52,32,72,42,2021-06-24 01:00:00>>/tmp/2.txt
echo 3,路口1,53,33,73,43,2021-06-24 02:00:00>>/tmp/2.txt
echo 4,路口2,54,34,74,44,2021-06-24 02:00:00>>/tmp/2.txt
echo 5,路口1,55,35,75,45,2021-06-25 03:00:00>>/tmp/2.txt
echo 6,路口2,56,36,76,46,2021-06-25 03:00:00>>/tmp/2.txt
echo 7,路口3,57,37,77,47,2021-06-25 03:00:00>>/tmp/2.txt
cat /tmp/2.txt

上传源文件
hadoop fs -mkdir /hadoopc_input
hadoop fs -put /tmp/2.txt /hadoopc_input

删除输出目录
hadoop fs -rm -r /hadoopc_output

执行任务
put D:\workspace_src\hadoop-demo\hadoopc\target\hadoopc-0.0.1-SNAPSHOT-jar-with-dependencies.jar
hadoop jar hadoopc-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.demo.hadoop.data.DataDriver /hadoopc_input /hadoopc_output

HDFS文件系统
http://hadoop76:9870/explorer.html

查看任务和日志
http://hadoop77:8088/cluster
