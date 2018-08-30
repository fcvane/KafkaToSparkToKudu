# KafkaToSparkToKudu
#实时流数据同步
SparkStreaming消费Kafka消息队列数据,实时写入Kudu列存数据库。
#调用方式
根据Kafka offset存储方式的不同，调用方式也不同。
Kafka offset 存储方式有以下三种：
1.ZooKeeper存储
zkCli.sh -server bigdata04:2181,bigdata05:2181,bigdata06:2181,bigdata07:2181,bigdata08:2181
ls /oggoffset
get /oggoffset/0
使用zk存储和读取采用以下方式调用：
spark-submit \
 --master local[*] \
 --class ConsumerMain \
 --files /home/kafka.keystore,/home/kafka.truststore \
 /home/ConsumerMain.jar zk

2.本地文件存储
存储位置在 ./files/offset.log
调用方式：
spark-submit \
 --master local[*] \
 --class ConsumerMain \
 --files /home/kafka.keystore,/home/kafka.truststore \
 /home/ConsumerMain.jar local
 
 3.新版本Kafka(0.10及以上)自身存储
 自身存储不需要专门编写读取Kafka offset的方法
 调用方式(不带参数)：
 spark-submit \
 --master local[*] \
 --class ConsumerMain \
 --files /home/kafka.keystore,/home/kafka.truststore \
 /home/ConsumerMain.jar
 
 #后续提升
 仅供参考
 现阶段只是模板，暂未实现正常的参数调用和配置实例化。
 
 
