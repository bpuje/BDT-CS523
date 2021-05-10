# BDT-CS523

Big Data Technology - CS523

$ tar -xzf kafka_2.13-2.8.0.tgz
$ cd kafka_2.13-2.8.0

=== into kafka folder ---- Start zookeeper ===
bin/zookeeper-server-start.sh config/zookeeper.properties


sudo service --status-all
bin/kafka-topics.sh --list --zookeeper localhost:2181

sudo lsof -i :2181
sudo kill -9 1005


=== start kafka server ======
bin/kafka-server-start.sh config/server.properties

=== Create topic ==== create the input topic named topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic mainTopic

==== Run Producer ====
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic mainTopic

=== Run Consumer ====
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic mainTopic --from-beginning
