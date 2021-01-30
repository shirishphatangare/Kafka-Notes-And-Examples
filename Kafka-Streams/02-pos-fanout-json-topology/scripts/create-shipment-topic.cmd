rem Commented commands are for Apache Kafka Version
rem kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 2 --topic shipment --config min.insync.replicas=2

rem Below Commands are for Confluent Kafka Version
%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --topic shipment --partitions 2 --replication-factor 3 --config min.insync.replicas=2