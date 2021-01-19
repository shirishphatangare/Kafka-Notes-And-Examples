rem For all topics which are included in a transaction should be configured with --
rem  	a) Replication Factor >= 3
rem  	b) min.insync.replicas >= 2

%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --topic hello-producer-1 --partitions 5 --replication-factor 3 --config min.insync.replicas=2