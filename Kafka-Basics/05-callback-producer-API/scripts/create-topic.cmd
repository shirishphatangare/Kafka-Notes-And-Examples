%KAFKA_HOME%\bin\windows\kafka-topics.bat --create --zookeeper localhost:2181 --topic sync-hello-producer --partitions 1 --replication-factor 3 --config min.insync.replicas=3