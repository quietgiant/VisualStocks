bin/kafka_2.12-2.3.1/bin/zookeeper-server-start.sh bin/kafka_2.12-2.3.1/config/zookeeper.properties & # start zookeeper
bin/kafka_2.12-2.3.1/bin/kafka-server-start.sh bin/kafka_2.12-2.3.1/config/server.properties & # start kafka server
launchctl start homebrew.mxcl.cassandra # start cassandra database
