launchctl stop homebrew.mxcl.cassandra # stop cassandra database
bin/kafka_2.12-2.3.1/bin/kafka-server-stop.sh config/zookeeper.properties & # stop kafka server
bin/kafka_2.12-2.3.1/bin/zookeeper-server-stop.sh config/zookeeper.properties & # stop zookeeper