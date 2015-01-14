# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/sh -Eux

#  Trap non-normal exit signals: 1/HUP, 2/INT, 3/QUIT, 15/TERM, ERR
trap founderror 1 2 3 15 ERR

founderror()
{
        exit 1
}

exitscript()
{
        #remove lock file
        #rm $lockfile
        exit 0
}

# Syslog Server configurations here.
PRODUCER_CONFIG="/vagrant/syslog/producer.properties"
TOPIC="syslog"
FORMAT="rfc5424"
TCP_PORT="5140"
TCP_HOST="0.0.0.0"
UDP_PORT="5141"
UDP_HOST="0.0.0.0"
NUM_PRODUCERS="1"
QUEUE_SIZE="10000"
LOG_LEVEL="info"
MAX_PROCS="0" # 0 does not modify current value, it's here just to expose the parameter

# Setup Zookeeper, Kafka, Go etc.
export HOST_IP=192.168.66.66
export KAFKA_VERSION=0.8.2-beta
export SCALA_VERSION=2.10
export KAFKA_RELEASE=kafka_$SCALA_VERSION-$KAFKA_VERSION
export KAFKA_PATH=/opt/$KAFKA_RELEASE
export ZK_VERSION=3.4.6
export ZK_RELEASE=zookeeper-$ZK_VERSION
export ZK_HOME=/opt/$ZK_RELEASE

/vagrant/vagrant/prep.sh

#Start Zookeeper
echo 'Starting Zookeeper'
$ZK_HOME/bin/zkServer.sh start

#Start Kafka
echo "advertised.host.name=$HOST_IP" >> $KAFKA_PATH/config/server.properties
sed -r -i "s/^(log4j.rootLogger)=(.*)( stdout)/\1=WARN\3/g" $KAFKA_PATH/config/log4j.properties

echo 'Starting Kafka'
$KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/server.properties &

# let Zookeeper and Kafka start normally
sleep 5
$KAFKA_PATH/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
/vagrant/syslog/syslog --producer.config $PRODUCER_CONFIG --topic $TOPIC --format $FORMAT --tcp.port $TCP_PORT --tcp.host $TCP_HOST --udp.port $UDP_PORT --udp.host $UDP_HOST --num.producers $NUM_PRODUCERS --queue.size $QUEUE_SIZE --log.level $LOG_LEVEL --max.procs $MAX_PROCS &

exitscript