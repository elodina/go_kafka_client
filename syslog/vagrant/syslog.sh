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

# Apply Syslog Server configurations.
source /vagrant/syslog/vagrant/params.sh

sudo echo "*.*                             @@$TCP_HOST:$TCP_PORT" >> /etc/rsyslog.d/50-default.conf
sudo service rsyslog restart

# Setup Zookeeper, Kafka, Go etc.
export HOST_IP_ADDR=192.168.66.66
source /vagrant/vagrant/prep.sh

#Build syslog binary
cd $GOPATH/src/github.com/stealthly/go_kafka_client && gpm install
cd $GOPATH/src/github.com/stealthly/go_kafka_client/syslog && go build

#Start Zookeeper
echo 'Starting Zookeeper'
$ZK_HOME/bin/zkServer.sh start

#Start Kafka
echo "advertised.host.name=$HOST_IP_ADDR" >> $KAFKA_PATH/config/server.properties
sed -r -i "s/^(log4j.rootLogger)=(.*)( stdout)/\1=WARN\3/g" $KAFKA_PATH/config/log4j.properties

echo 'Starting Kafka'
$KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/server.properties &

# let Zookeeper and Kafka start normally
sleep 5
$KAFKA_PATH/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic $TOPIC
$GOPATH/src/github.com/stealthly/go_kafka_client/syslog/syslog $SYSLOG_ARGS --broker.list $HOST_IP_ADDR:9092 &

exitscript