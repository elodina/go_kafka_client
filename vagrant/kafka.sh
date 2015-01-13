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

#Start Zookeeper
echo 'Starting Zookeeper'
$ZK_HOME/bin/zkServer.sh start

#Start Kafka
sed -r -i "s/(zookeeper.connect)=(.*)/\1=$ZK_PORT_2181_TCP_ADDR/g" $KAFKA_PATH/config/server.properties
sed -r -i "s/(broker.id)=(.*)/\1=$BROKER_ID/g" $KAFKA_PATH/config/server.properties
sed -r -i "s/#(advertised.host.name)=(.*)/\1=$HOST_IP/g" $KAFKA_PATH/config/server.properties
sed -r -i "s/^(port)=(.*)/\1=$PORT/g" $KAFKA_PATH/config/server.properties
sed -r -i "s/^(log4j.rootLogger)=(.*)( stdout)/\1=WARN\3/g" $KAFKA_PATH/config/log4j.properties

echo 'Starting Kafka'
$KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/server.properties &
