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

FROM stealthly/docker-java

MAINTAINER stealthly

#Kafka settings
ENV KAFKA_VERSION 0.8.2-beta
ENV SCALA_VERSION 2.10
ENV KAFKA_RELEASE kafka_$SCALA_VERSION-$KAFKA_VERSION
ENV KAFKA_URL https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_RELEASE.tgz
ENV KAFKA_PATH /opt/$KAFKA_RELEASE
ENV BROKER_ID 1
ENV HOST_IP localhost
ENV PORT 9092

#Zookeeper settings
ENV ZK_VERSION 3.4.6
ENV ZK_RELEASE zookeeper-$ZK_VERSION
ENV ZK_URL http://apache.ip-connect.vn.ua/zookeeper/zookeeper-$ZK_VERSION/$ZK_RELEASE.tar.gz
ENV ZK_HOME /opt/$ZK_RELEASE

#Go settings
ENV GOLANG_VERSION 1.4
ENV GOLANG_RELEASE go$GOLANG_VERSION
ENV GOLANG_URL https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOROOT /usr/bin/go
ENV GOPATH /
ENV PATH $GOROOT/bin:$PATH

#Syslog defaults
ENV FORMAT rfc5424
ENV TCP_PORT 5140
ENV TCP_HOST 0.0.0.0
ENV NUM_PRODUCERS 1
ENV QUEUE_SIZE 10000
ENV LOG_LEVEL info
ENV MAX_PROCS 0

#Get git and mercurial
RUN sudo apt-get update
RUN sudo apt-get -y install git
RUN sudo apt-get -y install mercurial
#Get Kafka
RUN wget -q $KAFKA_URL -O /tmp/$KAFKA_RELEASE.tgz
RUN tar xfz /tmp/$KAFKA_RELEASE.tgz -C /opt
#Get Zookeeper
RUN wget -q $ZK_URL -O /tmp/$ZK_RELEASE.tar.gz
RUN tar -xzf /tmp/$ZK_RELEASE.tar.gz -C /opt
RUN cp $ZK_HOME/conf/zoo_sample.cfg $ZK_HOME/conf/zoo.cfg
#Get Go
RUN wget -q $GOLANG_URL -O /tmp/$GOLANG_RELEASE.tar.gz
RUN tar -xzf /tmp/$GOLANG_RELEASE.tar.gz -C /usr/bin
RUN mkdir -p $GOPATH/src
#Get GPM
RUN git clone https://github.com/pote/gpm.git && cd gpm && git checkout v1.3.1 && ./configure && make install
RUN go get github.com/stealthly/go_kafka_client && cd $GOPATH/src/github.com/stealthly/go_kafka_client && git checkout syslog && gpm install
RUN cd $GOPATH/src/github.com/stealthly/go_kafka_client/syslog && go build
RUN $GOPATH/src/github.com/stealthly/go_kafka_client/vagrant/kafka.sh

EXPOSE 9092
EXPOSE 2181

#Adding startup script
ADD syslog /usr/bin/syslog

#Run tests
CMD syslog --producer.config $PRODUCER_CONFIG --topic $TOPIC --format $FORMAT --tcp.port $TCP_PORT --tcp.host $TCP_HOST --num.producers $NUM_PRODUCERS --queue.size $QUEUE_SIZE --log.level $LOG_LEVEL --max.procs $MAX_PROCS
