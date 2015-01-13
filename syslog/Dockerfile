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

FROM golang:1.4

MAINTAINER stealthly

#Syslog defaults
ENV FORMAT rfc5424
ENV TCP_PORT 5140
ENV TCP_HOST 0.0.0.0
ENV NUM_PRODUCERS 1
ENV QUEUE_SIZE 10000
ENV LOG_LEVEL info
ENV MAX_PROCS 0

#Get GPM
RUN git clone https://github.com/pote/gpm.git && cd gpm && git checkout v1.3.1 && ./configure && make install
RUN go get github.com/stealthly/go_kafka_client && cd $GOPATH/src/github.com/stealthly/go_kafka_client && git checkout syslog && gpm install
RUN cd $GOPATH/src/github.com/stealthly/go_kafka_client/syslog && go build

#Adding startup script
ADD syslog /usr/bin/syslog

#Run server
CMD syslog --producer.config $PRODUCER_CONFIG --topic $TOPIC --format $FORMAT --tcp.port $TCP_PORT --tcp.host $TCP_HOST --num.producers $NUM_PRODUCERS --queue.size $QUEUE_SIZE --log.level $LOG_LEVEL --max.procs $MAX_PROCS
