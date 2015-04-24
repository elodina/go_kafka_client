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

#Go settings
ENV GOLANG_VERSION 1.3.3
ENV GOLANG_RELEASE go$GOLANG_VERSION
ENV GOLANG_URL https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
ENV GOROOT /usr/bin/go
ENV GOPATH /
ENV PATH $GOROOT/bin:$PATH

#Consumer identity
ENV CLIENT_ID go-consumer
ENV GROUP_ID cs-rebalance-group
ENV NUM_CONSUMERS 1
ENV TOPIC testing1
ENV LOG_LEVEL info

#Zookeeper connection settings
ENV ZOOKEEPER_TIMEOUT 1s

#Workers settings
ENV NUM_WORKERS 1
ENV MAX_WORKER_RETRIES 3
ENV WORKER_BACKOFF 500ms
ENV WORKER_RETRY_THRESHOLD 100
ENV WORKER_CONSIDERED_FAILED_TIME_WINDOW 500ms
ENV WORKER_BATCH_TIMEOUT 5m
ENV WORKER_TASK_TIMEOUT 1m
ENV WORKER_MANAGERS_STOP_TIMEOUT 1m

#Rebalance settings
ENV REBALANCE_BARRIER_TIMEOUT 30s
ENV REBALANCE_MAX_RETRIES 4
ENV REBALANCE_BACKOFF 5s
ENV PARTITION_ASSIGNMENT_STRATEGY range
ENV EXCLUDE_INTERNAL_TOPICS true

#Fetcher settings
ENV NUM_CONSUMER_FETCHERS 1
ENV FETCH_BATCH_SIZE 1000
ENV FETCH_MESSAGE_MAX_BYTES 5242880
ENV FETCH_MIN_BYTES 1
ENV FETCH_BATCH_TIMEOUT 5s
ENV REQUEUE_ASK_NEXT_BACKOFF 1s
ENV FETCH_WAIT_MAX_MS 100
ENV SOCKET_TIMEOUT 30s
ENV QUEUED_MAX_MESSAGES 3
ENV REFRESH_LEADER_BACKOFF 200ms
ENV FETCH_METADATA_RETRIES 3
ENV FETCH_METADATA_BACKOFF 500ms

#Offsets settings
ENV OFFSETS_STORAGE zookeeper
ENV AUTO_OFFSET_RESET smallest
ENV OFFSETS_COMMIT_MAX_RETRIES 5

#Deployment
ENV DEPLOYMENT_TIMEOUT 0s

#Metrics
ENV FLUSH_INTERVAL 0s

#Get git and mercurial
RUN sudo apt-get update
RUN sudo apt-get -y install git
RUN sudo apt-get -y install mercurial

#Get Go
RUN wget -q $GOLANG_URL -O /tmp/$GOLANG_RELEASE.tar.gz
RUN tar -xzf /tmp/$GOLANG_RELEASE.tar.gz -C /usr/bin
RUN mkdir -p $GOPATH/src

#Get GPM
RUN git clone https://github.com/pote/gpm.git && cd gpm && git checkout v1.3.1 && ./configure && make install

COPY apply_config.sh /tmp/apply_config.sh
COPY build.sh /tmp/build.sh

ENV CLIENT_PATH /opt/go_kafka_client
RUN mkdir -p $CLIENT_PATH

RUN git clone https://github.com/stealthly/go_kafka_client.git && sh /tmp/build.sh

CMD cd $CLIENT_PATH && sh /tmp/apply_config.sh && ./consumers