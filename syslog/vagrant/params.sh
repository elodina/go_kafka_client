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

# Syslog Server configurations here.
export TOPIC=syslog #Topic MUST BE present in order to be created first
export TCP_PORT=5140 #TCP port and host are then read and written to rsyslog.d properties to listen for live syslog data
export TCP_HOST=0.0.0.0
export SYSLOG_ARGS="--producer.config /vagrant/syslog/producer.properties --topic $TOPIC --tcp.port $TCP_PORT --tcp.host $TCP_HOST --udp.port 5141 --udp.host 0.0.0.0 --num.producers 1 --queue.size 10000 --log.level info --max.procs 0"
