go_kafka_client Mesos Framework
==============================

[Description](#description)
[Installation](#installation)
* [Prerequisites](#prerequisites)
* [Scheduler configuration](#scheduler-configuration)
* [Run the scheduler](#run-the-scheduler)
* [Quick start](#quick-start)
* [Quick start using Vagrant](https://github.com/stealthly/go_kafka_client/tree/mesos-framework/mesos/vagrant/quickstart.md)
* [Running with Marathon](https://github.com/stealthly/go_kafka_client/tree/mesos-framework/mesos/marathon)
* [Running with Docker](https://github.com/stealthly/go_kafka_client/tree/mesos-framework/mesos/docker)

[Typical operations](#typical-operations)
* [Shutting down framework](#shutting-down-framework)

[Navigating the CLI](#navigating-the-cli)
* [Requesting help](#requesting-help)  
* [Adding tasks to the cluster](#adding-tasks-to-the-cluster)  
* [Configuring tasks](#configuring-tasks-in-the-cluster)  
* [Starting tasks](#starting-tasks-in-the-cluster)  
* [Stopping tasks](#stopping-tasks-in-the-cluster)  
* [Removing tasks](#removing-tasks-from-the-cluster)  
* [Cluster status](#cluster-status)  

Description
-----------

This framework aims to simplify running all sorts of software built on top of go_kafka_client on Mesos. Being actively developed right now.

Right now this framework supports running only the mirror maker.

Installation
============
Prerequisites
-------------

Install go 1.4 (or higher) http://golang.org/doc/install

Install godep https://github.com/tools/godep

Clone and build the project

    # git clone https://github.com/stealthly/go_kafka_client.git
    # cd go_kafka_client
    # godep restore ./...
    # cd mesos
    # go build cli.go
    # go build executor.go
    
Scheduler configuration
-----------------------

```
# ./cli help scheduler
Usage: scheduler [options]

Options:
    --master: Mesos Master addresses.
    --api: API host:port for advertizing.
    --user: Mesos user. Defaults to current system user.
    --storage: Storage for cluster state. Examples: file:go_kafka_client_mesos.json; zk:master:2181/go-mesos.
    --log.level: Log level. trace|debug|info|warn|error|critical. Defaults to info.
    --framework.name: Framework name.
    --framework.role: Framework role.
    --framework.timeout: Framework failover timeout.
```
    
Quick start
-----------

In order not to pass the API url to each CLI call lets export the URL as follows:

    # export GM_API=http://master:6666
    
First lets start 1 mirror maker task with the default settings. Further in the readme you can see how to change these from the defaults.    

```
# ./cli add mirrormaker 0
Added tasks 0

cluster:
  task:
    type: mirrormaker
    id: 0
    state: inactive
    cpu: 0.50
    mem: 512.00
    configs:
      num.producers: 1
      num.streams: 1
      queue.size: 10000
```
    
You now have a cluster with 1 task that is not started.    

```
# ./cli status
cluster:
  task:
    type: mirrormaker
    id: 0
    state: inactive
    cpu: 0.50
    mem: 512.00
    configs:
      queue.size: 10000
      num.producers: 1
      num.streams: 1
```

Each mirror maker task requires some basic configuration.

```
# ./cli update 0 --producer.config producer.config --consumer.config consumer.config --whitelist "^mytopic$"
Configuration updated:

cluster:
  task:
    type: mirrormaker
    id: 0
    state: inactive
    cpu: 0.50
    mem: 512.00
    configs:
      num.producers: 1
      num.streams: 1
      queue.size: 10000
      consumer.config: consumer.config
      producer.config: producer.config
      whitelist: ^mytopic$
```

Now lets start the task. This call to CLI will block until the task is actually started but will wait no more than a configured timeout. Timeout can be passed via --timeout flag and defaults to 30s. If a timeout of 0ms is passed CLI won't wait for tasks to start at all and will reply with "Scheduled tasks ..." message.

```
# ./cli start 0
Started tasks 0

cluster:
  task:
    type: mirrormaker
    id: 0
    state: running
    cpu: 0.50
    mem: 512.00
    task id: mirrormaker-0-e5ab7581-1c31-d85d-ff14-77b5fb3de4d4
    slave id: 20150914-091943-84125888-5050-10717-S1
    executor id: mirrormaker-0
    attributes:
      hostname: slave0
    configs:
      consumer.config: consumer.config
      producer.config: producer.config
      whitelist: ^mytopic$
      num.producers: 1
      num.streams: 1
      queue.size: 10000
```

By now you should have a single mirror maker instance running. Here's how you stop it:

```
# ./cli stop 0
Stopped tasks 0
```

And remove:

```
# ./cli remove 0
Removed tasks 0
```
    
Typical operations
=================
-----------------------
Shutting down framework
-----------------------

While the scheduler has a shutdown hook it doesn't actually finish the framework. To shutdown the framework completely (e.g. unregister it in Mesos) you may shoot a `POST` to `/teardown` specifying the framework id to shutdown:

```
# curl -d frameworkId=20150807-094500-84125888-5050-14187-0005 -X POST http://master:5050/teardown
```

Navigating the CLI
==================
Requesting help
--------------

```
# ./cli help
Usage:
  help: show this message
  scheduler: start scheduler
  add: add task
  update: update configuration
  start: start task
  status: get current cluster status
  stop: stop task
  remove: remove task
Get detailed help from ./cli help <command>
```
    
Adding tasks to the cluster
--------------------------

```
# ./cli help add
Usage: add <type> <id-expr> [options]

Types:
    mirrormaker

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --cpu: CPUs per task. Defaults to 0.5.
    --mem: Mem per task. Defaults to 512.
    --constraints: Constraints (hostname=like:^master$,rack=like:^1.*$).
    
id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster

constraint examples:
    like:slave0    - value equals 'slave0'
    unlike:slave0  - value is not equal to 'slave0'
    like:slave.*   - value starts with 'slave'
    unique         - all values are unique
    cluster        - all values are the same
    cluster:slave0 - value equals 'slave0'
    groupBy        - all values are the same
    groupBy:3      - all values are within 3 different groups
```
    
Configuring tasks in the cluster
-------------------------------

All file-related configs should be set as file names located `.` (e.g. setting --producer.config producer.properties) OR 
http/https URLs (e.g. http://192.168.3.1:6666/producer.config). Please note that http URLs MUST end with file name to be downloaded as Mesos does not respect `Content-Disposition` header.
```
# ./cli help update
Usage: update <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --cpu: CPUs per task.
    --mem: Mem per task.
    --constraints: Constraints (hostname=like:^master$,rack=like:^1.*$).
    --whitelist: Regex pattern for whitelist. Providing both whitelist and blacklist is an error.
    --blacklist: Regex pattern for blacklist. Providing both whitelist and blacklist is an error.
    --producer.config: Producer config url or file name.
    --consumer.config: Consumer config url or file name.
    --num.producers: Number of producers.
    --num.streams: Number of consumption streams.
    --preserve.partitions: Preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.
    --preserve.order: E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.
    --prefix: Destination topic prefix.")
    --queue.size: Maximum number of messages that are buffered between the consumer and producer.
    
id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster
constraint examples:
    like:slave0    - value equals 'slave0'
    unlike:slave0  - value is not equal to 'slave0'
    like:slave.*   - value starts with 'slave'
    unique         - all values are unique
    cluster        - all values are the same
    cluster:slave0 - value equals 'slave0'
    groupBy        - all values are the same
    groupBy:3      - all values are within 3 different groups
```
    
Starting tasks in the cluster
-----------------------------

```
# ./cli help start
Usage: start <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    --timeout: Timeout in seconds to wait until the task receives Running status.
    
id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster
```
    
Stopping tasks in the cluster
-----------------------------

```
# ./cli help stop
Usage: stop <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    
id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster
```
    
Removing tasks from the cluster
-----------------------------

```
# ./cli help remove
Usage: remove <id-expr> [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
    
id-expr examples:
    0      - task 0
    0,1    - tasks 0,1
    0..2   - tasks 0,1,2
    0,1..2 - tasks 0,1,2
    *      - all tasks in cluster
```
    
Cluster status
--------------

```
# ./cli help status
Usage: status [options]

Options:
    --api: API host:port for advertizing. Optional if GM_API env is set.
```