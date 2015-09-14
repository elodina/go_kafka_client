go_kafka_client Mesos Framework
==============================

[Description](#description)
[Installation](#installation)
* [Prerequisites](#prerequisites)
* [Scheduler configuration](#scheduler-configuration)
* [Run the scheduler](#run-the-scheduler)
* [Quick start](#quick-start)

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

    --master: Mesos Master addresses.
    --api: API host:port for advertizing.
    --user: Mesos user. Defaults to current system user.
    --storage: Storage for cluster state. Examples: file:go_kafka_client_mesos.json; zk:master:2181/go-mesos.
	--log.level: Log level. trace|debug|info|warn|error|critical. Defaults to info.
	--framework.name: Framework name.
	--framework.role: Framework role.
    --framework.timeout: Framework failover timeout.
    
Quick start
-----------

    # export GM_API=http://master:6666
    # ./cli scheduler --master master:5050 --log.level debug
    # ./cli add mirrormaker 0
    # ./cli update 0 --producer.config producer.config --consumer.config consumer.config --whitelist "^test$"
    # ./cli start 0
    ...
    # ./cli status
    # ./cli stop 0
    # ./cli remove 0
    
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

**TODO** incomplete!
```
# ./cli help
Usage:
  help: show this message
  scheduler: start scheduler
  add: add task
  start: start task
  stop: stop task
  update: update configuration
  status: get current cluster status
More help you can get from ./cli <command> -h
```
    
Adding tasks to the cluster
--------------------------

    # ./cli add <type> <id>
    
Example:

    # ./cli add mirrormaker 0
    
Options:

    --api: API host:port for advertizing.
    --cpu: CPUs per task. Defaults to 0.5.
	--mem: Mem per task. Defaults to 512.
    --constraints: Constraints (hostname=like:^master$,rack=like:^1.*$).
    
Configuring tasks in the cluster
-------------------------------

    # ./cli update <id> <flags>
    
Options (for mirrormaker):

    --api: API host:port for advertizing.
    --cpu: CPUs per task.
	--mem: Mem per task.
	--whitelist: Regex pattern for whitelist. Providing both whitelist and blacklist is an error.
	--blacklist: Regex pattern for blacklist. Providing both whitelist and blacklist is an error.
	--producer.config: Producer config file name.
	--consumer.config: Consumer config file name.
	--num.producers: Number of producers.
	--num.streams: Number of consumption streams.
	--preserve.partitions: Preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.
	--preserve.order: E.g. message sequence 1, 2, 3, 4, 5 will remain 1, 2, 3, 4, 5 in destination topic.
	--prefix: Destination topic prefix.")
	--queue.size: Maximum number of messages that are buffered between the consumer and producer.
    
Starting tasks in the cluster
-----------------------------

    # ./cli start <id>
    
Example:
    
    # ./cli start 0
    
Options:

    --api: API host:port for advertizing.
    --timeout: Timeout in seconds to wait until the task receives Running status.
    
Stopping tasks in the cluster
-----------------------------

    # ./cli stop <id>
    
Example:
    
    # ./cli stop 0
    
Options:

    --api: API host:port for advertizing.
    
Removing tasks from the cluster
-----------------------------

    # ./cli remove <id>
    
Example:
    
    # ./cli remove 0
    
Options:

    --api: API host:port for advertizing.
    
Cluster status
--------------

    # ./cli status
    
Options:

    --api: API host:port for advertizing.