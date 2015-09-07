go_kafka_client Mesos Framework
==============================

Description
-----------

This framework aims to simplify running all sorts of software built on top of go_kafka_client on Mesos. Being actively developed right now, lots of breaking changes coming soon.

Right now this framework supports running only the mirror maker.

Installation
------------

Install go 1.4 (or higher) http://golang.org/doc/install

Install godep https://github.com/tools/godep

Clone and build the project

    # git clone https://github.com/stealthly/go_kafka_client.git
    # cd go_kafka_client
    # godep restore ./...
    # cd mesos
    # go build cli.go
    # go build executor.go
    
Usage
-----

*TODO* coming soon

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
    
Scheduler configuration
-----------------------

    --master: Mesos Master addresses.
    --api: API host:port for advertizing.
	--user: Mesos user. Defaults to current system user.
	--log.level: Log level. trace|debug|info|warn|error|critical. Defaults to info.
	--framework.name: Framework name.
	--framework.role: Framework role.
    
Adding a task
-------------

    # ./cli add <type> <id>
    
Example:

    # ./cli add mirrormaker 0
    
Options:

    --api: API host:port for advertizing.
    --cpu: CPUs per task. Defaults to 0.5.
	--mem: Mem per task. Defaults to 512.
    
Updating task configuration
---------------------------

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