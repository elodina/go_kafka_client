Go Kafka Client
===============

The Apache Kafka Client Library for Go is sponsored by [CrowdStrike] (http://www.crowdstrike.com/) and developed by [Big Data Open Source Security LLC] (http://stealth.ly)

[![Build Status](https://travis-ci.org/stealthly/go_kafka_client.svg?branch=master)](https://travis-ci.org/stealthly/go_kafka_client)

***Ideas and goals behind the Go Kafka Client:***
 

***1) Partition Ownership***

We decided on implementing multiple strategies for this including static assignment. The concept of re-balancing is preserved but now there are a few different strategies to re-balancing and they can run at different times depending on what is going on (like a blue/green deploy is happening). For more on blue/green deployments check out [this video](https://www.youtube.com/watch?v=abK2Q_aecxY).
 
***2) Fetch Management***

This is what “fills up the reservoir” as I like to call it so the processing (either sequential or in batch) will always have data if there is data for it to have without making a network hop. The fetcher has to stay ahead here keeping the processing tap full (or if empty that is controlled) pulling the data for the Kafka partition(s) it is owning.
 
***3) Work Management***

For the Go consumer we currently only support “fan out” using go routines and channels. If you have ever used go this will be familiar to you if not you should drop everything and learn Go.
 
***4) Offset Management***

Our offset management is based on a per batch basis with each highest offset from the batch committed on a per partition basis.

***Prerequisites:***

1. Install Golang [http://golang.org/doc/install](http://golang.org/doc/install)
2. Make sure env variables GOPATH and GOROOT exist and point to correct places
3. Install GPM [https://github.com/pote/gpm](https://github.com/pote/gpm)
4. `mkdir -p $GOPATH/src/github.com/stealthly && cd $GOPATH/src/github.com/stealthly`
5. `git clone https://github.com/stealthly/go_kafka_client.git && cd go_kafka_client`
6. `gpm install`

*Optional (for all tests to work):*

1. Install Docker [https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)
2. `cd $GOPATH/src/github.com/stealthly/go_kafka_client`
3. Build docker image: `docker build -t stealthly/go_kafka_client .`
4. `docker run -v $(pwd):/go_kafka_client stealthly/go_kafka_client`

After this is done you're ready to write some code!

For email support https://groups.google.com/forum/#!forum/kafka-clients

***Related docs:***

1. [Offset Storage configuration](https://github.com/stealthly/go_kafka_client/blob/master/docs/offset_storage.md).
2. [Log and metrics emitters](https://github.com/stealthly/go_kafka_client/blob/master/docs/emitters.md).
