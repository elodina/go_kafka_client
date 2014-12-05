go_kafka_client
===============

Apache Kafka Client Library for Go

***Prerequisites:***

1. Install Golang [http://golang.org/doc/install](http://golang.org/doc/install)
2. Make sure env variables GOPATH and GOROOT exist and point to correct places
3. Install GPM [https://github.com/pote/gpm](https://github.com/pote/gpm)
4. go get github.com/stealthly/go_kafka_client && cd $GOPATH/src/github.com/stealthly/go_kafka_client
5. gpm install

*Optional (for all tests to work):*

1. Download Apache Zookeeper [http://zookeeper.apache.org/releases.html#download](http://zookeeper.apache.org/releases.html#download)
2. Add environment variable ZOOKEEPER_PATH poiting to the root of Zookeeper distribution
3. Download Apache Kafka [http://kafka.apache.org/downloads.html](http://kafka.apache.org/downloads.html). *Note: we used kafka_2.10-0.8.1.1 for this development
4. Add environment variable KAFKA_PATH pointing to the root of Kafka distribution
5. cd $GOPATH/src/github.com/stealthly/go_kafka_client && go test -v
6. All tests should pass 

After this is done you're ready to write some code!