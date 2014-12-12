go_kafka_client
===============

Apache Kafka Client Library for Go

[![Build Status](https://travis-ci.org/stealthly/go_kafka_client.svg?branch=master)](https://travis-ci.org/stealthly/go_kafka_client)

***Prerequisites:***

1. Install Golang [http://golang.org/doc/install](http://golang.org/doc/install)
2. Make sure env variables GOPATH and GOROOT exist and point to correct places
3. Install GPM [https://github.com/pote/gpm](https://github.com/pote/gpm)
4. `go get github.com/stealthly/go_kafka_client && cd $GOPATH/src/github.com/stealthly/go_kafka_client`
5. `gpm install`

*Optional (for all tests to work):*

1. Install Docker [https://docs.docker.com/installation/#installation](https://docs.docker.com/installation/#installation)
2. `cd $GOPATH/src/github.com/stealthly/go_kafka_client`
3. Build docker image: `docker build -t stealthly/go_kafka_client .`
4. `docker run -v $(pwd):/go_kafka_client stealthly/go_kafka_client`

After this is done you're ready to write some code!

For email support https://groups.google.com/forum/#!forum/kafka-clients

