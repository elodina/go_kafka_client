# siesta
A low level Apache Kafka library in Go

[![Build Status](https://travis-ci.org/elodina/siesta.svg?branch=master)](https://travis-ci.org/elodina/siesta)

***Installation:***

1. Install Golang [http://golang.org/doc/install](http://golang.org/doc/install)
2. Make sure env variables GOPATH and GOROOT exist and point to correct places
3. `go get github.com/elodina/siesta`
4. `go test -v` to make sure it works

You may also want to spin up a local broker at `localhost:9092` for the functional test to work as well (it will be skipped otherwise).
