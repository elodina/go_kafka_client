#!/bin/sh

cd go_kafka_client

gpm install

cd consumers

go get github.com/stealthly/go_kafka_client
go build

cp consumers $CLIENT_PATH
cp seelog.xml $CLIENT_PATH
cp consumers.properties $CLIENT_PATH
