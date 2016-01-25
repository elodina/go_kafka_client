Go Kafka Client on Mesos using Docker
====================================

```
# cd $GOPATH/src/github.com/stealthly/go_kafka_client
# sudo docker build -t stealthly/go_kafka_client-mesos --file Dockerfile.mesos .
# docker run --net=host -i -t stealthly/go_kafka_client-mesos ./cli scheduler --master 192.168.3.5:5050 --log.level debug --api http://192.168.3.1:6666
```