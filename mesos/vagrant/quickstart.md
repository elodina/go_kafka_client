Go Kafka Client on Mesos quickstart using Vagrant
================================================

This quickstart guide assumes you have no Mesos cluster running anywhere. This also assumes you have Zookeeper and Kafka running somewhere. **Please make sure your brokers advertise themselves properly and are accessible from Vagrant VMs**

Step by step guide:

* Clone and build the project

```
# git clone https://github.com/stealthly/go_kafka_client.git
# cd go_kafka_client
# godep restore ./...
# cd mesos
# go build cli.go
# go build executor.go
```

* Modify `Vagrantfile` if necessary. Default configuration will spin 2 VMs - one with Mesos Master and Mesos Slave, the second with Mesos Slave alone. This step is optional. More details on Vagrant VMs [here](https://github.com/stealthly/go_kafka_client/tree/mesos-framework/mesos/vagrant).
* ```vagrant up```
* ```vagrant ssh master```
* ```cd /vagrant/mesos```
* Modify `producer.config` and `consumer.config` to point to your Zookeeper and Kafka if necessary. Default configurations assume you have Zookeeper and Kafka running on localhost (192.168.3.1 should be accessible from `master` VM)
* Now you may follow quick start steps decribed [here](https://github.com/stealthly/go_kafka_client/tree/mesos-framework/mesos#quick-start) to start the scheduler and add mirror maker tasks.