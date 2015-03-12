#!/bin/bash -Eux
#Kafka settings

wget https://storage.googleapis.com/golang/go1.4.linux-amd64.tar.gz
tar -xvf go1.4.linux-amd64.tar.gz 
mv go /usr/local

export KAFKA_VERSION=0.8.2.1
echo "export KAFKA_VERSION=$KAFKA_VERSION">> /home/vagrant/.bashrc
echo "export KAFKA_VERSION=$KAFKA_VERSION">> /root/.bashrc

export SCALA_VERSION=2.10
echo "export SCALA_VERSION=$SCALA_VERSION">> /home/vagrant/.bashrc
echo "export SCALA_VERSION=$SCALA_VERSION">> /root/.bashrc

export KAFKA_RELEASE=kafka_$SCALA_VERSION-$KAFKA_VERSION
echo "export KAFKA_RELEASE=$KAFKA_RELEASE">> /home/vagrant/.bashrc
echo "export KAFKA_RELEASE=$KAFKA_RELEASE">> /root/.bashrc

export KAFKA_URL=https://people.apache.org/~junrao/kafka-0.8.2.1-candidate2/$KAFKA_RELEASE.tgz
#https://archive.apache.org/dist/kafka/$KAFKA_VERSION/$KAFKA_RELEASE.tgz
echo "export KAFKA_URL=$KAFKA_URL">> /home/vagrant/.bashrc
echo "export KAFKA_URL=$KAFKA_URL">> /root/.bashrc

export KAFKA_PATH=/opt/$KAFKA_RELEASE
echo "export KAFKA_PATH=$KAFKA_PATH">> /home/vagrant/.bashrc
echo "export KAFKA_PATH=$KAFKA_PATH">> /root/.bashrc

export BROKER_ID=1
echo "export BROKER_ID=$BROKER_ID">> /home/vagrant/.bashrc
echo "export BROKER_ID=$BROKER_ID">> /root/.bashrc

export HOST_IP=localhost
echo "export HOST_IP=$HOST_IP">> /home/vagrant/.bashrc
echo "export HOST_IP=$HOST_IP">> /root/.bashrc

export PORT=9092
echo "export PORT=$PORT">> /home/vagrant/.bashrc
echo "export PORT=$PORT">> /root/.bashrc

#Zookeeper settings
export ZK_VERSION=3.4.6
echo "export ZK_VERSION=$ZK_VERSION">> /home/vagrant/.bashrc
echo "export ZK_VERSION=$ZK_VERSION">> /root/.bashrc

export ZK_RELEASE=zookeeper-$ZK_VERSION
echo "export ZK_RELEASE=$ZK_RELEASE">> /home/vagrant/.bashrc
echo "export ZK_RELEASE=$ZK_RELEASE">> /root/.bashrc

export ZK_URL=https://archive.apache.org/dist/zookeeper/zookeeper-$ZK_VERSION/$ZK_RELEASE.tar.gz
echo "export ZK_URL=$ZK_URL">> /home/vagrant/.bashrc
echo "export ZK_URL=$ZK_URL">> /root/.bashrc

export ZK_HOME=/opt/$ZK_RELEASE
echo "export ZK_HOME=$ZK_HOME">> /home/vagrant/.bashrc
echo "export ZK_HOME=$ZK_HOME">> /root/.bashrc

#Go settings
export GOLANG_VERSION=1.3.3
echo "export GOLANG_VERSION=$GOLANG_VERSION">> /home/vagrant/.bashrc
echo "export GOLANG_VERSION=$GOLANG_VERSION">> /root/.bashrc

export GOLANG_RELEASE=$GOLANG_VERSION
echo "export GOLANG_RELEASE=$GOLANG_RELEASE">> /home/vagrant/.bashrc
echo "export GOLANG_RELEASE=$GOLANG_RELEASE">> /root/.bashrc

export GOLANG_URL=https://storage.googleapis.com/golang/go$GOLANG_VERSION.linux-amd64.tar.gz
echo "export GOLANG_URL=$GOLANG_URL">> /home/vagrant/.bashrc
echo "export GOLANG_URL=$GOLANG_URL">> /root/.bashrc

export GOROOT=/usr/local/go
echo "export GOROOT=$GOROOT">> /home/vagrant/.bashrc
echo "export GOROOT=$GOROOT">> /root/.bashrc

mkdir -p /opt/go
chmod a+rw /opt/go
export GOPATH=/opt/go
echo "export GOPATH=$GOPATH">> /home/vagrant/.bashrc
echo "export GOPATH=$GOPATH">> /root/.bashrc

export PATH=$GOROOT/bin:$PATH
echo "export PATH=$PATH">> /home/vagrant/.bashrc
echo "export PATH=$PATH">> /root/.bashrc

apt-get -y update
apt-get install -y software-properties-common python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

#Get git and mercurial
sudo apt-get update
sudo apt-get -y install git mercurial make

#Get Kafka
wget -q $KAFKA_URL -O /tmp/$KAFKA_RELEASE.tgz
tar xfz /tmp/$KAFKA_RELEASE.tgz -C /opt
#Get Zookeeper
wget -q $ZK_URL -O /tmp/$ZK_RELEASE.tar.gz
tar -xzf /tmp/$ZK_RELEASE.tar.gz -C /opt
cp $ZK_HOME/conf/zoo_sample.cfg $ZK_HOME/conf/zoo.cfg
#Get Go
wget -q $GOLANG_URL -O /tmp/$GOLANG_RELEASE.tar.gz
tar -xzf /tmp/$GOLANG_RELEASE.tar.gz -C /usr/bin
mkdir -p $GOPATH/src/github.com/stealthly/go_kafka_client
cp -r /vagrant/* $GOPATH/src/github.com/stealthly/go_kafka_client

mkdir -p /opt/github.com/pote
cd /opt/github.com/pote
#Get GPM
git clone https://github.com/pote/gpm.git && cd gpm && git checkout v1.3.1 && ./configure && make install
