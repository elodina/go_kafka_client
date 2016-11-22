FROM       golang:1.4
MAINTAINER Michael Nikitochkin <nikitochkin.michael@gmail.com>
RUN        apt-get -qy update && \
           rm -rf /var/lib/apt/lists/*

ADD        . /go/src/github.com/elodina/go_kafka_client
WORKDIR    /go/src/github.com/elodina/go_kafka_client

RUN        go get github.com/tools/godep
RUN        godep restore
RUN        go install -v ./mirrormaker

VOLUME     /mirrormaker
ENTRYPOINT ["/go/bin/mirrormaker"]
CMD        ["--help"]
