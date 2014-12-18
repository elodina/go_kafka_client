package main

import (
	"flag"
	"fmt"
	"os"
	kafka "github.com/stealthly/go_kafka_client"
)

type consumerConfigs []string

func (i *consumerConfigs) String() string {
	return fmt.Sprintf("%s", *i)
}

func (i *consumerConfigs) Set(value string) error {
	*i = append(*i, value)
	return nil
}

var consumers []*kafka.Consumer

var whitelist = flag.String("whitelist", "", "regex pattern for whitelist. Providing both whitelist and blacklist is an error.")
var blacklist = flag.String("blacklist", "", "regex pattern for blacklist. Providing both whitelist and blacklist is an error.")
var consumerConfig consumerConfigs
var producerConfig = flag.String("producer.config", "", "Path to producer configuration file.")
var numProducers = flag.Int("num.producers", 1, "Number of producers.")
var numStreams = flag.Int("num.streams", 1, "Number of consumption streams.")
var preservePartitions = flag.Bool("preserve.partitions", false, "preserve partition number. E.g. if message was read from partition 5 it'll be written to partition 5.")
var prefix = flag.String("prefix", "", "Destination topic prefix.")
//TODO queue size?

func parseAndValidateArgs() {
	flag.Var(&consumerConfig, "consumer.config", "Path to consumer configuration file.")
	flag.Parse()

	if (*whitelist != "" && *blacklist != "") || (*whitelist == "" && *blacklist == "") {
		fmt.Println("Exactly one of whitelist or blacklist is required.")
		os.Exit(1)
	}
	if *producerConfig == "" {
		fmt.Println("Producer config is required.")
		os.Exit(1)
	}
	if len(consumerConfig) == 0 {
		fmt.Println("At least one consumer config is required.")
		os.Exit(1)
	}
}

func main() {
	parseAndValidateArgs()

	startConsumers()
}

func startConsumers() {

}
