package main

import (
    marathon "github.com/gambol99/go-marathon"
    "os"
    "fmt"
    "flag"
    "strings"
)

func main() {
    defineFlags()
    appName := flag.String("app.name", "/go-kafka-consumers", "Application name")
    marathonUrl := flag.String("marathon.url", "http://192.168.3.5:8080", "Marathon URL")
    dockerRegistryAddress := flag.String("docker.registry.address", "master:5000", "Docker registry address host:port")
    cpu := flag.Float64("cpu", 0.5, "CPU allocation")
    memory := flag.Float64("memory", 256, "Memory allocation")
    storage := flag.Float64("storage", 256, "Storage allocation")
    instances := flag.Int("instances", 1, "Instances to launch")

    flag.Parse()

    config := marathon.NewDefaultConfig()
    config.URL = *marathonUrl
    config.LogOutput = os.Stdout
    fmt.Println(config.URL)

    if client, err := marathon.NewClient(config); err != nil {
        panic(fmt.Sprintf("Failed to create a client for marathon, error: %s", err))
    } else {
        application := marathon.NewDockerApplication()
        application.Name(*appName)
        application.CPU(float32(*cpu)).Memory(float32(*memory)).Storage(float32(*storage)).Count(*instances)
        flag.Visit(func(f *flag.Flag){
            application.AddEnv(strings.ToUpper(f.Name), f.Value.String())
        })

        // add the docker container
        application.Container.Docker.Container(fmt.Sprintf("%s/go_kafka_consumers:latest", *dockerRegistryAddress))

        if err := client.CreateApplication(application, true); err != nil {
            fmt.Printf("Failed to create application: %s, error: %s", application, err)
        } else {
            fmt.Printf("Created the application: %s", application)
        }
    }
}

func defineFlags() {
    flag.String("log_level", "", "log_level description")
    flag.String("num_consumers", "", "num_consumers description")
    flag.String("zookeeper_timeout", "", "zookeeper_timeout description")

    flag.String("num_workers", "", "num_workers description")
    flag.String("max_worker_retries", "", "max_worker_retries description")
    flag.String("worker_backoff", "", "worker_backoff description")
    flag.String("worker_retry_threshold", "", "worker_retry_threshold description")
    flag.String("worker_considered_failed_time_window", "", "worker_considered_failed_time_window description")
    flag.String("worker_task_timeout", "", "worker_task_timeout description")
    flag.String("worker_managers_stop_timeout", "", "worker_managers_stop_timeout description")

    flag.String("rebalance_barrier_timeout", "", "rebalance_barrier_timeout description")
    flag.String("rebalance_max_retries", "", "rebalance_max_retries description")
    flag.String("rebalance_backoff", "", "rebalance_backoff description")
    flag.String("partition_assignment_strategy", "", "partition_assignment_strategy description")
    flag.String("exclude_internal_topics", "", "exclude_internal_topics description")

    flag.String("num_consumer_fetchers", "", "num_consumer_fetchers description")
    flag.String("fetch_batch_size", "", "fetch_batch_size description")
    flag.String("fetch_message_max_bytes", "", "fetch_message_max_bytes description")
    flag.String("fetch_min_bytes", "", "fetch_min_bytes description")
    flag.String("fetch_batch_timeout", "", "fetch_batch_timeout description")
    flag.String("requeue_ask_next_backoff", "", "requeue_ask_next_backoff description")
    flag.String("fetch_wait_max_ms", "", "fetch_wait_max_ms description")
    flag.String("socket_timeout", "", "socket_timeout description")
    flag.String("queued_max_messages", "", "queued_max_messages description")
    flag.String("refresh_leader_backoff", "", "refresh_leader_backoff description")
    flag.String("fetch_metadata_retries", "", "fetch_metadata_retries description")
    flag.String("fetch_metadata_backoff", "", "fetch_metadata_backoff description")

    flag.String("offsets_commit_max_retries", "", "offsets_commit_max_retries description")

    flag.String("flush_interval", "", "flush_interval description")
    flag.String("deployment_timeout", "", "deployment_timeout description")
    flag.String("zookeeper_connect", "", "zookeeper_connect description")

    flag.String("group_id", "", "group_id description")
    flag.String("topic", "", "topic description")
}