#!/bin/sh
sed -r -i "s/(client_id)=(.*)/\1=$CLIENT_ID/g" consumers.properties
sed -r -i "s/(group_id)=(.*)/\1=$GROUP_ID/g" consumers.properties
sed -r -i "s/(num_consumers)=(.*)/\1=$NUM_CONSUMERS/g" consumers.properties
sed -r -i "s/(topic)=(.*)/\1=$TOPIC/g" consumers.properties
sed -r -i "s/(log_level)=(.*)/\1=$LOG_LEVEL/g" consumers.properties
sed -r -i "s/(zookeeper_connect)=(.*)/\1=$ZOOKEEPER_CONNECT/g" consumers.properties
sed -r -i "s/(zookeeper_timeout)=(.*)/\1=$ZOOKEEPER_TIMEOUT/g" consumers.properties
sed -r -i "s/(num_workers)=(.*)/\1=$NUM_WORKERS/g" consumers.properties
sed -r -i "s/(max_worker_retries)=(.*)/\1=$MAX_WORKER_RETRIES/g" consumers.properties
sed -r -i "s/(worker_backoff)=(.*)/\1=$WORKER_BACKOFF/g" consumers.properties
sed -r -i "s/(worker_retry_threshold)=(.*)/\1=$WORKER_RETRY_THRESHOLD/g" consumers.properties
sed -r -i "s/(worker_considered_failed_time_window)=(.*)/\1=$WORKER_CONSIDERED_FAILED_TIME_WINDOW/g" consumers.properties
sed -r -i "s/(worker_batch_timeout)=(.*)/\1=$WORKER_BATCH_TIMEOUT/g" consumers.properties
sed -r -i "s/(worker_task_timeout)=(.*)/\1=$WORKER_TASK_TIMEOUT/g" consumers.properties
sed -r -i "s/(worker_managers_stop_timeout)=(.*)/\1=$WORKER_MANAGERS_STOP_TIMEOUT/g" consumers.properties
sed -r -i "s/(rebalance_barrier_timeout)=(.*)/\1=$REBALANCE_BARRIER_TIMEOUT/g" consumers.properties
sed -r -i "s/(rebalance_max_retries)=(.*)/\1=$REBALANCE_MAX_RETRIES/g" consumers.properties
sed -r -i "s/(rebalance_backoff)=(.*)/\1=$REBALANCE_BACKOFF/g" consumers.properties
sed -r -i "s/(partition_assignment_strategy)=(.*)/\1=$PARTITION_ASSIGNMENT_STRATEGY/g" consumers.properties
sed -r -i "s/(exclude_internal_topics)=(.*)/\1=$EXCLUDE_INTERNAL_TOPICS/g" consumers.properties
sed -r -i "s/(num_consumer_fetchers)=(.*)/\1=$NUM_CONSUMER_FETCHERS/g" consumers.properties
sed -r -i "s/(fetch_batch_size)=(.*)/\1=$FETCH_BATCH_SIZE/g" consumers.properties
sed -r -i "s/(fetch_message_max_bytes)=(.*)/\1=$FETCH_MESSAGE_MAX_BYTES/g" consumers.properties
sed -r -i "s/(fetch_min_bytes)=(.*)/\1=$FETCH_MIN_BYTES/g" consumers.properties
sed -r -i "s/(fetch_batch_timeout)=(.*)/\1=$FETCH_BATCH_TIMEOUT/g" consumers.properties
sed -r -i "s/(requeue_ask_next_backoff)=(.*)/\1=$REQUEUE_ASK_NEXT_BACKOFF/g" consumers.properties
sed -r -i "s/(fetch_wait_max_ms)=(.*)/\1=$FETCH_WAIT_MAX_MS/g" consumers.properties
sed -r -i "s/(socket_timeout)=(.*)/\1=$SOCKET_TIMEOUT/g" consumers.properties
sed -r -i "s/(queued_max_messages)=(.*)/\1=$QUEUED_MAX_MESSAGES/g" consumers.properties
sed -r -i "s/(refresh_leader_backoff)=(.*)/\1=$REFRESH_LEADER_BACKOFF/g" consumers.properties
sed -r -i "s/(fetch_metadata_retries)=(.*)/\1=$FETCH_METADATA_RETRIES/g" consumers.properties
sed -r -i "s/(fetch_metadata_backoff)=(.*)/\1=$FETCH_METADATA_BACKOFF/g" consumers.properties
sed -r -i "s/(offsets_storage)=(.*)/\1=$OFFSETS_STORAGE/g" consumers.properties
sed -r -i "s/(auto_offset_reset)=(.*)/\1=$AUTO_OFFSET_RESET/g" consumers.properties
sed -r -i "s/(offsets_commit_max_retries)=(.*)/\1=$OFFSETS_COMMIT_MAX_RETRIES/g" consumers.properties
sed -r -i "s/(deployment_timeout)=(.*)/\1=$DEPLOYMENT_TIMEOUT/g" consumers.properties
sed -r -i "s/(graphite_connect)=(.*)/\1=$GRAPHITE_CONNECT/g" consumers.properties
sed -r -i "s/(flush_interval)=(.*)/\1=$FLUSH_INTERVAL/g" consumers.properties

