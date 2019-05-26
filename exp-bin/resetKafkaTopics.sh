#!/bin/sh
function deleteTopics()
{
tps=`kafka_2.12-2.1.0/bin/kafka-topics.sh  --zookeeper hw012:2181  --list`
for t in $tps;
do
kafka_2.12-2.1.0/bin/kafka-topics.sh  --zookeeper hw012:2181 --topic $t --delete
done
}
function createTopics()
{
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000000
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000001
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000002
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000003
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000004
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000005
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000006
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000007
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000008
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000009
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000010
kafka_2.12-2.1.0/bin/kafka-topics.sh --create --zookeeper hw012:2181 --replication-factor $1 --partitions 1 --topic kafka-write-$tbl-000011
}
replica=$1
tbl=$2
deleteTopics
createTopics $replica

