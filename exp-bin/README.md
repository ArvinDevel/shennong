A series shell script to execute experiments on CStream, Kafka and parquet over HDFS.
The default number of running process to exp is customized for 12 physical core machine.

## 


## Basic utils
`resetKafkaTopics` is used to clear previous data on kafka(this is necessary for small cluster)
and create topics with wanted replicas and topic name config.

`rwCustBytesOverOneKafkaStream` is used to execute kafka writing and e2e reading task using the specified config.


## Example of experiment setting
### Kafka
// kafka write and read e2e exp using one field(bytes different size) with 3 replicas
./we2eKafkaBytesBatch.sh 3 exp-kafka/bytes/5-26

// kafka write and read e2e exp using multiple field with 3 replicas
./we2eKafkaGroupsBatch.sh 3 exp-kafka/groups/5-26