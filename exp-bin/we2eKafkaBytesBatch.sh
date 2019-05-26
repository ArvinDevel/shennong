#!/bin/sh
replica=$1
path=$2
./resetKafkaTopics.sh $replica 10bytes
./rwCustBytesOverOneKafkaStream.sh 10bytes 1 "1 2 4 8" -1 /home/arvin/exp-schemas/binary.avsc 10 100 $path/reade2e/10

./resetKafkaTopics.sh $replica 100bytes
./rwCustBytesOverOneKafkaStream.sh 100bytes 1 "1 2 4 8" -1 /home/arvin/exp-schemas/binary.avsc 100 100 $path/reade2e/100

./resetKafkaTopics.sh $replica 1kbytes
./rwCustBytesOverOneKafkaStream.sh 1kbytes 1 "1 2 4 8" -1 /home/arvin/exp-schemas/binary.avsc 1000 100 $path/reade2e/1k

./resetKafkaTopics.sh $replica 10kbytes
./rwCustBytesOverOneKafkaStream.sh 10kbytes 1 "1 2 4 8" -1 /home/arvin/exp-schemas/binary.avsc 10000 100 $path/reade2e/10k
