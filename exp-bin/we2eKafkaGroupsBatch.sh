#!/bin/sh
replica=$1
path=$2
./resetKafkaTopics.sh $replica 1group
./rwCustBytesOverOneKafkaStream.sh 1group 1 "1 2 4 8" -1 /home/arvin/exp-schemas/1group.avsc 10 1000 $path/reade2e/1

./resetKafkaTopics.sh $replica 2group
./rwCustBytesOverOneKafkaStream.sh 2group 1 "1 2 4 8" -1 /home/arvin/exp-schemas/2group.avsc 10 1000 $path/reade2e/2

./resetKafkaTopics.sh $replica 10group
./rwCustBytesOverOneKafkaStream.sh 10group 1 "1 2 4 8" -1 /home/arvin/exp-schemas/10group.avsc 10 1000 $path/reade2e/10

./resetKafkaTopics.sh $replica 100group
./rwCustBytesOverOneKafkaStream.sh 100group 1 "1 2 4 8" -1 /home/arvin/exp-schemas/100group.avsc 10 1000 $path/reade2e/100

