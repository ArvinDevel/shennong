#!/bin/sh
replica=$1
path=$2
# del and create topics with replica
./resetKafkaTopics.sh $replica cutbytes
# write with 10 bytes single field
./writeCustomOverKafkaOneStream.sh cutbytes "1 2 4 8 12" 1000 /home/arvin/exp-schemas/binary.avsc 10 $path/rw/10

# read head
./readCustomOverOneKafkaStream.sh cutbytes "1 2 4 8 12" 0 200 /home/arvin/exp-schemas/binary.avsc $path/rw/10/readHead

./resetKafkaTopics.sh $replica cutbytes
./writeCustomOverKafkaOneStream.sh cutbytes "1 2 4 8 12" 1000 /home/arvin/exp-schemas/binary.avsc 100 $path/rw/100
# read head
./readCustomOverOneKafkaStream.sh cutbytes "1 2 4 8 12" 0 200 /home/arvin/exp-schemas/binary.avsc $path/rw/100/readHead

./resetKafkaTopics.sh $replica cutbytes
./writeCustomOverKafkaOneStream.sh cutbytes "1 2 4 8 12" 1000 /home/arvin/exp-schemas/binary.avsc 1000 $path/rw/1000
# read head
./readCustomOverOneKafkaStream.sh cutbytes "1 2 4 8 12" 0 200 /home/arvin/exp-schemas/binary.avsc $path/rw/1000/readHead

./resetKafkaTopics.sh $replica cutbytes
./writeCustomOverKafkaOneStream.sh cutbytes "1 2 4 8 12" 1000 /home/arvin/exp-schemas/binary.avsc 10000 $path/rw/10000
# read head
./readCustomOverOneKafkaStream.sh cutbytes "1 2 4 8 12" 0 200 /home/arvin/exp-schemas/binary.avsc $path/rw/10000/readHead

