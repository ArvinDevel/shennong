#!/bin/sh
# as $tbl is too big, use sf = 10, others use 100
sf=10
# url
u=kafka://hw013:9092
# tpch table name
tbl=$1
writeprocesses=$2
readprocesses=$3
consumPosition=$4
schemafile=$5
bytesize=$6
# max backoff number
mbn=8
runtime=$7
path=$8
function multipleProcessWrite()
{
   echo "enter multipleProcessWrite()"
   for((i=0;i<$1;i++));
   do
	order=0
	echo "./shennong-0.1.0-SNAPSHOT/bin/ssbench.sh write kafka -u $u -r $rate -tn $tn -tnum 1 -t 1 -so $order -sf $schemafile -bytessize $bytesize > write-kafka-$tbl-$i.log &"
	`./shennong-0.1.0-SNAPSHOT/bin/ssbench.sh write kafka -u $u -r $rate -tn $tn -tnum 1 -t 1 -so $order -sf $schemafile -bytessize $bytesize > write-kafka-$tbl-$i.log &`
   done
}
function multipleProcessRead()
{
   echo "enter multipleProcessRead()"
   for((i=0;i<$1;i++));
   do
	order=0
	echo "./shennong-0.1.0-SNAPSHOT/bin/ssbench.sh read kafka -u $u -sf $schemafile -cp $consumPosition -tn $tn -tnum 1 -t 1 -so $order -re 0 -mbn $mbn > read-kafka-$tbl-$i.log &"
	`./shennong-0.1.0-SNAPSHOT/bin/ssbench.sh read kafka -u $u  -sf $schemafile -cp $consumPosition -tn $tn -tnum 1 -t 1 -so $order -re 0 -mbn $mbn > read-kafka-$tbl-$i.log &`
   done
}
# options changed during different cycle
# 40 mb/s
rate=40000000
# stream name pattern
tn=kafka-write-$tbl-%06d

function killBenchAndRecordResult() 
{
jps | grep StreamStorageBench | awk '{print $1}' | xargs kill
sleep 5
`./extractKafkaWriteResult.sh`
`./extractKafkaReadE2eResult.sh`
mkdir -p $1
mv write-kafka-$tbl-* $1
mv kafka-write-total.log $1
mv read-kafka-$tbl-* $1
mv kafka-read-total.log $1
}
#only del and create when first run
#deleteTopics
#createTopics 
# write of $tbl
for wprocess in $writeprocesses; do
	for rprocess in $readprocesses; do
		multipleProcessWrite $wprocess
		sleep 5
		multipleProcessRead $rprocess
		
		sleep $runtime
		killBenchAndRecordResult $path/$wprocess-$rprocess
	done
done


