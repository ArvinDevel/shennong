#!/bin/sh
BINDIR=`dirname "$0"`
SN_HOME=`cd ${BINDIR}/..;pwd`

source ${SN_HOME}/bin/common.sh
source ${SN_HOME}/conf/sn_cli_env.sh

BENCH_MODULE_PATH=bench
BENCH_MODULE_NAME="(me.jinsui.shennong-)?streamstorage-bench"
BENCH_MODULE_HOME=${SN_HOME}/${BENCH_MODULE_PATH}

# find the module jar
BENCH_JAR=$(find_module_jar ${BENCH_MODULE_PATH} ${BENCH_MODULE_NAME})

# set up the classpath
BENCH_CLASSPATH=$(set_module_classpath ${BENCH_MODULE_PATH})


DEFAULT_LOG_CONF=${SN_HOME}/conf/log4j.bench.properties
if [ -z "${BENCH_LOG_CONF}" ]; then
  BENCH_LOG_CONF=${DEFAULT_LOG_CONF}
fi
BENCH_LOG_DIR=${BENCH_LOG_DIR:-"$SN_HOME/logs"}
BENCH_LOG_FILE=${BENCH_LOG_FILE:-"ssbench.log"}
BENCH_ROOT_LOGGER=${BENCH_ROOT_LOGGER:-"INFO,ROLLINGFILE"}

# Configure the classpath
BENCH_CLASSPATH="$BENCH_JAR:$BENCH_CLASSPATH:$BENCH_EXTRA_CLASSPATH"
BENCH_CLASSPATH="`dirname $BENCH_LOG_CONF`:$BENCH_CLASSPATH"

# Build the OPTs
GC_OPTS=$(build_cli_jvm_opts ${BENCH_LOG_DIR} "ssbench-gc.log")
LOGGING_OPTS=$(build_cli_logging_opts ${BENCH_LOG_CONF} ${BENCH_LOG_DIR} ${BENCH_LOG_FILE} ${BENCH_ROOT_LOGGER})

OPTS="${OPTS} -cp ${BENCH_CLASSPATH} ${GC_OPTS} ${LOGGING_OPTS} ${BENCH_EXTRA_OPTS}"

#Change to SN_HOME to support relative paths
cd "$SN_HOME"
exec ${JAVA} ${OPTS} me.jinsui.shennong.bench.exp.VerifySSE $@