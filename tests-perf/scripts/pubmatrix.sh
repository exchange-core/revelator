#!/bin/bash
# ./pubmatrix.sh <label>: publication modes and batching, all configurations interleaved per scenario
LABEL=$1
CONFIGS=${CONFIGS:-"revelator:cursor:1 revelator:cursor:16 revelator:header:1 revelator:header:16 disruptor:cursor:1 disruptor:cursor:16 raw:cursor:1 raw:cursor:16 raw:header:1 raw:header:16"}
SCEN=${SCEN:-"UNICAST PIPELINE3 PIPELINE3_1T MULTICAST3 DIAMOND SHARDED4"}
JAVA=${JAVA:-/usr/lib/jvm/jdk-27-oracle-x64/bin/java}
cd ~/rev2; OUT=results/$LABEL; mkdir -p $OUT
for s in $SCEN; do for c in $CONFIGS; do
  IFS=: read -r impl pub batch <<< "$c"
  taskset -c ${CPUS:-1,3} numactl --membind=1 $JAVA --enable-native-access=ALL-UNNAMED \
    -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages \
    -Dperf.impl=$impl -Dperf.scenario=$s -Dperf.publication=$pub -Dperf.batch=$batch -Dperf.label=$pub-b$batch \
    -Dperf.out=$OUT/results.csv $PERF_OPTS -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$impl-$pub-b$batch-$s.log 2>&1
  echo "$(date +%T) $s $c exit=$?"
done; done
python3 summarize2.py $OUT/results.csv > $OUT/summary.md
echo perf-done
