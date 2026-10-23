#!/bin/bash
# E5 samples: SHARDED4 join in order vs with bypass (ReorderingFlowProcessor), slow shard and equal shards
cd ~/rev2; OUT=results/e5; mkdir -p $OUT
JAVA=/usr/lib/jvm/jdk-27-oracle-x64/bin/java
run() { # <samples subdir> <series> <rates> <flags...>
  local dir=$1 series=$2 rates=$3; shift 3
  taskset -c 1,3 numactl --membind=1 $JAVA --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages \
    -Dperf.scenario=SHARDED4 -Dperf.rates=$rates -Dperf.seconds=2 -Dperf.bursts=1 -Dperf.burst=5000000 \
    -Dperf.samples=$OUT/samples/$dir -Dperf.series=$series -Dperf.label=$dir-$series -Dperf.out=$OUT/results.csv "$@" \
    -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$dir-$series.log 2>&1
  echo "$(date +%T) $dir $series exit=$? $(grep -c ^latency $OUT/$dir-$series.log)"
}
SLOW="-Dperf.slowShard=0 -Dperf.slowWork=700"
run slow revelator 1,2,3 -Dperf.impl=revelator $SLOW
run slow revelator-bypass 1,2,3 -Dperf.impl=revelator -Dperf.bypass=true $SLOW
run slow disruptor 1,2,3 -Dperf.impl=disruptor $SLOW
run slow raw 1,2,3 -Dperf.impl=raw $SLOW
run equal revelator 1,5 -Dperf.impl=revelator
run equal revelator-bypass 1,5 -Dperf.impl=revelator -Dperf.bypass=true
echo e5-done
