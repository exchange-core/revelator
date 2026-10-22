#!/bin/bash
# samples for charts: SHARDED4 equal / slow shard, results in event vs columns
cd ~/rev2; OUT=results/charts1; mkdir -p $OUT
JAVA=/usr/lib/jvm/jdk-27-oracle-x64/bin/java
run() { # <samples subdir> <series> <scenario> <rates> <flags...>
  local dir=$1 series=$2 scen=$3 rates=$4; shift 4
  taskset -c 1,3 numactl --membind=1 $JAVA --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages \
    -Dperf.scenario=$scen -Dperf.rates=$rates -Dperf.seconds=2 -Dperf.bursts=1 -Dperf.burst=5000000 \
    -Dperf.samples=$OUT/samples/$dir -Dperf.series=$series -Dperf.label=$dir -Dperf.out=$OUT/results.csv "$@" \
    -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$dir-$series-$scen.log 2>&1
  echo "$(date +%T) $dir $series $scen exit=$?"
}
for r in 1 2; do
  run equal revelator SHARDED4 1,5 -Dperf.impl=revelator
  run equal disruptor SHARDED4 1,5 -Dperf.impl=disruptor
  run equal raw SHARDED4 1,5 -Dperf.impl=raw
  run equal revelator-header SHARDED4 1,5 -Dperf.impl=revelator -Dperf.publication=header
  [ $r = 2 ] && break
done
for impl in revelator disruptor raw; do
  run slow $impl SHARDED4 1,2 -Dperf.impl=$impl -Dperf.slowShard=0 -Dperf.slowWork=1000
done
for s in PIPELINE3 DIAMOND; do for impl in disruptor revelator raw; do
  run results $impl $s 1,5 -Dperf.impl=$impl -Dperf.results=true
done; done
echo charts-done
