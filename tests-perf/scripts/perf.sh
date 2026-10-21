#!/bin/bash
# ./perf.sh <label> <repeats> [scenarios...]   extra harness flags via PERF_OPTS="-Dperf.work=50 ...", IMPLS="revelator disruptor raw"
# One JVM per (impl, scenario, repeat); impl order alternates between repeats.
LABEL=$1; REP=$2; shift 2
SCEN=${*:-UNICAST PIPELINE3 PIPELINE3_1T MULTICAST3 DIAMOND SHARDED4}
JAVA=${JAVA:-/usr/lib/jvm/jdk-27-oracle-x64/bin/java}
cd ~/rev2; OUT=results/$LABEL; mkdir -p $OUT
for r in $(seq 1 $REP); do
  # rotate the implementation order between repeats
  read -ra ALL <<< "${IMPLS:-revelator disruptor raw}"
  K=$(( (r - 1) % ${#ALL[@]} )); ORDER="${ALL[*]:K} ${ALL[*]:0:K}"
  for s in $SCEN; do for impl in $ORDER; do
    # JVM on the housekeeping CPUs of node 1, memory on node 1; stage threads pin themselves to isolated CPUs
    taskset -c ${CPUS:-1,3} numactl --membind=1 $JAVA --enable-native-access=ALL-UNNAMED \
      -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -Xlog:gc:file=$OUT/gc-$impl-$s-$r.log \
      -Dperf.impl=$impl -Dperf.scenario=$s -Dperf.label=$r -Dperf.out=$OUT/results.csv $PERF_OPTS \
      -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$impl-$s-$r.log 2>&1
    echo "$(date +%T) $r $s $impl exit=$?"
  done; done
done
python3 summarize.py $OUT/results.csv > $OUT/summary.md
echo perf-done
