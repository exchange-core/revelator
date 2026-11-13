#!/bin/bash
# ./perf.sh <label> <repeats> [scenarios...]   extra harness flags via PERF_OPTS="-Dperf.work=50 ...", IMPLS="revelator disruptor raw"
# One JVM per (impl, scenario, repeat); impl order alternates between repeats.
LABEL=$1; REP=$2; shift 2
SCEN=${*:-UNICAST PIPELINE3 PIPELINE3_1T MULTICAST3 DIAMOND DOUBLE_DIAMOND SHARDED4}
# DOUBLE_DIAMOND needs 7 CPUs: a stage takes CPU 3, so its JVM moves to 1 and the sibling 17 (rhel-lat, HT on)
DD_CPUS=${DD_CPUS:-15,13,11,9,7,5,3}; DD_JVM=${DD_JVM:-1,17}
JAVA=${JAVA:-/usr/lib/jvm/jdk-27-oracle-x64/bin/java}
# REV_DIR: dir with lib/ and summarize.py; PLACE: JVM placement prefix (default: housekeeping CPUs + memory of node 1)
cd ${REV_DIR:-~/rev2}; OUT=results/$LABEL; mkdir -p $OUT
for r in $(seq 1 $REP); do
  # rotate the implementation order between repeats
  read -ra ALL <<< "${IMPLS:-revelator disruptor raw}"
  K=$(( (r - 1) % ${#ALL[@]} )); ORDER="${ALL[*]:K} ${ALL[*]:0:K}"
  for s in $SCEN; do for impl in $ORDER; do
    JCPUS=${CPUS:-1,3}; SOPTS=
    if [ $s = DOUBLE_DIAMOND ]; then JCPUS=$DD_JVM; SOPTS=-Dperf.cpus=$DD_CPUS; fi
    # JVM on the housekeeping CPUs of node 1, memory on node 1; stage threads pin themselves to isolated CPUs
    # (PERF_OPTS comes last, so its -Dperf.cpus wins over SOPTS)
    ${PLACE:-taskset -c $JCPUS numactl --membind=1} $JAVA --enable-native-access=ALL-UNNAMED \
      -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -Xlog:gc:file=$OUT/gc-$impl-$s-$r.log \
      -Dperf.impl=$impl -Dperf.scenario=$s -Dperf.label=$r -Dperf.out=$OUT/results.csv $SOPTS $PERF_OPTS \
      -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$impl-$s-$r.log 2>&1
    echo "$(date +%T) $r $s $impl exit=$?"
  done; done
done
python3 summarize.py $OUT/results.csv > $OUT/summary.md
echo perf-done
