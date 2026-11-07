#!/bin/bash
# A/B of revelator a7adc42 (baseline) vs 0.5 development (candidate): cursor b1 and header-commit aligned.
# Usage: ab_release.sh <out>, next to baseline/lib and candidate/lib (all jars of tests-perf each); CPUs as on rhel-lat
# with HT on: JVM on 1,3, threads on the isolated 5..15. Summary: python3 ab_summarize.py <out>/results.csv
cd "$(dirname "$0")"
OUT=${1:-results}
mkdir -p "$OUT"
CPUS6=5,7,9,11,13,15
run() { # variant label scenario cpus extra...
  local v=$1 label=$2 sc=$3 cpus=$4; shift 4
  taskset -c 1,3 numactl --membind=1 java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g -XX:+AlwaysPreTouch \
    -Dperf.impl=revelator -Dperf.cpus=$cpus -Dperf.scenario=$sc -Dperf.seconds=3 -Dperf.bursts=3 -Dperf.burst=5000000 \
    -Dperf.label=$v-$label -Dperf.out=$OUT/results.csv "$@" \
    -cp "$v/lib/*" exchange.core2.revelator.perf.PerfMain > $OUT/$v-$label-$sc.log 2>&1
  echo "$(date +%T) $v $label $sc exit=$?"
}
for rep in 1 2 3; do
  if (( rep % 2 )); then order="baseline candidate"; else order="candidate baseline"; fi
  for v in $order; do
    run $v cursor UNICAST 5,7 -Dperf.publication=cursor -Dperf.batch=1 -Dperf.rates=1,5,10
    run $v cursor PIPELINE3_1T 5,7 -Dperf.publication=cursor -Dperf.batch=1 -Dperf.rates=1,5,10,15
    run $v cursor SHARDED4 $CPUS6 -Dperf.publication=cursor -Dperf.batch=1 -Dperf.rates=1,5
    run $v hdr16 SHARDED4 $CPUS6 -Dperf.publication=header -Dperf.frames=aligned -Dperf.batch=16 -Dperf.rates=5,10,20
    run $v hdr1 MULTICAST $CPUS6 -Dperf.publication=header -Dperf.frames=aligned -Dperf.batch=1 -Dperf.readers=5 -Dperf.rates=5,10
  done
done
echo ab-done
