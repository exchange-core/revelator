#!/bin/bash
# chart 2: latency vs offered load; chart 3: capacity vs number of readers (MULTICAST)
cd ~/rev2; OUT=results/charts23; mkdir -p $OUT
JAVA=/usr/lib/jvm/jdk-27-oracle-x64/bin/java
CONFIGS=("Revelator_cursor:revelator:cursor:1" "Revelator_cursor_batch16:revelator:cursor:16" "Revelator_header-commit:revelator:header:1"
         "Disruptor:disruptor:cursor:1" "Disruptor_batch16:disruptor:cursor:16" "raw_header-commit:raw:header:1")
run() { # <csv> <log> <scenario> <rates> <stop p99 us> <config> [flags]
  local csv=$1 log=$2 scen=$3 rates=$4 stop=$5 cfg=$6; shift 6
  IFS=: read -r name impl pub batch <<< "$cfg"
  taskset -c 1,3 numactl --membind=1 $JAVA --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages \
    -Dperf.impl=$impl -Dperf.publication=$pub -Dperf.batch=$batch -Dperf.scenario=$scen -Dperf.rates=$rates -Dperf.seconds=1 \
    -Dperf.stopAtP99Us=$stop -Dperf.bursts=1 -Dperf.burst=2000000 -Dperf.label=$name -Dperf.out=$csv "$@" \
    -cp "lib/*" exchange.core2.revelator.perf.PerfMain > $log 2>&1
  echo "$(date +%T) $scen $name $* exit=$?"
}
RATES2=0.5,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20
for s in DIAMOND MULTICAST3 SHARDED4; do for c in "${CONFIGS[@]}"; do
  run $OUT/load.csv $OUT/load-${c%%:*}-$s.log $s $RATES2 100 "$c"
done; done
RATES3=1,2,4,6,8,10,12,14,16,18,20,22,24,26,28,30
for n in 1 2 3 4 5; do for c in "${CONFIGS[@]}"; do
  run $OUT/readers-$n.csv $OUT/readers-$n-${c%%:*}.log MULTICAST $RATES3 10 "$c" -Dperf.readers=$n
done; done
echo charts23-done
