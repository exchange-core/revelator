#!/bin/bash
# A/B of header-commit variants on SHARDED4: capacity sweep (p99 <= 10 us) and bursts
cd ~/rev2
for cfg in ${CFGS:-"revelator header 1" "revelator header 4" "revelator header 16" "revelator cursor 16" "raw header 1" "raw header 16"}; do
  set -- $cfg
  taskset -c 1,3 numactl --membind=1 /usr/lib/jvm/jdk-27-oracle-x64/bin/java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
    -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -Dperf.impl=$1 -Dperf.publication=$2 -Dperf.batch=$3 \
    -Dperf.scenario=${SCEN:-SHARDED4} -Dperf.rates=4,8,10,12,14,16,18,20,22,24,26,28,30 -Dperf.seconds=1 -Dperf.stopAtP99Us=10 \
    -Dperf.burst=20000000 -Dperf.bursts=3 -cp "lib/*" exchange.core2.revelator.perf.PerfMain 2>&1 \
    | grep -E "^latency|^burst|stopping|Exception" | cut -d, -f1-2,5 | sed "s/latency //" | tr "\n" " "
  echo " <- $cfg"
done
echo ab-done
