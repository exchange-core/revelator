#!/bin/bash
# ./prof3.sh <label> <rate> <JVM flags...>: counters per CPU during one long latency step at a fixed rate
cd ~/rev2; L=$1; R=$2; shift 2; mkdir -p results/prof
taskset -c 1,3 numactl --membind=1 /usr/lib/jvm/jdk-27-oracle-x64/bin/java --enable-native-access=ALL-UNNAMED \
  -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages \
  -Dperf.rates=$R -Dperf.seconds=10 -Dperf.burst=1000000 -Dperf.bursts=1 "$@" \
  -cp "lib/*" exchange.core2.revelator.perf.PerfMain > results/prof/$L.log 2>&1 &
sleep 8
sudo perf stat -e cycles,instructions,mem_load_l3_hit_retired.xsnp_hitm,l2_rqsts.rfo_miss,mem_inst_retired.all_stores -A -C 15,13,11,9,7,5 -x, -o results/prof/$L.stat -- sleep 3
wait
echo "$L $(grep ^latency results/prof/$L.log)"
# per message (rate R M/s over 3 s)
grep -v "^#" results/prof/$L.stat | grep -v "^$" | awk -F, -v n=$R 'BEGIN{m=n*3e6} {split($4,e," "); v[$1" "e[1]]=$2/m} END{for (c in v) print c, v[c]}' | sort -k2,2 -k1,1V | awk '{printf "%-6s %-36s %8.2f\n",$1,$2,$3}'
