#!/bin/bash
# ./prof2.sh <label> <JVM flags...>: perf profile + counters of the burst phase on the isolated CPUs
cd ~/rev2; L=$1; shift; mkdir -p results/prof
taskset -c 1,3 numactl --membind=1 /usr/lib/jvm/jdk-27-oracle-x64/bin/java --enable-native-access=ALL-UNNAMED \
  -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UnlockDiagnosticVMOptions -XX:+DumpPerfMapAtExit \
  -Dperf.rates=1 -Dperf.seconds=1 -Dperf.burst=50000000 -Dperf.bursts=8 "$@" \
  -cp "lib/*" exchange.core2.revelator.perf.PerfMain > results/prof/$L.log 2>&1 &
until grep -q "^latency" results/prof/$L.log 2>/dev/null; do sleep 0.1; done
sleep 1
sudo perf stat -e cycles,instructions,mem_load_l3_hit_retired.xsnp_hitm,l2_rqsts.rfo_miss,mem_inst_retired.all_stores,machine_clears.memory_ordering -A -C 15,13,11,9,7,5 -x, -o results/prof/$L.stat -- sleep 2
sudo perf record -F 1999 -C 15,13,11,9,7,5 -o results/prof/$L.data -- sleep 2 > /dev/null 2>&1
wait
sudo perf report -i results/prof/$L.data --sort cpu,sym --stdio 2>/dev/null | grep -v "^#" | grep -v "^$" | head -30 > results/prof/$L.txt
grep burst results/prof/$L.log
echo "== top"; cut -c1-150 results/prof/$L.txt
echo "== counters"; grep -v "^#" results/prof/$L.stat | grep -v "^$" | awk -F, '{printf "%s %-36s %14s\n", $1, $4, $2}'
