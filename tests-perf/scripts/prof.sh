#!/bin/bash
# ./prof.sh <impl> <scenario>: perf profile of the burst phase on the isolated CPUs
cd ~/rev2; L=prof-$1-$2; mkdir -p results/prof
taskset -c 1,3 numactl --membind=1 /usr/lib/jvm/jdk-27-oracle-x64/bin/java --enable-native-access=ALL-UNNAMED \
  -Xms4g -Xmx4g -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -XX:+UnlockDiagnosticVMOptions -XX:+DumpPerfMapAtExit \
  -Dperf.impl=$1 -Dperf.scenario=$2 -Dperf.rates=1 -Dperf.seconds=1 -Dperf.burst=50000000 -Dperf.bursts=6 \
  -cp "lib/*" exchange.core2.revelator.perf.PerfMain > results/prof/$L.log 2>&1 &
until grep -q "^latency" results/prof/$L.log 2>/dev/null; do sleep 0.1; done
sleep 1
sudo perf record -F 1999 -C 15,13,11,9,7,5 -o results/prof/$L.data -- sleep 3 > /dev/null 2>&1
sudo perf stat -e cycles,instructions,mem_load_l3_hit_retired.xsnp_hitm,machine_clears.memory_ordering -A -C 15,13,11,9 -x, -o results/prof/$L.stat -- sleep 2
wait
sudo perf report -i results/prof/$L.data --sort cpu,sym --stdio 2>/dev/null | grep -v "^#" | grep -v "^$" | head -25 > results/prof/$L.txt
echo prof-done
