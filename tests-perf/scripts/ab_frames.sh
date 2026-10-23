#!/bin/bash
# frame layout A/B, Revelator header-commit: capacity sweep (p99 <= 10 us) and bursts
cd ~/rev2
run() { # <label> <scenario> <flags...>
  local label=$1 scen=$2; shift 2
  taskset -c 1,3 numactl --membind=1 /usr/lib/jvm/jdk-27-oracle-x64/bin/java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
    -XX:+AlwaysPreTouch -XX:+UseTransparentHugePages -Dperf.impl=revelator -Dperf.scenario=$scen "$@" \
    -Dperf.rates=4,8,10,12,14,16,18,20,22,24,26,28,30 -Dperf.seconds=1 -Dperf.stopAtP99Us=10 \
    -Dperf.burst=20000000 -Dperf.bursts=3 -cp "lib/*" exchange.core2.revelator.perf.PerfMain 2>&1 \
    | grep -E "^latency|^burst|stopping|Exception" | cut -d, -f1-2,5 | sed "s/latency //" | tr "\n" " "
  echo " <- $label"
}
for s in SHARDED4 MULTICAST; do
  run "$s header b1 packed" $s -Dperf.readers=5 -Dperf.publication=header
  run "$s header b1 aligned phase 6" $s -Dperf.readers=5 -Dperf.publication=header -Dperf.frames=aligned
  run "$s header b1 aligned WRONG phase 2" $s -Dperf.readers=5 -Dperf.publication=header -Dperf.frames=aligned -Drevelator.linePhase=2
  run "$s header b16 aligned" $s -Dperf.readers=5 -Dperf.publication=header -Dperf.frames=aligned -Dperf.batch=16
done
echo ab-done
