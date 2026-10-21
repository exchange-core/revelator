#!/usr/bin/env python3
# median over repeats: latency p50/p99/p99.99 per offered rate, burst throughput
import csv, sys, statistics
from collections import defaultdict
lat = defaultdict(list); burst = defaultdict(list); impls = []
for row in csv.reader(open(sys.argv[1])):
    label, impl, scen, mode = row[:4]
    if impl not in impls:
        impls.append(impl)
    if mode == 'latency':
        rate, achieved, p50, p90, p99, p999, p9999, mx = row[4:12]
        lat[(scen, float(rate), impl)].append((float(achieved), int(p50), int(p99), int(p9999), int(mx)))
    else:
        burst[(scen, impl)].append(float(row[5]))
impls.sort(key=lambda i: ['revelator', 'disruptor', 'raw'].index(i) if i in ('revelator', 'disruptor', 'raw') else 9)
def us(ns):
    return f"{ns/1000:.2f}" if ns < 10_000_000 else f"{ns/1e6:.0f}ms"
med = lambda v, i: statistics.median(x[i] for x in v)
print("| scenario | MT/s | " + " | ".join(f"{i} p50 / p99 / p99.99 (us)" for i in impls) + " |")
print("|---|---|" + "---|" * len(impls))
for scen, rate in sorted({(k[0], k[1]) for k in lat}):
    cells = []
    for impl in impls:
        v = lat.get((scen, rate, impl))
        cells.append(" / ".join(us(med(v, i)) for i in (1, 2, 3)) + ("" if med(v, 0) > rate * 0.97 else f" (sent {med(v,0):.1f})") if v else "-")
    print(f"| {scen} | {rate:g} | " + " | ".join(cells) + " |")
print()
print("| scenario | " + " | ".join(f"{i} burst MT/s" for i in impls) + " |")
print("|---|" + "---|" * len(impls))
f = lambda v: f"{statistics.median(v):.1f} ({min(v):.1f}-{max(v):.1f})" if v else "-"
for scen in sorted({k[0] for k in burst}):
    print(f"| {scen} | " + " | ".join(f(burst.get((scen, i), [])) for i in impls) + " |")
