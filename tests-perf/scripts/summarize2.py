#!/usr/bin/env python3
# one table per scenario: rows = implementation + publication + batch, columns = p50/p99 per rate (us), burst MT/s
import csv, sys, statistics
from collections import defaultdict, OrderedDict
lat = defaultdict(list); burst = defaultdict(list); rates = set(); configs = OrderedDict(); scens = OrderedDict()
for row in csv.reader(open(sys.argv[1])):
    label, impl, scen, mode = row[:4]
    cfg = f"{impl} {label}"; configs[cfg] = 1; scens[scen] = 1
    if mode == 'latency':
        rate = float(row[4]); rates.add(rate)
        lat[(scen, cfg, rate)].append((float(row[5]), int(row[6]), int(row[8])))
    else:
        burst[(scen, cfg)].append(float(row[5]))
def us(ns):
    return f"{ns/1000:.2f}" if ns < 10_000_000 else f"{ns/1e6:.0f}ms"
rates = sorted(rates)
for scen in scens:
    print(f"### {scen}\n")
    print("| config | " + " | ".join(f"{r:g} MT/s p50/p99" for r in rates) + " | burst MT/s |")
    print("|---|" + "---|" * (len(rates) + 1))
    for cfg in configs:
        cells = []
        for r in rates:
            v = lat.get((scen, cfg, r))
            if not v:
                cells.append("-"); continue
            sent = statistics.median(x[0] for x in v)
            c = us(statistics.median(x[1] for x in v)) + " / " + us(statistics.median(x[2] for x in v))
            cells.append(c if sent > r * 0.97 else f"sat {sent:.1f}")
        b = burst.get((scen, cfg))
        print(f"| {cfg} | " + " | ".join(cells) + f" | {statistics.median(b):.0f} |" if b else " | - |")
    print()
