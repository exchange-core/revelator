#!/usr/bin/env python3
"""Medians over repetitions: latency p50/p99 per rate, burst MT/s. Input: PerfMain CSV with label variant-config."""
import csv
import statistics
import sys
from collections import defaultdict

lat = defaultdict(list)    # (config, scenario, rate, variant) -> [(achieved, p50, p99)]
burst = defaultdict(list)  # (config, scenario, variant) -> [MT/s]
for r in csv.reader(open(sys.argv[1])):
    variant, config = r[0].split('-', 1)
    if r[3] == 'latency':
        lat[(config, r[2], float(r[4]), variant)].append((float(r[5]), int(r[6]), int(r[8])))
    elif r[3] == 'burst':
        burst[(config, r[2], variant)].append(float(r[5]))

med = statistics.median
print('| config | scenario | MT/s | baseline p50 / p99 ns | candidate p50 / p99 ns |')
print('|---|---|---|---|---|')
for key in sorted({k[:3] for k in lat}):
    cells = []
    for v in ('baseline', 'candidate'):
        xs = lat.get(key + (v,), [])
        if not xs:
            cells.append('-')
            continue
        sent = med(x[0] for x in xs)
        s = f'{med(x[1] for x in xs):.0f} / {med(x[2] for x in xs):.0f}'
        if sent < key[2] * 0.97:
            s += f' (sent {sent:.1f})'
        cells.append(s)
    print(f'| {key[0]} | {key[1]} | {key[2]:g} | {cells[0]} | {cells[1]} |')
print()
print('| config | scenario | baseline burst MT/s | candidate burst MT/s |')
print('|---|---|---|---|')
for key in sorted({k[:2] for k in burst}):
    cells = []
    for v in ('baseline', 'candidate'):
        xs = burst.get(key + (v,), [])
        cells.append(f'{med(xs):.1f} ({min(xs):.1f}-{max(xs):.1f})' if xs else '-')
    print(f'| {key[0]} | {key[1]} | {cells[0]} | {cells[1]} |')
