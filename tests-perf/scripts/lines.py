#!/usr/bin/env python3
"""
Turns PerfMain CSV results into series,x,y files for `ChartsCli --lines`.

  lines.py load <load.csv> <out-dir>
      per scenario and percentile: latency (ps) against offered load, points where the publisher kept up
  lines.py readers <dir with readers-N.csv> <out-dir> [p99 limit us]
      highest offered load with p99 under the limit, against the number of readers
"""
import csv
import glob
import os
import re
import sys
from collections import OrderedDict

PERCENTILES = {'p50': 6, 'p99': 8, 'p99.99': 10}  # column index in a latency row


def rows(path):
    with open(path) as f:
        for r in csv.reader(f):
            if len(r) >= 12 and r[3] == 'latency':
                yield r


def load(src, out):
    data = OrderedDict()  # (scenario, percentile) -> series -> [(x, y)]
    for r in rows(src):
        series, scenario, rate, achieved = r[0], r[2], float(r[4]), float(r[5])
        if achieved < rate * 0.97:
            continue  # publisher fell behind: latency is queue length, not service time
        for name, col in PERCENTILES.items():
            data.setdefault((scenario, name), OrderedDict()).setdefault(series, []).append((rate, int(r[col]) * 1000))
    os.makedirs(out, exist_ok=True)
    for (scenario, name), series in data.items():
        with open(os.path.join(out, f'load-{scenario}-{name}.csv'), 'w') as f:
            f.write('series,x,y\n')
            for s, points in series.items():
                for x, y in sorted(points):
                    f.write(f'{s},{x:g},{y}\n')


def readers(src_dir, out, limit_us):
    best = OrderedDict()  # series -> {readers: max rate}
    for path in sorted(glob.glob(os.path.join(src_dir, 'readers-*.csv'))):
        n = int(re.search(r'readers-(\d+)\.csv', path).group(1))
        for r in rows(path):
            series, rate, achieved, p99 = r[0], float(r[4]), float(r[5]), int(r[8])
            ok = achieved >= rate * 0.97 and p99 <= limit_us * 1000
            per = best.setdefault(series, {})
            per.setdefault(n, 0.0)
            if ok:
                per[n] = max(per[n], rate)
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, 'readers-capacity.csv'), 'w') as f:
        f.write('series,x,y\n')
        for s, per in best.items():
            for n in sorted(per):
                f.write(f'{s},{n},{per[n]:g}\n')


if __name__ == '__main__':
    if sys.argv[1] == 'load':
        load(sys.argv[2], sys.argv[3])
    else:
        readers(sys.argv[2], sys.argv[3], float(sys.argv[4]) if len(sys.argv) > 4 else 10)
