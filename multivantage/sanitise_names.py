#!/usr/bin/env python3
"""Generic recursive JSON/JSONL name sanitiser. Mapping via --map only."""
import argparse, json, os, sys

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument('infile'); p.add_argument('outfile')
    p.add_argument('--map', action='append', default=[], metavar='OLD=NEW')
    p.add_argument('--split-on', default=None)
    p.add_argument('--check', default=None, help='comma-separated forbidden names')
    return p.parse_args()

def make_rename(mapping, split):
    def rename(s):
        if s in mapping:
            return mapping[s]
        if split and split in s:
            return split.join(mapping.get(part, part) for part in s.split(split))
        return s
    return rename

def transform(obj, rename):
    if isinstance(obj, dict):
        return {rename(k): transform(v, rename) for k, v in obj.items()}
    if isinstance(obj, list):
        return [transform(v, rename) for v in obj]
    if isinstance(obj, str):
        return rename(obj)
    return obj

def residue(obj, names, split, path='$'):
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in names:
                hits.append(f'{path} key {k!r}')
            if split and split in k:
                hits += [f'{path} key-part {p!r}' for p in k.split(split) if p in names]
            hits += residue(v, names, split, f'{path}.{k}')
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            hits += residue(v, names, split, f'{path}[{i}]')
    elif isinstance(obj, str):
        if obj in names:
            hits.append(f'{path} value {obj!r}')
        if split and split in obj:
            hits += [f'{path} value-part {p!r}' for p in obj.split(split) if p in names]
    return hits

def main():
    a = parse_args()
    mapping = dict(m.split('=', 1) for m in a.map)
    names = set(a.check.split(',')) if a.check else set()
    rename = make_rename(mapping, a.split_on)
    jsonl = a.infile.endswith('.jsonl')
    os.makedirs(os.path.dirname(a.outfile) or '.', exist_ok=True)
    hits, n = [], 0
    with open(a.infile) as fin, open(a.outfile, 'w') as fout:
        if jsonl:
            for line in fin:
                if not line.strip():
                    continue
                rec = transform(json.loads(line), rename)
                hits += residue(rec, names, a.split_on, f'$[{n}]')
                fout.write(json.dumps(rec) + '\n'); n += 1
        else:
            rec = transform(json.load(fin), rename)
            hits += residue(rec, names, a.split_on)
            json.dump(rec, fout, indent=2); fout.write('\n'); n = 1
    if hits:
        os.remove(a.outfile)
        print(f'RESIDUE: {len(hits)} hit(s), output deleted', file=sys.stderr)
        for h in hits[:50]:
            print('  ' + h, file=sys.stderr)
        sys.exit(1)
    print(f'OK: {n} record(s) written, check passed ({len(names)} forbidden names)')

if __name__ == '__main__':
    main()
