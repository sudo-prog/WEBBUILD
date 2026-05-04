#!/usr/bin/env python3
import argparse, json, re, sys
from pathlib import Path

TRADE_KEYWORDS = {
    "plumber":        ["plumber","plumbing","drain","blocked drain","leak detection","hot water"],
    "electrician":    ["electrician","electrical","sparky","wiring","switchboard","lighting"],
    "builder":        ["builder","carpenter","construction","renovation","extensions","home builder"],
    "painter":        ["painter","painting","decorator","wallpaper","stripping"],
    "roofer":         ["roofer","roofing","tiling","guttering","downpipes","metal roof"],
    "air conditioning": ["air conditioning","hvac","ducted","split system","cooling","evaporative"],
    "kitchen":        ["kitchen","bathroom","joinery","cabinet","benchtop","outdoor kitchen"],
    "flooring":       ["flooring","tiles","laminate","carpet","polished concrete","timber floor"],
    "solar":          ["solar","solar panel","photovoltaic","pv","solar power","battery"],
    "pest control":   ["pest control","termite","exterminator","rodent","fumigation"],
    "gardener":       ["gardener","landscaper","tree","lawn mowing","hedge trimming","garden design"],
    "mechanic":       ["mechanic","auto repair","vehicle","car service","mechanical","tyre"],
}
SYDNEY_SUBURBS = [
    "Sydney CBD","Parramatta","Chatswood","Hurstville","Bankstown",
    "Blacktown","Bondi","Cronulla","Newcastle","Penrith"
]
DORK_TEMPLATES = [
    '{trade} {suburb} -site:.au -site:.com -site:http -site:https',
    '{trade} {suburb} "official website"',
    '{trade} {suburb} .com.au',
]

def gen(cat, out_path):
    kwds = TRADE_KEYWORDS[cat]
    queries = []
    for kw in kwds:
        for sub in SYDNEY_SUBURBS:
            for tpl in DORK_TEMPLATES:
                queries.append(tpl.format(trade=kw, suburb=sub))
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    with open(out_path,'w') as f:
        f.write('\n'.join(queries)+'\n')
    print(f'{cat}: {len(queries)} queries → {out_path}')

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--category', required=True)
    ap.add_argument('--output',   required=True)
    args = ap.parse_args()
    gen(args.category, args.output)
