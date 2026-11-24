#!/usr/bin/env python3
# Copyright (c) The Cairnflow Authors
# SPDX-License-Identifier: BUSL-1.1

"""Measures the length of Rust functions — a code-health criterion (no function over 300
lines on the turn path).

Length = the number of lines from the line holding `fn <name>` to the matching `}` line,
both ends included (a doc comment above does not count, comments inside do). Braces are
counted after stripping comments, strings/raw strings and char literals. Not a Rust
parser: unusual macros can shift the count by a few lines.

    MIN=300 python3 scripts/fn-lengths.py crates/cairn-server/src crates/cairn-engine/src

Prints every function at least MIN (default 100) lines long, longest first.
"""
import re, sys, os

def strip(src):
    out=[];i=0;n=len(src)
    while i<n:
        c=src[i]
        if src.startswith('//',i):
            j=src.find('\n',i); j=n if j<0 else j
            out.append(' '*(j-i)); i=j; continue
        if src.startswith('/*',i):
            d=1;j=i+2
            while j<n and d:
                if src.startswith('/*',j): d+=1;j+=2
                elif src.startswith('*/',j): d-=1;j+=2
                else: j+=1
            out.append(re.sub(r'[^\n]',' ',src[i:j])); i=j; continue
        m=re.match(r'b?r(#*)"',src[i:i+10])
        if m and (i==0 or not (src[i-1].isalnum() or src[i-1]=='_')):
            hashes=m.group(1); end='"'+hashes
            j=src.find(end,i+m.end()); j=n if j<0 else j+len(end)
            out.append(re.sub(r'[^\n]',' ',src[i:j])); i=j; continue
        if c=='"':
            j=i+1
            while j<n and src[j]!='"':
                j+= 2 if src[j]=='\\' else 1
            j+=1
            out.append(re.sub(r'[^\n]',' ',src[i:j])); i=j; continue
        if c=="'":
            m=re.match(r"'(\\.|\\u\{[0-9a-fA-F]+\}|\\x..|[^\\'])'",src[i:i+12])
            if m:
                out.append(' '*m.end()); i+=m.end(); continue
        out.append(c); i+=1
    return ''.join(out)

FN=re.compile(r'\bfn\s+([A-Za-z_][A-Za-z0-9_]*)')
def measure(path):
    src=open(path).read(); s=strip(src)
    lines_at=[0]
    for k,ch in enumerate(s):
        if ch=='\n': lines_at.append(k+1)
    import bisect
    def lineno(p): return bisect.bisect_right(lines_at,p)
    res=[]
    for m in FN.finditer(s):
        # find body start: first '{' or ';' at depth 0 of parens/angles after signature
        j=m.end(); dp=0
        while j<len(s):
            ch=s[j]
            if ch in '([': dp+=1
            elif ch in ')]': dp-=1
            elif dp==0 and ch==';': j=-1; break
            elif dp==0 and ch=='{': break
            j+=1
        if j<0 or j>=len(s): continue
        d=0;k=j
        while k<len(s):
            if s[k]=='{': d+=1
            elif s[k]=='}':
                d-=1
                if d==0: break
            k+=1
        res.append((lineno(k)-lineno(m.start())+1, m.group(1), path, lineno(m.start())))
    return res

allr=[]
for a in sys.argv[1:]:
    if os.path.isdir(a):
        for r,_,fs in os.walk(a):
            for f in fs:
                if f.endswith('.rs'): allr+=measure(os.path.join(r,f))
    else: allr+=measure(a)
thr=int(os.environ.get('MIN','100'))
for L,name,p,ln in sorted(allr,reverse=True):
    if L>=thr: print(f"{L:5d}  {name:30s} {p}:{ln}")
