/ hft_bench.q — kdb+ HFT TC1–TC5 benchmark
/ Mirrors crates/gtv-cli/examples/hft_bench.rs (same test cases, same thresholds).
/ Run: q hft_bench.q
//
/ All data is deterministic synthetic data (seeded), same shapes/distributions
/ as the Rust benchmark. Timings are min-of-N wall-clock (nanosecond .z.p).

/ ---------------- timing helpers ----------------
ms0:{[f] t0:.z.p; f[]; (`long$.z.p - t0) % 1e6};          / ms (float)
minbench:{[f;n] min ms0 each n#enlist f};

res: ();
add:{[tc;sc;m;th;note] res,: enlist `tc`scale`ms`threshold`note!(tc;sc;m;th;note)};

/ ================= TC1: cross-asset as-of temporal join =================
/ a: ascending ns timestamps; b: ascending ns + price/spread. as-of join with a
/ 500us lag tolerance (matches the Rust 500_000 ns tolerance).
tc1gen:{[n]
  system "S 101";
  at: (1000 * `long$til n) + `long$(n?500);
  bt: (1000 * `long$til n) + `long$(n?500);
  pr: 100.0 + sums 0.2 * ((n?1.0) - 0.5);
  sp: 0.02 + pr * 0.0001;
  a: ([] time: at);
  b: ([] time: bt; btime: bt; price: pr; spread: sp);
  (a; b)};

tc1run:{[ab]
  a: ab 0; b: ab 1;
  x: aj[`time; a; b];
  x: update price: ?[within[time - btime; 0 500000]; price; 0n],
           spread: ?[within[time - btime; 0 500000]; spread; 0n] from x;
  x};

/ ================= TC2: order-flow imbalance (OFI) rolling 100 =================
/ OFI_t = bid_sz * dBid - ask_sz * dAsk, then msum[100].
tc2gen:{[n]
  system "S 202";
  mid: 100.0 + sums 0.2 * ((n?1.0) - 0.5);
  spr: 0.01 + n?0.02;
  bid: mid - spr % 2;
  ask: mid + spr % 2;
  bsz: 1 + n?10000;
  asz: 1 + n?10000;
  (bid; ask; bsz; asz)};

tc2run:{[bq]
  bid: bq 0; ask: bq 1; bsz: bq 2; asz: bq 3;
  dbid: (1_bid) - (-1_bid);
  dask: (1_ask) - (-1_ask);
  ofi: (bsz * (0f, dbid)) - asz * (0f, dask);
  msum[100; ofi]};

/ ================= TC3: wash-trade cycle detection (A->B->C->A) =================
/ chain spine (no back-edges) + planted cycles with equal amounts and strictly
/ increasing event times. Amount deviation <= 0.1% filter inside the join.
tc3gen:{[node_count; cycles]
  cn: node_count - cycles * 3;
  i: til cn - 1;
  src1: i; dst1: i + 1; vf1: i; amt1: 1000.0 + (i mod 7);
  k: til cycles;
  aa: cn + 3 * k;
  src2: raze (aa; aa + 1; aa + 2);
  dst2: raze (aa + 1; aa + 2; aa);
  vf2: raze (1000 + 0 * aa; 2000 + 0 * aa; 3000 + 0 * aa);
  amt2: 1000.0 + 0 * src2;
  ([] src: src1, src2; dst: dst1, dst2; vf: vf1, vf2; amount: amt1, amt2)};

tc3run:{[e]
  ab: select a: src, b: dst, t0: vf, amt0: amount from e;
  bc: `b xkey select b: src, c: dst, t1: vf, amt1: amount from e;
  abc: ab lj bc;
  abc: select from abc where not null c, t1 > t0, (abs[amt1 - amt0] % amt0) <= 0.001;
  ca: `c`a xkey select c: src, a: dst, t2: vf, amt2: amount from e;
  rr: abc lj ca;
  rr: select from rr where not null t2, t2 > t1, (abs[amt2 - amt1] % amt1) <= 0.001;
  count rr};

/ ================= TC4: 512-dim KNN (brute force, exact f32) =================
/ Squared-L2 via the identity |x-y|^2 = |x|^2 + |y|^2 - 2 x.y, using mmu (BLAS)
/ for the dot products — the q-native way to scan a corpus.
/ Corpus is built as a list of chunk matrices (memory-bounded: q floats are
/ 8 bytes, so 1M x 512 would be 4 GB + a 4 GB m*m temp; chunks keep the working
/ set ~sub-GB). The scan is still exact brute force over every vector.
tc4gen:{[n; dim; cs]
  system "S 303";
  qv: dim ? 1.0;
  chunks: ();
  c: 0;
  while[c < n;
    rows: cs & n - c;
    chunks,: enlist (rows; dim) # (rows * dim) ? 1.0;
    c: c + rows;
  ];
  (chunks; qv)};

tc4run:{[mq; k]
  chunks: mq 0; qv: mq 1;
  qv2: sum qv * qv;
  bestd: k # 0w;   / float +inf
  besti: k # 0N;   / null longs
  base: 0; i: 0; nc: count chunks;
  while[i < nc;
    mc: chunks i;
    rows: count mc;
    sd: sum each mc * mc;
    dots: raze mc mmu flip enlist qv;
    dist: sd + qv2 - 2 * dots;
    alld: bestd, dist;
    alli: besti, base + til rows;
    idx: k # iasc alld;
    bestd: alld idx;
    besti: alli idx;
    base: base + rows;
    i: i + 1;
  ];
  besti};

/ ================= TC5: point-in-time order-book snapshot =================
/ valid_from ascending, valid_to = valid_from + 100 (duration). Active at T =
/ vf in (T-100, T] — an O(log n) binary-search count (the q-native zone map).
tc5gen:{[n]
  vf: `long$til n;
  vt: vf + 100;
  (vf; vt)};

tc5run:{[vtbl; T]
  vf: vtbl 0;
  (vf bin T) - (vf bin (T - 100))};

/ ---------------- smoke tests (small data, correctness) ----------------
show "== smoke tests ==";
ab0: tc1gen 1000; r1: tc1run ab0;
show "TC1 aj rows: ", string count r1;
bq0: tc2gen 1000; r2: tc2run bq0;
show "TC2 ofi last: ", string last r2;
e0: tc3gen[1000; 5];
show ("TC3 edges/matches: "; count[e0]; " / "; tc3run e0);
mq0: tc4gen[1000; 8; 1000];
d1: tc4run[mq0; 5];
d2: 5 # iasc sum each ((first mq0 0) - \: (mq0 1)) xexp 2;
show ("TC4 top5 mmu==naive: "; d1 ~ d2);
show "TC5 active@T: ", string tc5run[tc5gen 10000; 5000];

/ ---------------- full benchmark ----------------
show "";
show "== benchmark ==";

/ free a named global and force GC
free1:{[n] n set (); .Q.gc[];};

/ TC1
tc1_ab: tc1gen 100000;
add["TC1"; 100000; minbench[{tc1run tc1_ab}; 10]; 0n; "as-of join aj + 500us lag filter"];
free1 `tc1_ab;
tc1_ab1m: tc1gen 1000000;
add["TC1"; 1000000; minbench[{tc1run tc1_ab1m}; 5]; 5.0; "as-of join aj + 500us lag filter"];
free1 `tc1_ab1m;
tc1_ab5m: tc1gen 5000000;
add["TC1"; 5000000; minbench[{tc1run tc1_ab5m}; 3]; 0n; "as-of join aj + 500us lag filter"];
free1 `tc1_ab5m;

/ TC2
tc2_b: tc2gen 100000;
add["TC2"; 100000; minbench[{tc2run tc2_b}; 10]; 0n; "OFI + msum[100]"];
free1 `tc2_b;
tc2_b1m: tc2gen 1000000;
add["TC2"; 1000000; minbench[{tc2run tc2_b1m}; 5]; 2.0; "OFI + msum[100]"];
free1 `tc2_b1m;
tc2_b5m: tc2gen 5000000;
add["TC2"; 5000000; minbench[{tc2run tc2_b5m}; 3]; 0n; "OFI + msum[100]"];
free1 `tc2_b5m;

/ TC3
tc3_e: tc3gen[100000; 20];
add["TC3"; 100000; minbench[{tc3run tc3_e}; 5]; 0n; "triangle join (A->B->C->A), amount<=0.1%"];
free1 `tc3_e;
tc3_e500k: tc3gen[500000; 100];
add["TC3"; 500000; minbench[{tc3run tc3_e500k}; 3]; 10.0; "triangle join (A->B->C->A), amount<=0.1%"];
free1 `tc3_e500k;

/ TC4
tc4_m: tc4gen[100000; 512; 100000];
add["TC4"; 100000; minbench[{tc4run[tc4_m; 10]}; 3]; 8.0; "brute-force 512-dim KNN (mmu, chunked), top-10"];
free1 `tc4_m;
tc4_m1m: tc4gen[1000000; 512; 100000];
add["TC4"; 1000000; minbench[{tc4run[tc4_m1m; 10]}; 2]; 8.0; "brute-force 512-dim KNN (mmu, chunked), top-10"];
free1 `tc4_m1m;

/ TC5
tc5_v: tc5gen 100000;
add["TC5"; 100000; minbench[{tc5run[tc5_v; 50000]}; 1000]; 0n; "point-in-time snapshot (vf bin)"];
free1 `tc5_v;
tc5_v1m: tc5gen 1000000;
add["TC5"; 1000000; minbench[{tc5run[tc5_v1m; 500000]}; 1000]; 0n; "point-in-time snapshot (vf bin)"];
free1 `tc5_v1m;
tc5_v5m: tc5gen 5000000;
add["TC5"; 5000000; minbench[{tc5run[tc5_v5m; 2500000]}; 1000]; 1.0; "point-in-time snapshot (vf bin)"];
free1 `tc5_v5m;

/ ---------------- report ----------------
fmt: {$[x < 1000.0; (string x), " ms"; (string (x % 1000.0)), " s"]};
passfn: {$[null y; "-"; $[x <= y; "PASS"; "FAIL"]]};

show "";
show "| TC | scale | Latency | threshold | result |";
show "|----|------:|--------:|----------:|--------|";
showrow: {[x]
  th: $[null x`threshold; "-"; (string x`threshold), "ms"];
  r: raze (x`tc; " | "; string x`scale; " | "; fmt[x`ms]; " | ";
           th; " | "; passfn[x`ms; x`threshold]);
  -1 raze ("| "; r; " |");
  };
showrow each res;

show "";
npass: sum {[x] (not null x[`threshold]) & (x[`ms] <= x[`threshold])} each res;
ntot: sum not null res`threshold;
-1 raze ("threshold tests passed: "; string npass; " / "; string ntot);

exit 0;
