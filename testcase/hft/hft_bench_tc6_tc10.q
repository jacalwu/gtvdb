/ hft_bench_tc6_tc10.q — kdb+ TC6–TC10 HFT baseline benchmark
/ Mirrors hft_tc6-tc10.md. Deterministic synthetic data; min-of-N timing (ns).
/ Run: q hft_bench_tc6_tc10.q
//
/ Per-event latency is derived as (total ns / number of events) from a batch,
/ which is the idiomatic q way to express per-record cost.

/ ---------------- timing ----------------
ns0:{[f] t0:.z.p; f[]; `long$.z.p - t0};
minns:{[f;n] min ns0 each n#enlist f};

r: ();
add:{[tc; scale; ns; unit; note]
  r,: enlist `tc`scale`ns`unit`note!(tc; scale; ns; unit; note)};

/ ================= TC6: pre-trade risk check =================
/ 4 hard checks: price band (5%), max qty, max notional, self-match flag.
/ (Rate limiting is stateful per-account and is noted, not timed here.)
tc6gen:{[n; nsyms]
  system "S 606";
  price: 150.0 + (n?1000) % 100.0;
  qty: 1 + n?50000;
  side: n?2;
  smp: n?2;                        / 1 = would self-match
  sym: n?nsyms;
  mid: 150.0 + (nsyms?1000) % 100.0;
  (price; qty; side; smp; sym; mid)};

tc6run:{[d]
  price:d 0; qty:d 1; side:d 2; smp:d 3; sym:d 4; mid:d 5;
  m: mid sym;                      / per-order reference mid
  band: abs[price - m] <= 0.05 * m;
  maxq: qty <= 10000;
  maxn: (price * qty) <= 10e6;
  smpchk: not smp;
  pass: band & maxq & maxn & smpchk;
  sum pass};

/ ================= TC7: tick-to-trade (decode -> logic -> encode) =================
tc7gen:{[n]
  system "S 707";
  ts: (1000 * `long$til n) + `long$(n?500);
  sym: n?500;
  bid: 150.0 + (n?1000) % 100.0;
  ask: bid + 0.01 + (n?5) % 100.0;
  bsz: 1 + n?10000;
  asz: 1 + n?10000;
  (ts; sym; bid; ask; bsz; asz)};

tc7run:{[d]
  ts:d 0; sym:d 1; bid:d 2; ask:d 3; bsz:d 4; asz:d 5;
  mid: (bid + ask) % 2;            / strategy signal
  side: ?[mid < 150.5; 0; ?[mid > 150.5; 1; 2]];
  opx: ?[side = 0; bid; ask];      / encode order
  oqty: ?[side = 2; 0; 100];
  (sum side < 2) + sum oqty > 0};

/ ================= TC8: L2 OBI + micro-price (10 levels x nsyms) =================
tc8gen:{[nsyms]
  system "S 808";
  base: 150.0 + (nsyms?1000) % 100.0;
  l: (1 + til 10) * 0.01;
  bp: base -\: l;                  / nsyms x 10
  ap: base +\: l;
  bs: (nsyms; 10) # (nsyms * 10) ? 1.0;
  as: (nsyms; 10) # (nsyms * 10) ? 1.0;
  (bp; ap; bs; as)};

tc8run:{[d]
  bp:d 0; ap:d 1; bs:d 2; as:d 3;
  bsum: sum each bs;               / nsyms vector
  asum: sum each as;
  obi: (bsum - asum) % (bsum + asum);
  b0: bp[;0]; a0: ap[;0]; bs0: bs[;0]; as0: as[;0];
  micro: ((b0 * as0) + (a0 * bs0)) % (bs0 + as0);
  sum obi + sum micro};

/ ================= TC9: 500x500 streaming covariance (rank-1 update) =================
tc9gen:{[nsyms; k]
  system "S 909";
  C: (nsyms; nsyms) # (nsyms * nsyms) # 0.0;
  rs: (k; nsyms) # (k * nsyms) ? 1.0;   / k return vectors
  (C; rs)};

tc9one:{[C; rv] (0.999 * C) + ((flip enlist rv) mmu enlist rv)};

tc9loop:{[C; rs]
  i: 0; k: count rs;
  while[i < k;
    C: tc9one[C; rs i];
    i: i + 1;
  ];
  C};

/ ================= TC10: local matching engine (price-time, level-aggregate) =================
tc10gen:{[n]
  system "S 1010";
  side: n?2;                       / 0 buy, 1 sell
  ismkt: (n?10) = 0;               / ~10% market orders
  price: 150.0 + (n?1000) % 100.0;
  qty: 1 + `long$(n?100);
  (side; ismkt; price; qty)};

tc10run:{[d]
  side:d 0; ismkt:d 1; price:d 2; qty:d 3;
  bookb: (`float$())!(`long$());
  booka: (`float$())!(`long$());
  nf: 0; i: 0; nn: count side;
  while[i < nn;
    s: side i; m: ismkt i; p: price i; rem: qty i;
    if[m;
      if[s = 0;                    / market buy -> hit ask
        while[(rem > 0) & 0 < count booka;
          best: min key booka;
          av: booka best;
          f: rem & av;             / min
          rem: rem - f; nf: nf + 1;
          if[av = f; booka: best _ booka];
          if[av > f; booka[best]: av - f];
        ];
      ];
      if[s = 1;                    / market sell -> hit bid
        while[(rem > 0) & 0 < count bookb;
          best: max key bookb;
          bv: bookb best;
          f: rem & bv;
          rem: rem - f; nf: nf + 1;
          if[bv = f; bookb: best _ bookb];
          if[bv > f; bookb[best]: bv - f];
        ];
      ];
    ];
    if[not m;                      / limit -> rest in book
      if[s = 0; bookb[p]: rem + 0^bookb p];
      if[s = 1; booka[p]: rem + 0^booka p];
    ];
    i: i + 1;
  ];
  nf};

/ ---------------- smoke tests ----------------
show "== smoke ==";
d6: tc6gen[10000; 50];
show ("tc6 pass: "; tc6run d6);
d7: tc7gen[10000];
show ("tc7 out: "; tc7run d7);
d8: tc8gen[100];
show ("tc8 sum: "; tc8run d8);
d9: tc9gen[50; 3];
show ("tc9 dim: "; count d9 0; count d9 1; " sum: "; sum tc9loop[d9 0; d9 1]);
d10: tc10gen[10000];
show ("tc10 fills: "; tc10run d10);

/ ---------------- full benchmark ----------------
show "";
show "== benchmark ==";

/ TC6: per-order risk check
d6b: tc6gen[5000000; 500];
t6: minns[{tc6run d6b}; 5];
add["TC6"; 5000000; t6; "ns/order"; "4 hard checks (band/qty/notional/SMP), batch vectorized"];

/ TC7: per-packet tick-to-trade
d7b: tc7gen[1000000];
t7: minns[{tc7run d7b}; 5];
add["TC7"; 1000000; t7; "ns/packet"; "decode->logic->encode (simulated), batch vectorized"];

/ TC8: 10k / 100k / 1M symbols
d8a: tc8gen[10000];
t8a: minns[{tc8run d8a}; 10];
add["TC8"; 10000; t8a; "ns/symbol"; "L2 OBI + micro-price, 10 levels, batch vectorized"];
d8b: tc8gen[100000];
t8b: minns[{tc8run d8b}; 5];
add["TC8"; 100000; t8b; "ns/symbol"; "L2 OBI + micro-price, 10 levels, batch vectorized"];
d8c: tc8gen[1000000];
t8c: minns[{tc8run d8c}; 3];
add["TC8"; 1000000; t8c; "ns/symbol"; "L2 OBI + micro-price, 10 levels, batch vectorized"];

/ TC9: per-tick covariance update (500x500), 100 ticks
d9b: tc9gen[500; 100];
t9: minns[{tc9loop[d9b 0; d9b 1]}; 3];
add["TC9"; 100; t9; "ns/tick"; "500x500 rank-1 covariance update (mmu)"];

/ TC10: per-order matching (200k orders)
d10b: tc10gen[200000];
t10: minns[{tc10run d10b}; 3];
add["TC10"; 200000; t10; "ns/order"; "price-time matching, level-aggregate book (per-order loop)"];

/ ---------------- report ----------------
show "";
show "| TC | scale | total | per-event | throughput |";
show "|----|------:|------:|----------:|-----------:|";
showrow: {[x]
  tot: x[`ns];
  per: tot % x[`scale];
  tput: 1000000000.0 * x[`scale] % tot;
  parts: ("| "; x[`tc]; " | "; string[x[`scale]]; " | ";
          string[tot % 1000000.0]; " ms | "; string[per]; " "; x[`unit]; " | ";
          string[`long$tput]; " |");
  -1 raze parts;
  };
showrow each r;

show "";
-1 "note: per-event = total_batch_ns / events (amortized single-thread per-event cost)";
exit 0;
