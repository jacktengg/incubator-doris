<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Nullable aggregation: type-version performance recheck

Final production source: `219f6f095d41e15feb2279c1a10557404c23d6e3`.
Earlier measured source: `c72c5cf7b8a077568a5dbf2de41492fbe1919b8d`.
The intermediate single-store source is `18fb6aabbcc`; it did not remove the slowdown.
Reference source: `e0bf4874070ba0b6837ad93f695ea63532b77a47`.
Measurements were made on 2026-11-13. They are separate from PR #60228 and from
all earlier archives in this directory.

## Source and method

The reference production code is unchanged. Only the same benchmark harness and
its target definition were copied to that checkout. The reference harness selects
V1 or V2 with the old attribute. The migrated harness always executes V2.
Both production builds use the standard `build.sh` entry point, Clang 20,
RELEASE/O3, AVX2, PCH off, and JDK 17. Exact binary hashes are in the new manifest.
The migrated FE's embedded display version came from an older generated
`Version.java`. It is not used as source provenance. The source commit, build log,
and binary hash identify the measured build.

The final archive check found a stale SQL runtime. `build.sh --be --benchmark`
builds the benchmark target instead of the BE server. Thus an earlier 270-query
cohort still used the `c72c5cf7b8a` BE, while its kernels used the final source.
Those records remain under `pre-runtime-rebuild` and are excluded from the final
SQL comparison. The final BE is built with `build.sh --be -j12`. Before timing,
the driver checks the running process's executable hash against the new binary.

The machine has two Xeon Platinum 8457C sockets, 192 logical CPUs, and about
1.48 TiB RAM. It is shared. CPU frequency scaling and transparent huge pages
remain enabled. Each timing cohort uses one BE with a 16 GiB limit and one FE
with an 8 GiB heap. Pipeline parallelism is 4, aggregation phase is 2, SQL cache
is disabled, and profile level is 2. Ports and storage paths are isolated.
No build, test, or profiler runs concurrently with the timed workload.

The source table has four million deterministic rows. `prepare.sql` and
`prepare_bulk.sql` create the source and old state tables on the reference build.
TPC-DS SF1 uses the repository generator, DDL, loader, and unchanged Q78 SQL.
All 24 query tables were loaded and analyzed. Source row counts and file hashes
are retained. The stock loader's missing DBGEN_VERSION metadata table is recorded;
it does not affect those query tables.

Stopped reference metadata and storage were copied before the upgrade.
All timed tables remain unchanged across cohorts. Upgrade inserts and full
compaction run only after SQL timing ends. SQL and DDL are included in the archive.

Each full SQL round has five warmups per case followed by ten recorded runs.
The initial rounds also have a complete matrix prewarm. The reference V2 and
earlier `c72c5cf7b8a` cohorts each receive a second full round. The final source
starts a new cohort and reuses the unchanged reference records. Cases whose last
five samples have a coefficient of variation
(CV) of at least 5% receive up to five further blocks of five warmups and five
recorded runs. Every recorded sample remains in the summaries. The final-five
criterion is a repeatability check, not a confidence interval or a test of causality.

The kernel matrix has 48 cases per mode: streaming and merge SUM/AVG,
BIGINT/DECIMAL128, four NULL ratios, and two merge group counts. The original
reference runs both modes, for 144 configurations in total. Kernels run on CPU 12
with five repetitions, 0.1 s warmup, and 0.5 s minimum measurement time.
Noisy kernels receive longer runs. Allocation counts come from separate binaries
and cover 16 operations. Their timing is not used for performance comparisons.

## Results and limits

All 1,100 queries in the final comparison agree across 21 cases: 325 reference V1,
515 reference V2, and 260 final-source queries. The earlier source has 480 separate
migrated queries. Reused reference records are not new measurements.

| SQL case | Reference V2 mean ms | Final mean ms | Change |
| --- | ---: | ---: | ---: |
| numeric_low_null0 | 36.36 | 38.84 | +6.8% |
| numeric_low_null100 | 33.27 | 36.15 | +8.6% |
| numeric_high_null1 | 182.17 | 175.26 | -3.8% |
| numeric_high_null50 | 195.93 | 172.86 | -11.8% |
| numeric_high_null100 | 172.07 | 117.06 | -32.0% |
| variadic_null50 | 82.57 | 66.83 | -19.1% |
| wide_string_set | 1243.99 | 1273.92 | +2.4% |
| wide_topn | 549.17 | 620.76 | +13.0% |
| stored_bulk_merge | 44.68 | 47.04 | +5.3% |
| tpcds_sf1_q78 | 135.66 | 143.86 | +6.0% |

The low-cardinality, no-NULL mean is 6.8% higher than the combined reference
mean. The same unchanged reference binary measured 31.62 ms in its first full
round and 39.41 ms in its second full round. The final source measures 38.84 ms.
This measured reference shift is larger than the apparent migration difference.
All rounds remain included. A range overlap alone is not proof of no regression.

TOPN has a 13.0% higher mean. Its first five final-source samples average 693.42 ms,
including a 990.85 ms sample. The next five average 548.09 ms. The old binary's
five-sample block means range from 526.12 to 581.94 ms. Q78 has a 6.0% higher
combined mean, but its four final-source block means are 166.17, 153.86, 131.08,
and 124.35 ms. These shifts motivate the separate nearby old/new SQL check below.
They do not support a general Q78 speedup claim.

Each SQL case and cohort has at least one complete block of five warmed samples
with CV below 5%. All final-source cases have last-five CV below 5%, with a maximum
of 4.94%. The old-V1 TOPN final-five CV is 8.83%; its first block has CV 3.68%.
The summaries retain every sample. A stable block does not make the combined
sample set uniformly stable.

See the complete [query statistics](metadata-version-query-summary.json),
[query table](metadata-version-query-summary.md), and
[operator task avg/min/max](metadata-version-operator-summary.md).
The operator JSON also retains repetition statistics. Task times are separate
from query wall time and must not be summed into query latency.

### Nearby SQL checks

The additional SQL checks run one runtime at a time on the same unchanged source
and TPC-DS tables. Each runtime gets a full matrix prewarm. Each selected case
gets five further warmups and at least five recorded repetitions. No build,
regression test, or CPU recorder runs during these checks. The running BE hash
is checked again. All 115 additional queries agree with the original results.
They remain separate from the 1,100-query primary summary.

| Case | Order | Reference V2 mean ms | Final mean ms | Change |
| --- | --- | ---: | ---: | ---: |
| numeric_low_null1 | old then new | 40.31 | 42.34 | +5.0% |
| numeric_low_null50 | old then new | 43.00 | 48.18 | +12.0% |
| numeric_low_null100 | old then new | 37.48 | 37.73 | +0.7% |
| wide_topn | old then new | 527.32 | 552.01 | +4.7% |
| tpcds_sf1_q78 | old then new | 138.65 | 148.10 | +6.8% |
| numeric_low_null50 | new then old | 44.21 | 44.16 | -0.1% |
| tpcds_sf1_q78 | new then old | 152.50 | 147.21 | -3.5% |

The first check retains 55 old and 30 new samples. After three allowed repeat
blocks, the old sparse-NULL and all-NULL cases still have last-five CV above 5%.
That failure is retained. The reverse check retains 20 old and 10 new samples;
both cases have last-five CV below 5% on both binaries.

For the first Q78 pair, FE plan time increases from 42.4 to 53.8 ms. Wait/fetch
time falls from 54.8 to 51.4 ms, and BE query CPU falls from 778.2 to 763.2 ms.
These are measured counter means. The 6.8% wall-time increase is therefore not
an equal increase in BE aggregation time. In the reverse check, the unchanged
old runtime's wall time increases, while the final runtime stays close to its
prior mean. The low-cardinality 50%-NULL difference also disappears in reverse
order. The data do not establish a stable migration regression for these cases.
They also do not remove shared-host or process-start variation. Keep all records
and avoid a general SQL speedup claim. The raw profiles, audit metrics, commands,
and repetition statistics are under `records/sql-paired-probe` and
`records/sql-reverse-probe` in the archive.

### Kernel results and the SUM stall

For 4,096 rows with 1% NULL, the primary CPU-12 mean is 7.241→1.680 us for SUM
and 18.272→9.135 us for AVG versus reference V2. SUM requests 7→5 heap allocations
and 139,392→73,856 bytes per operation. AVG requests 7→5 allocations and
434,304→270,464 bytes. Native grouped SUM/AVG merge requests no scratch allocation.
The old benchmark retained one scratch allocation across 16 operations; this
was not an allocation on every batch.

The earlier source had intermittent all-NULL SUM samples of 34–89 us. A separate
CPU recording found about 93% of cycles at its native serialization loop, near
AVX2 masked loads. A live counter sample measured about 0.04 instructions/cycle.
Writing each state once still produced 63–85 us samples in three new processes.
The final code detects an all-NULL batch and fills empty states without payload
loads. Its first 30 independent-process samples are 0.90–1.07 us. The later
CPU recording has memory fill/copy work as its main cost, not the earlier masked
payload-load stall. Both intermediate sources and their raw records are retained.

The primary [kernel table](metadata-version-kernel-summary.md) and
[JSON statistics](metadata-version-kernel-summary.json) include all CPU-12 samples.
They still contain timing variation. In particular, DECIMAL AVG merge with 64
groups and 1% NULL has a 7.947 us mean, versus 7.422 us for old V2, but a 7.167 us
median. One long round averaged 10.677 us. Independent processes of the same final
binary later averaged 7.193, 8.395, and 6.910 us. The earlier `c72c5cf7b8a` binary
was also measured between those processes; its raw results remain available.

Four noisy cases therefore receive a separate paired check on CPU 37, on the same
NUMA node. CPU 37 was selected from a one-second utilization sample; it was not
reserved or isolated. Old/new order alternates by case. The default is five runs,
2 s minimum measurement, and 1 s warmup. All-NULL SUM receives five additional
runs with 5 s measurement/warmup. These records are not merged into the CPU-12
statistics. Each side of each pair has five consecutive samples with CV below 5%.

| CPU-37 case | Reference V2 mean us | Final mean us | Change |
| --- | ---: | ---: | ---: |
| streaming/nullable_sum/4096/100/1 | 5.204 | 0.932 | -82.1% |
| merge/nullable_sum/4096/50/1/64 | 4.375 | 2.620 | -40.1% |
| merge/nullable_avg/4096/100/1/64 | 7.452 | 1.745 | -76.6% |
| merge/nullable_decimal_avg/4096/1/1/64 | 7.962 | 7.091 | -10.9% |

The paired results do not show a stable AVG regression. Process and CPU placement
sensitivity prevent a causal conclusion from the first CPU-12 mean alone. The
hardware cause of every timing spike was not isolated. Keep the original values,
use the paired check to bound the conclusion, and do not claim that shared-host
noise has been removed. The SUM payload-load stall is a separate finding with a
code change and before/after instruction evidence.

### Query memory

Query peak memory does not uniformly decrease. The largest increase in the mean
among these cases is 7.10%: low-cardinality, 1%-NULL numeric aggregation measures
193.1→206.8 MiB, with ranges 177.3–213.4 and 175.8–244.6 MiB. Weighted AVG at 50%
NULL measures 256.8→268.2 MiB, with ranges 252.0–285.3 and 256.3–285.5 MiB.
Wide-string streaming measures 961.3→1003.0 MiB, with ranges 876.9–1020.8 and
968.7–1039.9 MiB.

The nearby sparse-NULL check reverses that first memory increase: the old query
peak mean is 198.5 MiB, with a 175.8–240.1 range; the final mean is 190.9 MiB,
with a 175.8–208.8 range. The main run's higher peaks remain reported. This
repeat does not support a persistent 7.10% query-memory increase.

The weighted-AVG aggregation state counters remain 7.64 MB per phase. Wide
string/set state counters remain about 2.00 GB; the state-counter difference is
about 0.25 MB. That large arena comes from the unchanged collect-set path, which
copies each valid string before testing for a duplicate. The kernel allocation
counters do not show scratch or temporary-state growth. Concurrent buffered blocks
and spare capacity are possible contributors to the query peaks; this is an
inference, not a byte-by-byte attribution. Retain the state/queue limits, disclose
all maxima and ranges, and make no claim of universal memory reduction.

Seven separate query CPU recordings cover old V1, old V2, and migrated numeric
streaming, plus old V2 and migrated wide-string and variadic workloads.
They use 199 Hz sampling for eight seconds after warmup. Profiles and raw perf
files are retained. These runs are excluded from SQL timing summaries.

## Upgrade checks

Real reference binaries create the old data. At the type-version fix, compatibility
queries run under
old FE/old BE, old FE/new BE, new FE pinned to version 19/old BE, and new FE20/new BE.
Both values of the retired session name are accepted. SQL cache is disabled.
A new FE pinned to version 19 also creates and writes SUM, fixed COUNT, and
BITMAP_INTERSECT state columns on the old BE. The upgraded reader returns the
same values. These checks establish the tested reference boundary. They do not
claim that every published Doris version was run.

The final source repeats storage checks from a fresh copy of the stopped original
snapshot. The bitmap marker fixture reduces three rowsets to one. A later valid `{1,2}`
state survives a no-input partial, while a valid empty bitmap still yields an
empty intersection. The mixed numeric/bitmap/collection/Array/Map fixture reduces
four rowsets to one without changing its results. The fixed COUNT fixture also
reduces four rowsets to one and retains count 2 after one new valid contribution.

## Reproduction and artifacts

Extract `metadata-version-records.tar.gz` into a separate directory. Its
`records` directory contains raw query profiles, audit rows, benchmark repetitions,
allocation counts, CPU recordings, settings, SQL, DDL, source hashes, and upgrade
results. Its `validation` directory contains standard build/test/style logs.
The `before-sum-fix`, `single-store-only`, and `pre-runtime-rebuild` directories
retain earlier sources and intermediate measurements. Their source records state
the actual runtime and benchmark versions. Shared reference files use archive
hard links. The [manifest](metadata-version-manifest.json) records the archive hash
and exact runtime/benchmark hashes.
Do not copy the generated dataset or binaries into the repository.

```bash
python3 summarize.py --input extracted/records --output . --prefix metadata-version-
python3 summarize_operators.py --input extracted/records --output . --prefix metadata-version-
python3 summarize_kernels.py --input extracted/records --output . --prefix metadata-version-
python3 -m unittest discover -s . -p 'test_*.py' -v
```

Use the committed drivers with `--port`, `--http-port`, `--be-http-port`, and
`--output` to select an isolated cluster. The archived command records give the
exact arguments for this run. The archived orchestration scripts also show the
recheck order and retain earlier failures instead of replacing their records.

See the [correctness review](metadata-version-review.md) for the codec defect,
minimal tests, upgrade restrictions, and review checkpoints. The original private
DORIS-29405 SQL and issue status remain unverified. The same-class bitmap tests
are not presented as proof that the original issue has been resolved.
