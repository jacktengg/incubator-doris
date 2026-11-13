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

# AGG_STATE type-version review

Reviewed starting source: `2998a3acc3f3ebdcdb04206427424512c0b67eb6`.
Type-version fix: `c72c5cf7b8a077568a5dbf2de41492fbe1919b8d`.
Final SUM source: `219f6f095d4`.

The starting branch already contained the V2 migration and its earlier test and
performance records. This review found a remaining type-version defect. It did
not reintroduce a V1 execution path.

## Failure and fix

A stored version-20 bitmap state must retain its format version when a query
uses execution version 19. Before this fix, two paths replaced that version:

1. FE catalog JSON did not store it. Planning, schema encoding, and replay used
   the current FE configuration.
2. BE COMBINE, UNION, and MERGE passed the query version to the nested state
   function. That function was also owned by the state type.

A no-input bitmap state then changed from `[0]` to `[1,1,0]`. The latter bytes
mean a valid empty bitmap. Intersecting it with `{1}` incorrectly produces an
empty result.

The SQL test has one table row. This reduced plan shows the failure boundary:

```text
bitmap_intersect_merge
  UNION ALL
    bitmap_intersect_union
      Scan(k < 0)       -- zero input rows; stored state type is version 20
    Scan(k = 1)         -- one state with bitmap {1}
```

New FE catalog types now store `beExecVersion`. Nereids conversion, query Thrift
types, replica Thrift schemas, and cloud Protobuf schemas preserve it. The BE
combinators update only their own query version. They retain the nested codec
version from their AGG_STATE input or output type.

Old catalog JSON has no version field. Its fallback retains the old codec choice
below version 20 and caps it at 19 when the FE advances to 20. This does not infer
an exact historical per-function version that was never saved. It also cannot
recover version-20 metadata created by an unreleased draft without this field.
An explicitly stored version remains fixed.

Version 19 cannot encode a fresh no-input nullable BITMAP_INTERSECT state with a
nonnullable result. Its published empty encoding remains an empty bitmap. New
version-20 state types can retain the no-input mark. The decoder also retains an
existing version-20 mark through compaction of a version-19 tablet schema. An old
NULL-only value already collapsed to an empty bitmap cannot be recovered.

Keep FE execution version 19 throughout a rolling upgrade. Enable version 20
only after all FEs retain state type versions and all BEs support version 20.
Changing the query version does not downgrade existing type metadata or bytes.
See the [complete entry and format matrix](../../nullable-aggregation-v2-migration.md).

## Correctness evidence

| Check | Result |
| --- | --- |
| New BE test before the fix | Fails for query/type version mismatches; checks bytes and the final intersection |
| New FE tests before the fix | Both fail; old code replaces the type version with the current configuration |
| Focused BE ASAN tests | 408 tests pass after the type-version fix; 409 tests in 67 suites pass after the SUM fix |
| FE tests after the fix | 44 tests pass: type versions, column Gson serialization, and Nereids types |
| AGG_STATE SQL directory | All 20 suites pass, including forced full compaction |
| New SQL expected output | Generated with `run-regression-test.sh -genOut`, then compared in the directory run |
| Actual FE restart and replay | The one-row query returns `1` at query versions 19 and 20 after restart |
| Build and style | BE ASAN and FE builds, FE Checkstyle, clang-format 16, header hygiene, and changed-line clang-tidy pass |
| Earlier evidence archives | All six archive hashes match. The initial query, operator, and kernel summaries regenerated exactly. Operator summaries were then corrected for FE integer-remainder formatting. |

The BE test uses one empty state and one bitmap element. It covers both type
versions, both query versions, and all three combinators. The FE tests need no
rows or server. They also read legal old catalog JSON without the new field.
The SQL test keeps its one table row for inspection after the run.

One BE build attempt overlapped an FE test build. The FE generator removed a
C++ generated file while BE was compiling it. Running the build steps in sequence
resolved this build failure. It was not counted as a passing build.

An archive hash check also found that a benchmark build had left the BE server
binary unchanged. Its SQL results are retained as earlier-source diagnostics.
The production BE was then built separately. The final SQL run checks the running
executable hash before measurement and uses a fresh copy of the old-data snapshot.

## Review checkpoints

| Checkpoint | Conclusion |
| --- | --- |
| Goal and proof | Normal nullable execution remains V2. The new tests prove that state type versions survive query changes and replay. |
| Scope and abstraction | Reuse the inherited version setter and existing type/metadata paths. No new execution wrapper is needed. |
| Concurrency and locks | Remove a mutation of the function shared with a type. Add no thread, lock, or lock order. New FE type versions are assigned at construction or replay. |
| Lifetime and static initialization | Keep existing function and type ownership. Add no static initializer, temporary state, or ownership cycle. |
| Configuration | Add no configuration switch. The existing dynamic query-version configuration no longer changes an explicit stored type version. |
| Format and rolling upgrade | Add one optional FE JSON property. Retain the existing byte codecs and version fields. Both FE and BE upgrades must finish before version 20 is enabled. |
| Thrift and Protobuf IDs | Change no definitions or IDs. Query types and both tablet schema encoders use their existing version field. |
| Parallel paths | Cover COMBINE, UNION, MERGE, Nereids conversion, query Thrift, replica Thrift, and cloud Protobuf. Pipeline and window evaluators use the same combinator setters. |
| Conditional checks | The missing JSON field is a real old-format case. Its fallback is documented and tested. |
| Positive and negative tests | Cover both version directions, missing metadata, serialized bytes, results, replay, and the existing storage/NULL/selected-reader tests. |
| Expected results | Both generated SQL outputs are `1`. The later regression run compares them without regenerating them. |
| Observability | Existing query profiles and metadata records are sufficient for this version-propagation fix. Add no per-row log or metric. |
| EditLog and persistence | The Gson property follows the existing table metadata path. A real FE restart confirms journal replay as well as the unit round trip. |
| Transactions, writes, and crashes | Keep existing table publication, insert, and compaction transactions. Add no independent write or recovery step. |
| FE-to-BE propagation | Preserve the version in expression types and both schema encoders. Unit tests check all three outputs. |
| CPU and memory | The type-version fix adds no row work or batch allocation. The SUM fix scans the NULL map once before serialization and skips payload loads for all-NULL input. It adds no allocation. Separate source records and paired rechecks bound the performance conclusion. |
| Other limits | Original private issue SQL and status remain unverified. Old bitmap bytes that lost the no-input distinction remain irrecoverable. |

## Additional performance finding

The new kernel run found an intermittent SUM slowdown with all-NULL input.
The slow sample used about 0.04 instructions per cycle. About 93% of sampled CPU
cycles were in the native serialization loop. Instruction annotations placed the
hot samples at the AVX2 masked-load sequence. There was no CPU migration during
the live counter sample.

The first change, `18fb6aabbcc`, selects the value before one output store.
It still produced slow all-NULL samples in three independent processes. Those
records are retained as an unsuccessful intermediate step, not final evidence.

The final change checks whether the batch has any valid input. An all-NULL batch
fills empty SUM states and does not issue payload loads. Other batches select the
value before the output store. The new unit test uses one row in two batches:
first 7, then NULL with hidden payload 7. It checks that a reused output becomes
zero with its NULL mark set. This test checks the state contract; the separate
4,096-row benchmark reproduces the timing problem. No unit test asserts latency.
The existing scalar/native/legacy codec tests and four-NULL-ratio benchmark matrix
also cover the changed path. Each measured source keeps its own records.

## Profile text precision

FE `RuntimeProfile.printCounter(TIME_NS)` prints the fractional remainder as an
integer. It does not pad it to three digits. Thus `12.75ms` means 12 ms plus
75 us, which is 12.075 ms. The previous report parser treated it as 12.750 ms.
One recorded counter reproduced the defect: its parsed minimum exceeded its
maximum. The new test fails before the parser fix and passes after it.

The operator parser now uses the FE remainder rule. Generic BE duration parsing
keeps standard decimal fractions. All four parser tests pass. Earlier operator
summaries are regenerated from their unchanged raw tokens. Query wall times,
CPU audit values, memory audit values, and kernel timings are not changed by this
report correction.

The original DORIS-29405 issue was not available through the connected tools.
The available Atlassian integration was not installed or connected during this
review. The same-class NULL-only partial tests do not establish that the original
issue's exact SQL or current resolution status has been verified.
