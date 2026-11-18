// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_numeric_boundary_coverage", "p0,nonConcurrent") {
    // One stored row forces BE evaluation. The small positive DOUBLE needs a decimal unit
    // that cannot be represented as its reciprocal in DOUBLE (10^309 is infinite).
    sql "drop table if exists test_numeric_boundary_coverage"
    sql """create table test_numeric_boundary_coverage (k int, x double)
        duplicate key(k) distributed by hash(k) buckets 1
        properties ('replication_num' = '1')"""
    sql "insert into test_numeric_boundary_coverage values (0, 1e-310)"
    order_qt_positive_rounding """select round(x, 309) = 0, round_bankers(x, 309) = 0,
        truncate(x, 309) = 0, floor(x, 309) = 0,
        ceil(x, 309) = cast('1e-309' as double),
        floor(-x, 309) = cast('-1e-309' as double), ceil(-x, 309) = 0,
        round(x, 32767) = x from test_numeric_boundary_coverage"""
    // These aliases must reach the same BE rounding implementation with a column argument.
    order_qt_rounding_aliases """select dround(cast('1e19' as double) + k, -20),
        dfloor(cast('1e19' as double) + k, -20), dceil(cast('1e19' as double) + k, -20)
        from test_numeric_boundary_coverage"""
    test {
        sql "select floor(cast(-1 as double) + k, -309) from test_numeric_boundary_coverage"
        exception "Arithmetic overflow"
    }
    // A valid ordinal must still select the second of two fields. Use a literal because
    // STRUCT analysis requires the field to be known before it folds CAST expressions.
    // StructOrdinalTest covers a LARGEINT literal directly at the FE expression boundary.
    order_qt_struct_second """select element_at(struct(k, 7), 2)
        from test_numeric_boundary_coverage"""
    order_qt_bitmap_zero """select bitmap_contains(to_bitmap(k), k),
        bitmap_count(bitmap_remove(to_bitmap(k), k)) from test_numeric_boundary_coverage"""
    // Disable the parallel scanner so the scan-range byte divisor is used.
    // 4096 MiB used to wrap to zero. Read the same single row; do not create a 4 GiB file.
    sql "set enable_parallel_scan = false"
    setBeConfigTemporary([doris_scan_range_max_mb: 4096]) {
        order_qt_large_scan_range "select k from test_numeric_boundary_coverage"
    }
}
