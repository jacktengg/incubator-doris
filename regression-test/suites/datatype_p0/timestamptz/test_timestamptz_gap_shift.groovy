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

suite("test_timestamptz_gap_shift") {
    def originalTimeZone = sql("select @@time_zone")[0][0]
    def originalSkipFold = sql("select @@debug_skip_fold_constant")[0][0]
    // Each case uses one constant row. No table or data load is needed.
    // Apia skipped 2011-12-30. Keep midnight, noon, the fraction, and the last microsecond.
    // The UTC values use explicit offsets, so they do not depend on gap resolution.
    def cases = [
        ["Pacific/Apia", "2011-12-30 00:00:00", "2011-12-30 10:00:00+00:00"],
        ["Pacific/Apia", "2011-12-30 12:00:00", "2011-12-30 22:00:00+00:00"],
        ["Pacific/Apia", "2011-12-30 12:00:00.123456", "2011-12-30 22:00:00.123456+00:00"],
        ["Pacific/Apia", "2011-12-30 23:59:59.999999", "2011-12-31 09:59:59.999999+00:00"],
        // New York moves 02:30 to 03:30. Lord Howe moves 02:15 to 02:45.
        // These gaps have different lengths. The rule must use the actual gap length.
        ["America/New_York", "2024-03-10 02:30:00.123456", "2024-03-10 07:30:00.123456+00:00"],
        ["Australia/Lord_Howe", "2024-10-06 02:15:00.123456", "2024-10-05 15:45:00.123456+00:00"],
        // A repeated time must still select the first occurrence.
        ["America/New_York", "2024-11-03 01:30:00", "2024-11-03 05:30:00+00:00"]
    ]
    try {
        [false, true].each { skipFold ->
            // Run the same input with FE constant folding and with BE evaluation.
            sql "set debug_skip_fold_constant = ${skipFold}"
            cases.eachWithIndex { entry, index ->
                def (zone, local, expected) = entry
                sql "set time_zone = '${zone}'"
                "order_qt_gap_${index}_${skipFold}"("""
                    select cast(ts as string),
                           cast(cast(cast('${local}' as datetimev2(6)) as timestamptz(6)) as string),
                           cast(cast(cast('${local}' as timestamp_ns) as timestamptz(6)) as string),
                           microseconds_diff(ts, cast('${expected}' as timestamptz(6)))
                    from (select cast('${local}' as timestamptz(6)) as ts) t
                """)
                // The named zone must override a different session zone.
                sql "set time_zone = '+00:00'"
                "order_qt_named_gap_${index}_${skipFold}"("""
                    select cast('${local} ${zone}' as timestamptz(6))
                """)
            }
            sql "set time_zone = 'Pacific/Apia'"
            // This is the reported failure. Noon must be 43200 seconds after the transition.
            "order_qt_apia_noon_${skipFold}"("""
                select cast(ts as string),
                       seconds_diff(ts, cast('2011-12-30 10:00:00+00:00' as timestamptz(6))),
                       seconds_diff(ts, cast('2011-12-30 22:00:00+00:00' as timestamptz(6)))
                from (select cast('2011-12-30 12:00:00' as timestamptz(6)) as ts) t
            """)
        }
    } finally {
        sql "set debug_skip_fold_constant = ${originalSkipFold}"
        sql "set time_zone = '${originalTimeZone}'"
    }
}
