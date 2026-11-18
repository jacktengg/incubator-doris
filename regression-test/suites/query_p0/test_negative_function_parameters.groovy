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

suite("test_negative_function_parameters") {
    // FE must reject the BE-invalid scale before NULL propagation removes the function.
    // These checks need no table or input rows.
    def roundingFunctions = ["round", "round_bankers", "truncate", "floor", "ceil", "dround", "dfloor", "dceil"]
    roundingFunctions.each { function ->
        test {
            sql "select ${function}(null, -32769)"
            exception "Scale argument for function"
        }
        test {
            sql "select ${function}(cast(1 as double), 32768)"
            exception "Scale argument for function"
        }
        test {
            sql "select ${function}(cast(1 as double), -32769)"
            exception "Scale argument for function"
        }
    }

    // One row exercises BE column arguments. Three numeric values cover -1 and the two
    // signed minima. The path column exercises JSON parsing after FE constant folding.
    sql "drop table if exists test_negative_function_parameters"
    sql """create table test_negative_function_parameters (
        i int not null, l bigint not null, n bigint not null, p string not null
    ) duplicate key(i) distributed by hash(i) buckets 1
    properties("replication_num" = "1")"""
    sql """insert into test_negative_function_parameters values
        (-2147483648, -9223372036854775808, -1, '\$[last--1]')"""

    // Valid negative positions retain their defined results. INT_MIN must not overflow
    // when a function takes its absolute value or subtracts one.
    order_qt_string_columns """select right('a', i), split_part('a,', ',', i),
        substring_index('a', ',', i), count_substrings('a', 'a', i),
        export_set(1, '1', '0', ',', cast(n + 1 as int))
        from test_negative_function_parameters"""
    // Literal calls also cover FE string folding with the same minimum input data.
    order_qt_string_literals """select right('a', -2147483648),
        split_part('a,', ',', -2147483648), substring_index('a', ',', -2147483648)"""
    order_qt_array_bounds """select array_slice([1], l),
        array_slice([1,2], 2, 9223372036854775807), array_slice([1,2], n, 1),
        array_slice([1], 1, n) from test_negative_function_parameters"""
    // A singleton must support -1 like a multi-element bitmap. Two elements suffice
    // for the minimum signed offset to reach the SET or BITMAP implementation.
    order_qt_bitmap_offsets """select bitmap_to_string(sub_bitmap(to_bitmap(7), n, 1)),
        sub_bitmap(bitmap_from_string('1,2'), l, 1) is null
        from test_negative_function_parameters"""
    order_qt_width_negative_bounds """select width_bucket(n, cast(-1 as bigint), cast(0 as bigint), cast(2 as bigint)),
        width_bucket(n, cast(-2 as bigint), cast(0 as bigint), cast(2 as bigint)) from test_negative_function_parameters"""
    // A column keeps rounding on BE. A literal exercises FE with the same value.
    // The decimal factor for scale -20 must be 10^20, not UINT64_MAX.
    order_qt_round_column """select round(cast('1e19' as double) + n, -20),
        round_bankers(cast('1e19' as double) + n, -20),
        truncate(cast('1e19' as double) + n, -20),
        floor(cast('1e19' as double) + n, -20),
        ceil(cast('1e19' as double) + n, -20)
        from test_negative_function_parameters"""
    order_qt_round_literal "select round(cast('1e19' as double), -20)"
    order_qt_zero_weight "select topn_weighted(1, 0, 1)"
    // Zero keeps the no-wait behavior, and a NULL duration keeps the NULL result.
    order_qt_zero_sleep "select sleep(0), sleep(null)"
    order_qt_zero_window """select window_funnel_v2(0, 'default',
        cast('2024-01-01' as datetime), false)"""

    // -1 must not select or remove UINT64_MAX. The first two checks use BE columns;
    // the next two use constants, which FE must reject before NULL propagation.
    ["bitmap_contains", "bitmap_remove"].each { function ->
        test {
            sql """select ${function}(to_bitmap('18446744073709551615'), n)
                from test_negative_function_parameters"""
            exception "element must be >= 0"
        }
        test {
            sql "select ${function}(null, -1)"
            exception "element must be between 0"
        }
    }
    test {
        sql "select topn_weighted(i, n, 1) from test_negative_function_parameters"
        exception "weight must be nonnegative"
    }
    test {
        // A column reaches BE. A negative duration must not return successful completion.
        sql "select sleep(i) from test_negative_function_parameters"
        exception "sleep duration must be nonnegative"
    }
    test {
        // CRC32 has no FE executor. Its result is zero, so BE receives window -1.
        // A false event formerly skipped validation and returned 0.
        sql """select window_funnel_v2(cast(crc32('') as bigint) - 1, 'default',
            cast('2024-01-01' as datetime), false)"""
        exception "window must be a nonnegative BIGINT"
    }
    test {
        sql "select json_extract_int('[1]', p) from test_negative_function_parameters"
        exception "Invalid Json Path"
    }
    test {
        sql "select json_extract_int('[1]', '\$[last--2147483648]')"
        exception "Invalid Json Path"
    }
    test {
        sql "select width_bucket(n, n, n, 1) from test_negative_function_parameters"
        exception "bounds must be different"
    }
    // FE and BE use the same signed 16-bit scale. Constant folding must not hide an invalid scale.
    roundingFunctions.each { function ->
        test {
            sql "select ${function}(cast(1 as double), i) from test_negative_function_parameters"
            exception "Scale argument for function"
        }
    }
    // These errors need no table. Full-width validation must precede an INT or BIGINT cast.
    [
        ["select sleep(-1)", "sleep duration must be between"],
        ["select sleep(-18446744073709551616)", "sleep duration must be between"],
        ["select now(-18446744073709551616)", "Precision of NOW must be between"],
        ["select utc_timestamp(-18446744073709551616)", "Precision of UTC_TIMESTAMP must be between"],
        ["select utc_time(-18446744073709551616)", "scale must be between 0 and 6"],
        ["select ngram_search('a', 'a', -18446744073709551615)", "gram_num must be a positive constant"],
        ["select ntile(-18446744073709551615) over ()", "within the range of BIGINT"],
        ["select width_bucket(-1, -1, 0, -1)", "number of buckets must be between"],
        ["select width_bucket(0, -1, 0, 9223372036854775807)", "number of buckets must be between"],
        ["select topn_weighted(1, -1, 1)", "weight must be a nonnegative BIGINT"],
        ["select datasketches_hll_union_agg('', -18446744073709551609)", "requires lg_max_k to be between"]
    ].each { entry ->
        test {
            sql entry[0]
            exception entry[1]
        }
    }
}
