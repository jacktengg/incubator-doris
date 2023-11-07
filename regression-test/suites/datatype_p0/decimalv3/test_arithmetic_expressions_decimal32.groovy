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

suite("test_arithmetic_expressions_decimal32") {
    sql "drop table if exists test_decimalv3_32"
    sql """
    CREATE TABLE IF NOT EXISTS test_decimalv3_32 (
      k1 decimalv3(8, 3),
      k2 decimalv3(8, 4),
      k3 decimalv3(9, 5)
    ) ENGINE=OLAP
    COMMENT "OLAP"
    DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 8
    PROPERTIES (
    "replication_num" = "1"
    );
    """
    sql """
        insert into test_decimalv3_32 values
            (0.001, 0.0001, 0.00001),
            (-0.001, -0.0001, -0.00001),
            (0.999, 0.9999, 0.99999),
            (-0.999, -0.9999, -0.99999),
            (0.123, 0.1234, 0.12345),
            (-0.123, -0.1234, -0.12345),
            (0, 0, 0),
            (1, 1, 1),
            (12345.678, 2345.6789, 1234.56789),
            (-12345.678, -2345.6789, -1234.56789),
            (99999.999, 9999.9999, 9999.99999),
            (-99999.999, -9999.9999, -9999.99999),
            (null, null, null);
    """
    qt_decimal32_select_all "select * from test_decimalv3_32 order by 1,2,3;"


    //=================================
    // decimal32 add/sub
    //=================================

    // disable decimal256
    sql "set enable_decimal256 = false;"
    qt_decimal32_add_sub_no_overflow0 "select k1, k2, k1 + k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_add_sub_no_overflow1 "select k1, k3, k1 + k3 from test_decimalv3_32 order by 1;"
    // result precision is 38
    qt_decimal32_add_sub_no_overflow2 """
        select 
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_no_overflow3 "select k1, k2, k1 - k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_add_sub_no_overflow4 "select k1, k2, k1 - k3 from test_decimalv3_32 order by 1;"
    // result precision is 38
    qt_decimal32_add_sub_no_overflow5 """
        select 
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    // precision is 39, overflow, result type is double
    qt_decimal32_add_sub_overflow0 """
        select 
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_overflow1 """
        select 
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    // enable decimal256
    sql "set enable_decimal256 = true;"

    qt_decimal32_add_sub_decimal256_no_overflow0 "select k1, k2, k1 + k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_add_sub_decimal256_no_overflow1 "select k1, k3, k1 + k3 from test_decimalv3_32 order by 1;"
    // result precision is 38
    qt_decimal32_add_sub_decimal256_no_overflow2 """
        select 
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_decimal256_no_overflow3 "select k1, k2, k1 - k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_add_sub_decimal256_no_overflow4 "select k1, k2, k1 - k3 from test_decimalv3_32 order by 1;"
    // result precision is 38
    qt_decimal32_add_sub_decimal256_no_overflow5 """
        select 
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    // result precision is 39
    qt_decimal32_add_sub_decimal256_no_overflow6 """
        select
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_decimal256_no_overflow7 """
        select
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    // result precision is 76
    qt_decimal32_add_sub_decimal256_no_overflow8 """
        select
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_decimal256_no_overflow9 """
        select
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    // result precision is 77, overflow, result type is double
    qt_decimal32_add_sub_decimal256_overflow0 """
        select
            k3 + k3 + 
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 + k3 +
            k3 + k3 + k3 + k3 + k3 + k3 + k3
        from test_decimalv3_32 order by 1;
    """
    qt_decimal32_add_sub_decimal256_overflow1 """
        select
            k3 - k3 - 
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 - k3 -
            k3 - k3 - k3 - k3 - k3 - k3 - k3
        from test_decimalv3_32 order by 1;
    """

    //=================================
    // decimal32 multiply/division
    //=================================

    // disable decimal256
    sql "set enable_decimal256 = false;"
    qt_decimal32_multi_div_no_overflow0 "select k1 * k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_multi_div_no_overflow1 """
        select k3 * k3 from test_decimalv3_32 order by 1;
    """
    qt_decimal32_multi_div_no_overflow2 """
        select
            k3, k3 * k3 * k3 * k3
        from test_decimalv3_32 order by 1, 2;
    """
    qt_decimal32_multi_div_no_overflow3 "select k1 / k2 from test_decimalv3_32 order by 1;"
    qt_decimal32_multi_div_no_overflow4 """
        select k3 / k3 from test_decimalv3_32 order by 1;
    """
    // TODO: devide by 0???
    /*
    postgres 16:
    k3	?column?
-9999.99999	0.000000010000000020000000
-1234.56789	0.000000656100011941020165
-0.99999	1.00002000030000400005
-0.12345	65.61721769545441645703
-0.00001	10000000000.00000000000000000000
0.00001	10000000000.00000000000000000000
0.12345	65.61721769545441645703
0.99999	1.00002000030000400005
1.00000	1.00000000000000000000
1234.56789	0.000000656100011941020165
9999.99999	0.000000010000000020000000
    */
    qt_decimal32_multi_div_no_overflow5 """
        select
            k3, k3 / k3 / k3 / k3
        from test_decimalv3_32 where k3 != 0 order by 1, 2;
    """

    // overflow, calculated result precision is 9 * 5 = 45,
    // result precision is set to MAX_DECIMAL128_PRECISION, result scale is 25
    qt_decimal32_multi_div_overflow0 """
        select
            k3, k3 * k3 * k3 * k3 * k3
        from test_decimalv3_32 order by 1,2;
    """
    // overflow, result type is double
    qt_decimal32_multi_div_overflow1 """
        select
            k3, k3 / k3 / k3 / k3 / k3
        from test_decimalv3_32 where k3 != 0 order by 1,2;
    """

    // enable decimal256
    sql "set enable_decimal256 = true;"
    qt_decimal32_multi_div_decimal256_no_overflow0 """
        select
            k3, k3 * k3 * k3 * k3 * k3
        from test_decimalv3_32 order by 1,2;
    """
    qt_decimal32_multi_div_decimal256_no_overflow1 """
        select
            k3, k3 / k3 / k3 / k3 / k3
        from test_decimalv3_32 where k3 != 0 order by 1,2;
    """
    qt_decimal32_multi_div_decimal256_no_overflow2 """
        select
            k3, k3 * k3 * k3 * k3 * k3 * k3 * k3 * k3
        from test_decimalv3_32 order by 1,2;
    """
    qt_decimal32_multi_div_decimal256_no_overflow3 """
        select
            k3, k3 / k3 / k3 / k3 / k3 / k3 / k3 / k3
        from test_decimalv3_32 where k3 != 0 order by 1,2;
    """

    // overflow, calculated result precision is 9 * 9 = 81,
    // result precision is set to MAX_DECIMAL256_PRECISION(76), result scale is 45
    qt_decimal32_multi_div_decimal256_overflow0 """
        select
            k3, k3 * k3 * k3 * k3 * k3 * k3 * k3 * k3 * k3
        from test_decimalv3_32 order by 1,2;
    """
    // overflow, result type is double
    qt_decimal32_multi_div_decimal256_overflow1 """
        select
            k3, k3 / k3 / k3 / k3 / k3 / k3 / k3 / k3 / k3
        from test_decimalv3_32 where k3 != 0 order by 1,2;
    """
}