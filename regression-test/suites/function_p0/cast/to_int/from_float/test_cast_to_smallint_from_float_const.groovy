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


suite("test_cast_to_smallint_from_float_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as float) as smallint);"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "-0", cast(cast("-0" as float) as smallint);"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "0", cast(cast("0" as float) as smallint);"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "1", cast(cast("1" as float) as smallint);"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "9", cast(cast("9" as float) as smallint);"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "123", cast(cast("123" as float) as smallint);"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    def const_sql_0_6 = """select "127.9", cast(cast("127.9" as float) as smallint);"""
    qt_sql_0_6_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    def const_sql_0_7 = """select "-1", cast(cast("-1" as float) as smallint);"""
    qt_sql_0_7_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")
    def const_sql_0_8 = """select "-9", cast(cast("-9" as float) as smallint);"""
    qt_sql_0_8_strict "${const_sql_0_8}"
    testFoldConst("${const_sql_0_8}")
    def const_sql_0_9 = """select "-123", cast(cast("-123" as float) as smallint);"""
    qt_sql_0_9_strict "${const_sql_0_9}"
    testFoldConst("${const_sql_0_9}")
    def const_sql_0_10 = """select "-128.9", cast(cast("-128.9" as float) as smallint);"""
    qt_sql_0_10_strict "${const_sql_0_10}"
    testFoldConst("${const_sql_0_10}")
    def const_sql_0_11 = """select "-32768", cast(cast("-32768" as float) as smallint);"""
    qt_sql_0_11_strict "${const_sql_0_11}"
    testFoldConst("${const_sql_0_11}")
    def const_sql_0_12 = """select "1.9", cast(cast("1.9" as float) as smallint);"""
    qt_sql_0_12_strict "${const_sql_0_12}"
    testFoldConst("${const_sql_0_12}")
    def const_sql_0_13 = """select "-1.9", cast(cast("-1.9" as float) as smallint);"""
    qt_sql_0_13_strict "${const_sql_0_13}"
    testFoldConst("${const_sql_0_13}")
    def const_sql_0_14 = """select "0.9999", cast(cast("0.9999" as float) as smallint);"""
    qt_sql_0_14_strict "${const_sql_0_14}"
    testFoldConst("${const_sql_0_14}")
    def const_sql_0_15 = """select "-0.9999", cast(cast("-0.9999" as float) as smallint);"""
    qt_sql_0_15_strict "${const_sql_0_15}"
    testFoldConst("${const_sql_0_15}")
    def const_sql_0_16 = """select "1e-45", cast(cast("1e-45" as float) as smallint);"""
    qt_sql_0_16_strict "${const_sql_0_16}"
    testFoldConst("${const_sql_0_16}")
    def const_sql_0_17 = """select "-1e-45", cast(cast("-1e-45" as float) as smallint);"""
    qt_sql_0_17_strict "${const_sql_0_17}"
    testFoldConst("${const_sql_0_17}")
    def const_sql_0_18 = """select "32767.9", cast(cast("32767.9" as float) as smallint);"""
    qt_sql_0_18_strict "${const_sql_0_18}"
    testFoldConst("${const_sql_0_18}")
    def const_sql_0_19 = """select "-32768.9", cast(cast("-32768.9" as float) as smallint);"""
    qt_sql_0_19_strict "${const_sql_0_19}"
    testFoldConst("${const_sql_0_19}")

    sql "set enable_strict_cast=false;"
    qt_sql_0_0_non_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    qt_sql_0_1_non_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    qt_sql_0_2_non_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    qt_sql_0_3_non_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    qt_sql_0_4_non_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    qt_sql_0_5_non_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    qt_sql_0_6_non_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    qt_sql_0_7_non_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")
    qt_sql_0_8_non_strict "${const_sql_0_8}"
    testFoldConst("${const_sql_0_8}")
    qt_sql_0_9_non_strict "${const_sql_0_9}"
    testFoldConst("${const_sql_0_9}")
    qt_sql_0_10_non_strict "${const_sql_0_10}"
    testFoldConst("${const_sql_0_10}")
    qt_sql_0_11_non_strict "${const_sql_0_11}"
    testFoldConst("${const_sql_0_11}")
    qt_sql_0_12_non_strict "${const_sql_0_12}"
    testFoldConst("${const_sql_0_12}")
    qt_sql_0_13_non_strict "${const_sql_0_13}"
    testFoldConst("${const_sql_0_13}")
    qt_sql_0_14_non_strict "${const_sql_0_14}"
    testFoldConst("${const_sql_0_14}")
    qt_sql_0_15_non_strict "${const_sql_0_15}"
    testFoldConst("${const_sql_0_15}")
    qt_sql_0_16_non_strict "${const_sql_0_16}"
    testFoldConst("${const_sql_0_16}")
    qt_sql_0_17_non_strict "${const_sql_0_17}"
    testFoldConst("${const_sql_0_17}")
    qt_sql_0_18_non_strict "${const_sql_0_18}"
    testFoldConst("${const_sql_0_18}")
    qt_sql_0_19_non_strict "${const_sql_0_19}"
    testFoldConst("${const_sql_0_19}")
}