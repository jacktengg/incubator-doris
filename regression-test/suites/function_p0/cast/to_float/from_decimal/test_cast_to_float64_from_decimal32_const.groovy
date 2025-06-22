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


suite("test_cast_to_float64_from_decimal32_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(1, 0)) as double);"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "0", cast(cast("0" as decimalv3(1, 0)) as double);"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "1", cast(cast("1" as decimalv3(1, 0)) as double);"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "-1", cast(cast("-1" as decimalv3(1, 0)) as double);"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "8", cast(cast("8" as decimalv3(1, 0)) as double);"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "-8", cast(cast("-8" as decimalv3(1, 0)) as double);"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    def const_sql_0_6 = """select "9", cast(cast("9" as decimalv3(1, 0)) as double);"""
    qt_sql_0_6_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    def const_sql_0_7 = """select "-9", cast(cast("-9" as decimalv3(1, 0)) as double);"""
    qt_sql_0_7_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0", cast(cast("0.0" as decimalv3(1, 1)) as double);"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.0", cast(cast("0.0" as decimalv3(1, 1)) as double);"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.1", cast(cast("0.1" as decimalv3(1, 1)) as double);"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "-0.1", cast(cast("-0.1" as decimalv3(1, 1)) as double);"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "0.8", cast(cast("0.8" as decimalv3(1, 1)) as double);"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "-0.8", cast(cast("-0.8" as decimalv3(1, 1)) as double);"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "0.9", cast(cast("0.9" as decimalv3(1, 1)) as double);"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "-0.9", cast(cast("-0.9" as decimalv3(1, 1)) as double);"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")

    sql "set enable_strict_cast=false;"
    qt_sql_1_0_non_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    qt_sql_1_1_non_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    qt_sql_1_2_non_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    qt_sql_1_3_non_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    qt_sql_1_4_non_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    qt_sql_1_5_non_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    qt_sql_1_6_non_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    qt_sql_1_7_non_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0", cast(cast("0" as decimalv3(9, 0)) as double);"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0", cast(cast("0" as decimalv3(9, 0)) as double);"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "1", cast(cast("1" as decimalv3(9, 0)) as double);"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "-1", cast(cast("-1" as decimalv3(9, 0)) as double);"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "9", cast(cast("9" as decimalv3(9, 0)) as double);"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "-9", cast(cast("-9" as decimalv3(9, 0)) as double);"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "99999999", cast(cast("99999999" as decimalv3(9, 0)) as double);"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "-99999999", cast(cast("-99999999" as decimalv3(9, 0)) as double);"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "900000000", cast(cast("900000000" as decimalv3(9, 0)) as double);"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "-900000000", cast(cast("-900000000" as decimalv3(9, 0)) as double);"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "900000001", cast(cast("900000001" as decimalv3(9, 0)) as double);"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "-900000001", cast(cast("-900000001" as decimalv3(9, 0)) as double);"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "999999998", cast(cast("999999998" as decimalv3(9, 0)) as double);"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "-999999998", cast(cast("-999999998" as decimalv3(9, 0)) as double);"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "999999999", cast(cast("999999999" as decimalv3(9, 0)) as double);"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "-999999999", cast(cast("-999999999" as decimalv3(9, 0)) as double);"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")

    sql "set enable_strict_cast=false;"
    qt_sql_2_0_non_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    qt_sql_2_1_non_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    qt_sql_2_2_non_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    qt_sql_2_3_non_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    qt_sql_2_4_non_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    qt_sql_2_5_non_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    qt_sql_2_6_non_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    qt_sql_2_7_non_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    qt_sql_2_8_non_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    qt_sql_2_9_non_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    qt_sql_2_10_non_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    qt_sql_2_11_non_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    qt_sql_2_12_non_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    qt_sql_2_13_non_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    qt_sql_2_14_non_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    qt_sql_2_15_non_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.0", cast(cast("0.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.0", cast(cast("0.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.1", cast(cast("0.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "-0.1", cast(cast("-0.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.8", cast(cast("0.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "-0.8", cast(cast("-0.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.9", cast(cast("0.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "-0.9", cast(cast("-0.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "1.0", cast(cast("1.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "-1.0", cast(cast("-1.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "1.1", cast(cast("1.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "-1.1", cast(cast("-1.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "1.8", cast(cast("1.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "-1.8", cast(cast("-1.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "1.9", cast(cast("1.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "-1.9", cast(cast("-1.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "9.0", cast(cast("9.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "-9.0", cast(cast("-9.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "9.1", cast(cast("9.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "-9.1", cast(cast("-9.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "9.8", cast(cast("9.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "-9.8", cast(cast("-9.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "9.9", cast(cast("9.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "-9.9", cast(cast("-9.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_23_strict "${const_sql_3_23}"
    testFoldConst("${const_sql_3_23}")
    def const_sql_3_24 = """select "9999999.0", cast(cast("9999999.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_24_strict "${const_sql_3_24}"
    testFoldConst("${const_sql_3_24}")
    def const_sql_3_25 = """select "-9999999.0", cast(cast("-9999999.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_25_strict "${const_sql_3_25}"
    testFoldConst("${const_sql_3_25}")
    def const_sql_3_26 = """select "9999999.1", cast(cast("9999999.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_26_strict "${const_sql_3_26}"
    testFoldConst("${const_sql_3_26}")
    def const_sql_3_27 = """select "-9999999.1", cast(cast("-9999999.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_27_strict "${const_sql_3_27}"
    testFoldConst("${const_sql_3_27}")
    def const_sql_3_28 = """select "9999999.8", cast(cast("9999999.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_28_strict "${const_sql_3_28}"
    testFoldConst("${const_sql_3_28}")
    def const_sql_3_29 = """select "-9999999.8", cast(cast("-9999999.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_29_strict "${const_sql_3_29}"
    testFoldConst("${const_sql_3_29}")
    def const_sql_3_30 = """select "9999999.9", cast(cast("9999999.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_30_strict "${const_sql_3_30}"
    testFoldConst("${const_sql_3_30}")
    def const_sql_3_31 = """select "-9999999.9", cast(cast("-9999999.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_31_strict "${const_sql_3_31}"
    testFoldConst("${const_sql_3_31}")
    def const_sql_3_32 = """select "90000000.0", cast(cast("90000000.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_32_strict "${const_sql_3_32}"
    testFoldConst("${const_sql_3_32}")
    def const_sql_3_33 = """select "-90000000.0", cast(cast("-90000000.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_33_strict "${const_sql_3_33}"
    testFoldConst("${const_sql_3_33}")
    def const_sql_3_34 = """select "90000000.1", cast(cast("90000000.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_34_strict "${const_sql_3_34}"
    testFoldConst("${const_sql_3_34}")
    def const_sql_3_35 = """select "-90000000.1", cast(cast("-90000000.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_35_strict "${const_sql_3_35}"
    testFoldConst("${const_sql_3_35}")
    def const_sql_3_36 = """select "90000000.8", cast(cast("90000000.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_36_strict "${const_sql_3_36}"
    testFoldConst("${const_sql_3_36}")
    def const_sql_3_37 = """select "-90000000.8", cast(cast("-90000000.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_37_strict "${const_sql_3_37}"
    testFoldConst("${const_sql_3_37}")
    def const_sql_3_38 = """select "90000000.9", cast(cast("90000000.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_38_strict "${const_sql_3_38}"
    testFoldConst("${const_sql_3_38}")
    def const_sql_3_39 = """select "-90000000.9", cast(cast("-90000000.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_39_strict "${const_sql_3_39}"
    testFoldConst("${const_sql_3_39}")
    def const_sql_3_40 = """select "90000001.0", cast(cast("90000001.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_40_strict "${const_sql_3_40}"
    testFoldConst("${const_sql_3_40}")
    def const_sql_3_41 = """select "-90000001.0", cast(cast("-90000001.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_41_strict "${const_sql_3_41}"
    testFoldConst("${const_sql_3_41}")
    def const_sql_3_42 = """select "90000001.1", cast(cast("90000001.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_42_strict "${const_sql_3_42}"
    testFoldConst("${const_sql_3_42}")
    def const_sql_3_43 = """select "-90000001.1", cast(cast("-90000001.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_43_strict "${const_sql_3_43}"
    testFoldConst("${const_sql_3_43}")
    def const_sql_3_44 = """select "90000001.8", cast(cast("90000001.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_44_strict "${const_sql_3_44}"
    testFoldConst("${const_sql_3_44}")
    def const_sql_3_45 = """select "-90000001.8", cast(cast("-90000001.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_45_strict "${const_sql_3_45}"
    testFoldConst("${const_sql_3_45}")
    def const_sql_3_46 = """select "90000001.9", cast(cast("90000001.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_46_strict "${const_sql_3_46}"
    testFoldConst("${const_sql_3_46}")
    def const_sql_3_47 = """select "-90000001.9", cast(cast("-90000001.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_47_strict "${const_sql_3_47}"
    testFoldConst("${const_sql_3_47}")
    def const_sql_3_48 = """select "99999998.0", cast(cast("99999998.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_48_strict "${const_sql_3_48}"
    testFoldConst("${const_sql_3_48}")
    def const_sql_3_49 = """select "-99999998.0", cast(cast("-99999998.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_49_strict "${const_sql_3_49}"
    testFoldConst("${const_sql_3_49}")
    def const_sql_3_50 = """select "99999998.1", cast(cast("99999998.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_50_strict "${const_sql_3_50}"
    testFoldConst("${const_sql_3_50}")
    def const_sql_3_51 = """select "-99999998.1", cast(cast("-99999998.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_51_strict "${const_sql_3_51}"
    testFoldConst("${const_sql_3_51}")
    def const_sql_3_52 = """select "99999998.8", cast(cast("99999998.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_52_strict "${const_sql_3_52}"
    testFoldConst("${const_sql_3_52}")
    def const_sql_3_53 = """select "-99999998.8", cast(cast("-99999998.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_53_strict "${const_sql_3_53}"
    testFoldConst("${const_sql_3_53}")
    def const_sql_3_54 = """select "99999998.9", cast(cast("99999998.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_54_strict "${const_sql_3_54}"
    testFoldConst("${const_sql_3_54}")
    def const_sql_3_55 = """select "-99999998.9", cast(cast("-99999998.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_55_strict "${const_sql_3_55}"
    testFoldConst("${const_sql_3_55}")
    def const_sql_3_56 = """select "99999999.0", cast(cast("99999999.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_56_strict "${const_sql_3_56}"
    testFoldConst("${const_sql_3_56}")
    def const_sql_3_57 = """select "-99999999.0", cast(cast("-99999999.0" as decimalv3(9, 1)) as double);"""
    qt_sql_3_57_strict "${const_sql_3_57}"
    testFoldConst("${const_sql_3_57}")
    def const_sql_3_58 = """select "99999999.1", cast(cast("99999999.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_58_strict "${const_sql_3_58}"
    testFoldConst("${const_sql_3_58}")
    def const_sql_3_59 = """select "-99999999.1", cast(cast("-99999999.1" as decimalv3(9, 1)) as double);"""
    qt_sql_3_59_strict "${const_sql_3_59}"
    testFoldConst("${const_sql_3_59}")
    def const_sql_3_60 = """select "99999999.8", cast(cast("99999999.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_60_strict "${const_sql_3_60}"
    testFoldConst("${const_sql_3_60}")
    def const_sql_3_61 = """select "-99999999.8", cast(cast("-99999999.8" as decimalv3(9, 1)) as double);"""
    qt_sql_3_61_strict "${const_sql_3_61}"
    testFoldConst("${const_sql_3_61}")
    def const_sql_3_62 = """select "99999999.9", cast(cast("99999999.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_62_strict "${const_sql_3_62}"
    testFoldConst("${const_sql_3_62}")
    def const_sql_3_63 = """select "-99999999.9", cast(cast("-99999999.9" as decimalv3(9, 1)) as double);"""
    qt_sql_3_63_strict "${const_sql_3_63}"
    testFoldConst("${const_sql_3_63}")

    sql "set enable_strict_cast=false;"
    qt_sql_3_0_non_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    qt_sql_3_1_non_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    qt_sql_3_2_non_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    qt_sql_3_3_non_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    qt_sql_3_4_non_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    qt_sql_3_5_non_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    qt_sql_3_6_non_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    qt_sql_3_7_non_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    qt_sql_3_8_non_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    qt_sql_3_9_non_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    qt_sql_3_10_non_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    qt_sql_3_11_non_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    qt_sql_3_12_non_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    qt_sql_3_13_non_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    qt_sql_3_14_non_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    qt_sql_3_15_non_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    qt_sql_3_16_non_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    qt_sql_3_17_non_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    qt_sql_3_18_non_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    qt_sql_3_19_non_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    qt_sql_3_20_non_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    qt_sql_3_21_non_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    qt_sql_3_22_non_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    qt_sql_3_23_non_strict "${const_sql_3_23}"
    testFoldConst("${const_sql_3_23}")
    qt_sql_3_24_non_strict "${const_sql_3_24}"
    testFoldConst("${const_sql_3_24}")
    qt_sql_3_25_non_strict "${const_sql_3_25}"
    testFoldConst("${const_sql_3_25}")
    qt_sql_3_26_non_strict "${const_sql_3_26}"
    testFoldConst("${const_sql_3_26}")
    qt_sql_3_27_non_strict "${const_sql_3_27}"
    testFoldConst("${const_sql_3_27}")
    qt_sql_3_28_non_strict "${const_sql_3_28}"
    testFoldConst("${const_sql_3_28}")
    qt_sql_3_29_non_strict "${const_sql_3_29}"
    testFoldConst("${const_sql_3_29}")
    qt_sql_3_30_non_strict "${const_sql_3_30}"
    testFoldConst("${const_sql_3_30}")
    qt_sql_3_31_non_strict "${const_sql_3_31}"
    testFoldConst("${const_sql_3_31}")
    qt_sql_3_32_non_strict "${const_sql_3_32}"
    testFoldConst("${const_sql_3_32}")
    qt_sql_3_33_non_strict "${const_sql_3_33}"
    testFoldConst("${const_sql_3_33}")
    qt_sql_3_34_non_strict "${const_sql_3_34}"
    testFoldConst("${const_sql_3_34}")
    qt_sql_3_35_non_strict "${const_sql_3_35}"
    testFoldConst("${const_sql_3_35}")
    qt_sql_3_36_non_strict "${const_sql_3_36}"
    testFoldConst("${const_sql_3_36}")
    qt_sql_3_37_non_strict "${const_sql_3_37}"
    testFoldConst("${const_sql_3_37}")
    qt_sql_3_38_non_strict "${const_sql_3_38}"
    testFoldConst("${const_sql_3_38}")
    qt_sql_3_39_non_strict "${const_sql_3_39}"
    testFoldConst("${const_sql_3_39}")
    qt_sql_3_40_non_strict "${const_sql_3_40}"
    testFoldConst("${const_sql_3_40}")
    qt_sql_3_41_non_strict "${const_sql_3_41}"
    testFoldConst("${const_sql_3_41}")
    qt_sql_3_42_non_strict "${const_sql_3_42}"
    testFoldConst("${const_sql_3_42}")
    qt_sql_3_43_non_strict "${const_sql_3_43}"
    testFoldConst("${const_sql_3_43}")
    qt_sql_3_44_non_strict "${const_sql_3_44}"
    testFoldConst("${const_sql_3_44}")
    qt_sql_3_45_non_strict "${const_sql_3_45}"
    testFoldConst("${const_sql_3_45}")
    qt_sql_3_46_non_strict "${const_sql_3_46}"
    testFoldConst("${const_sql_3_46}")
    qt_sql_3_47_non_strict "${const_sql_3_47}"
    testFoldConst("${const_sql_3_47}")
    qt_sql_3_48_non_strict "${const_sql_3_48}"
    testFoldConst("${const_sql_3_48}")
    qt_sql_3_49_non_strict "${const_sql_3_49}"
    testFoldConst("${const_sql_3_49}")
    qt_sql_3_50_non_strict "${const_sql_3_50}"
    testFoldConst("${const_sql_3_50}")
    qt_sql_3_51_non_strict "${const_sql_3_51}"
    testFoldConst("${const_sql_3_51}")
    qt_sql_3_52_non_strict "${const_sql_3_52}"
    testFoldConst("${const_sql_3_52}")
    qt_sql_3_53_non_strict "${const_sql_3_53}"
    testFoldConst("${const_sql_3_53}")
    qt_sql_3_54_non_strict "${const_sql_3_54}"
    testFoldConst("${const_sql_3_54}")
    qt_sql_3_55_non_strict "${const_sql_3_55}"
    testFoldConst("${const_sql_3_55}")
    qt_sql_3_56_non_strict "${const_sql_3_56}"
    testFoldConst("${const_sql_3_56}")
    qt_sql_3_57_non_strict "${const_sql_3_57}"
    testFoldConst("${const_sql_3_57}")
    qt_sql_3_58_non_strict "${const_sql_3_58}"
    testFoldConst("${const_sql_3_58}")
    qt_sql_3_59_non_strict "${const_sql_3_59}"
    testFoldConst("${const_sql_3_59}")
    qt_sql_3_60_non_strict "${const_sql_3_60}"
    testFoldConst("${const_sql_3_60}")
    qt_sql_3_61_non_strict "${const_sql_3_61}"
    testFoldConst("${const_sql_3_61}")
    qt_sql_3_62_non_strict "${const_sql_3_62}"
    testFoldConst("${const_sql_3_62}")
    qt_sql_3_63_non_strict "${const_sql_3_63}"
    testFoldConst("${const_sql_3_63}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0.0000", cast(cast("0.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0.0000", cast(cast("0.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0.0001", cast(cast("0.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "-0.0001", cast(cast("-0.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0.0009", cast(cast("0.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "-0.0009", cast(cast("-0.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0.0999", cast(cast("0.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "-0.0999", cast(cast("-0.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "0.9000", cast(cast("0.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "-0.9000", cast(cast("-0.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "0.9001", cast(cast("0.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "-0.9001", cast(cast("-0.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "0.9998", cast(cast("0.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "-0.9998", cast(cast("-0.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "0.9999", cast(cast("0.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "-0.9999", cast(cast("-0.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    def const_sql_4_16 = """select "1.0000", cast(cast("1.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_16_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    def const_sql_4_17 = """select "-1.0000", cast(cast("-1.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_17_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    def const_sql_4_18 = """select "1.0001", cast(cast("1.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_18_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    def const_sql_4_19 = """select "-1.0001", cast(cast("-1.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_19_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    def const_sql_4_20 = """select "1.0009", cast(cast("1.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_20_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    def const_sql_4_21 = """select "-1.0009", cast(cast("-1.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_21_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    def const_sql_4_22 = """select "1.0999", cast(cast("1.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_22_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    def const_sql_4_23 = """select "-1.0999", cast(cast("-1.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_23_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    def const_sql_4_24 = """select "1.9000", cast(cast("1.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_24_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    def const_sql_4_25 = """select "-1.9000", cast(cast("-1.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_25_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    def const_sql_4_26 = """select "1.9001", cast(cast("1.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_26_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    def const_sql_4_27 = """select "-1.9001", cast(cast("-1.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_27_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    def const_sql_4_28 = """select "1.9998", cast(cast("1.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_28_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    def const_sql_4_29 = """select "-1.9998", cast(cast("-1.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_29_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")
    def const_sql_4_30 = """select "1.9999", cast(cast("1.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_30_strict "${const_sql_4_30}"
    testFoldConst("${const_sql_4_30}")
    def const_sql_4_31 = """select "-1.9999", cast(cast("-1.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_31_strict "${const_sql_4_31}"
    testFoldConst("${const_sql_4_31}")
    def const_sql_4_32 = """select "9.0000", cast(cast("9.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_32_strict "${const_sql_4_32}"
    testFoldConst("${const_sql_4_32}")
    def const_sql_4_33 = """select "-9.0000", cast(cast("-9.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_33_strict "${const_sql_4_33}"
    testFoldConst("${const_sql_4_33}")
    def const_sql_4_34 = """select "9.0001", cast(cast("9.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_34_strict "${const_sql_4_34}"
    testFoldConst("${const_sql_4_34}")
    def const_sql_4_35 = """select "-9.0001", cast(cast("-9.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_35_strict "${const_sql_4_35}"
    testFoldConst("${const_sql_4_35}")
    def const_sql_4_36 = """select "9.0009", cast(cast("9.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_36_strict "${const_sql_4_36}"
    testFoldConst("${const_sql_4_36}")
    def const_sql_4_37 = """select "-9.0009", cast(cast("-9.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_37_strict "${const_sql_4_37}"
    testFoldConst("${const_sql_4_37}")
    def const_sql_4_38 = """select "9.0999", cast(cast("9.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_38_strict "${const_sql_4_38}"
    testFoldConst("${const_sql_4_38}")
    def const_sql_4_39 = """select "-9.0999", cast(cast("-9.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_39_strict "${const_sql_4_39}"
    testFoldConst("${const_sql_4_39}")
    def const_sql_4_40 = """select "9.9000", cast(cast("9.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_40_strict "${const_sql_4_40}"
    testFoldConst("${const_sql_4_40}")
    def const_sql_4_41 = """select "-9.9000", cast(cast("-9.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_41_strict "${const_sql_4_41}"
    testFoldConst("${const_sql_4_41}")
    def const_sql_4_42 = """select "9.9001", cast(cast("9.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_42_strict "${const_sql_4_42}"
    testFoldConst("${const_sql_4_42}")
    def const_sql_4_43 = """select "-9.9001", cast(cast("-9.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_43_strict "${const_sql_4_43}"
    testFoldConst("${const_sql_4_43}")
    def const_sql_4_44 = """select "9.9998", cast(cast("9.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_44_strict "${const_sql_4_44}"
    testFoldConst("${const_sql_4_44}")
    def const_sql_4_45 = """select "-9.9998", cast(cast("-9.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_45_strict "${const_sql_4_45}"
    testFoldConst("${const_sql_4_45}")
    def const_sql_4_46 = """select "9.9999", cast(cast("9.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_46_strict "${const_sql_4_46}"
    testFoldConst("${const_sql_4_46}")
    def const_sql_4_47 = """select "-9.9999", cast(cast("-9.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_47_strict "${const_sql_4_47}"
    testFoldConst("${const_sql_4_47}")
    def const_sql_4_48 = """select "9999.0000", cast(cast("9999.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_48_strict "${const_sql_4_48}"
    testFoldConst("${const_sql_4_48}")
    def const_sql_4_49 = """select "-9999.0000", cast(cast("-9999.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_49_strict "${const_sql_4_49}"
    testFoldConst("${const_sql_4_49}")
    def const_sql_4_50 = """select "9999.0001", cast(cast("9999.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_50_strict "${const_sql_4_50}"
    testFoldConst("${const_sql_4_50}")
    def const_sql_4_51 = """select "-9999.0001", cast(cast("-9999.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_51_strict "${const_sql_4_51}"
    testFoldConst("${const_sql_4_51}")
    def const_sql_4_52 = """select "9999.0009", cast(cast("9999.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_52_strict "${const_sql_4_52}"
    testFoldConst("${const_sql_4_52}")
    def const_sql_4_53 = """select "-9999.0009", cast(cast("-9999.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_53_strict "${const_sql_4_53}"
    testFoldConst("${const_sql_4_53}")
    def const_sql_4_54 = """select "9999.0999", cast(cast("9999.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_54_strict "${const_sql_4_54}"
    testFoldConst("${const_sql_4_54}")
    def const_sql_4_55 = """select "-9999.0999", cast(cast("-9999.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_55_strict "${const_sql_4_55}"
    testFoldConst("${const_sql_4_55}")
    def const_sql_4_56 = """select "9999.9000", cast(cast("9999.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_56_strict "${const_sql_4_56}"
    testFoldConst("${const_sql_4_56}")
    def const_sql_4_57 = """select "-9999.9000", cast(cast("-9999.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_57_strict "${const_sql_4_57}"
    testFoldConst("${const_sql_4_57}")
    def const_sql_4_58 = """select "9999.9001", cast(cast("9999.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_58_strict "${const_sql_4_58}"
    testFoldConst("${const_sql_4_58}")
    def const_sql_4_59 = """select "-9999.9001", cast(cast("-9999.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_59_strict "${const_sql_4_59}"
    testFoldConst("${const_sql_4_59}")
    def const_sql_4_60 = """select "9999.9998", cast(cast("9999.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_60_strict "${const_sql_4_60}"
    testFoldConst("${const_sql_4_60}")
    def const_sql_4_61 = """select "-9999.9998", cast(cast("-9999.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_61_strict "${const_sql_4_61}"
    testFoldConst("${const_sql_4_61}")
    def const_sql_4_62 = """select "9999.9999", cast(cast("9999.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_62_strict "${const_sql_4_62}"
    testFoldConst("${const_sql_4_62}")
    def const_sql_4_63 = """select "-9999.9999", cast(cast("-9999.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_63_strict "${const_sql_4_63}"
    testFoldConst("${const_sql_4_63}")
    def const_sql_4_64 = """select "90000.0000", cast(cast("90000.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_64_strict "${const_sql_4_64}"
    testFoldConst("${const_sql_4_64}")
    def const_sql_4_65 = """select "-90000.0000", cast(cast("-90000.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_65_strict "${const_sql_4_65}"
    testFoldConst("${const_sql_4_65}")
    def const_sql_4_66 = """select "90000.0001", cast(cast("90000.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_66_strict "${const_sql_4_66}"
    testFoldConst("${const_sql_4_66}")
    def const_sql_4_67 = """select "-90000.0001", cast(cast("-90000.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_67_strict "${const_sql_4_67}"
    testFoldConst("${const_sql_4_67}")
    def const_sql_4_68 = """select "90000.0009", cast(cast("90000.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_68_strict "${const_sql_4_68}"
    testFoldConst("${const_sql_4_68}")
    def const_sql_4_69 = """select "-90000.0009", cast(cast("-90000.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_69_strict "${const_sql_4_69}"
    testFoldConst("${const_sql_4_69}")
    def const_sql_4_70 = """select "90000.0999", cast(cast("90000.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_70_strict "${const_sql_4_70}"
    testFoldConst("${const_sql_4_70}")
    def const_sql_4_71 = """select "-90000.0999", cast(cast("-90000.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_71_strict "${const_sql_4_71}"
    testFoldConst("${const_sql_4_71}")
    def const_sql_4_72 = """select "90000.9000", cast(cast("90000.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_72_strict "${const_sql_4_72}"
    testFoldConst("${const_sql_4_72}")
    def const_sql_4_73 = """select "-90000.9000", cast(cast("-90000.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_73_strict "${const_sql_4_73}"
    testFoldConst("${const_sql_4_73}")
    def const_sql_4_74 = """select "90000.9001", cast(cast("90000.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_74_strict "${const_sql_4_74}"
    testFoldConst("${const_sql_4_74}")
    def const_sql_4_75 = """select "-90000.9001", cast(cast("-90000.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_75_strict "${const_sql_4_75}"
    testFoldConst("${const_sql_4_75}")
    def const_sql_4_76 = """select "90000.9998", cast(cast("90000.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_76_strict "${const_sql_4_76}"
    testFoldConst("${const_sql_4_76}")
    def const_sql_4_77 = """select "-90000.9998", cast(cast("-90000.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_77_strict "${const_sql_4_77}"
    testFoldConst("${const_sql_4_77}")
    def const_sql_4_78 = """select "90000.9999", cast(cast("90000.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_78_strict "${const_sql_4_78}"
    testFoldConst("${const_sql_4_78}")
    def const_sql_4_79 = """select "-90000.9999", cast(cast("-90000.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_79_strict "${const_sql_4_79}"
    testFoldConst("${const_sql_4_79}")
    def const_sql_4_80 = """select "90001.0000", cast(cast("90001.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_80_strict "${const_sql_4_80}"
    testFoldConst("${const_sql_4_80}")
    def const_sql_4_81 = """select "-90001.0000", cast(cast("-90001.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_81_strict "${const_sql_4_81}"
    testFoldConst("${const_sql_4_81}")
    def const_sql_4_82 = """select "90001.0001", cast(cast("90001.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_82_strict "${const_sql_4_82}"
    testFoldConst("${const_sql_4_82}")
    def const_sql_4_83 = """select "-90001.0001", cast(cast("-90001.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_83_strict "${const_sql_4_83}"
    testFoldConst("${const_sql_4_83}")
    def const_sql_4_84 = """select "90001.0009", cast(cast("90001.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_84_strict "${const_sql_4_84}"
    testFoldConst("${const_sql_4_84}")
    def const_sql_4_85 = """select "-90001.0009", cast(cast("-90001.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_85_strict "${const_sql_4_85}"
    testFoldConst("${const_sql_4_85}")
    def const_sql_4_86 = """select "90001.0999", cast(cast("90001.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_86_strict "${const_sql_4_86}"
    testFoldConst("${const_sql_4_86}")
    def const_sql_4_87 = """select "-90001.0999", cast(cast("-90001.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_87_strict "${const_sql_4_87}"
    testFoldConst("${const_sql_4_87}")
    def const_sql_4_88 = """select "90001.9000", cast(cast("90001.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_88_strict "${const_sql_4_88}"
    testFoldConst("${const_sql_4_88}")
    def const_sql_4_89 = """select "-90001.9000", cast(cast("-90001.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_89_strict "${const_sql_4_89}"
    testFoldConst("${const_sql_4_89}")
    def const_sql_4_90 = """select "90001.9001", cast(cast("90001.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_90_strict "${const_sql_4_90}"
    testFoldConst("${const_sql_4_90}")
    def const_sql_4_91 = """select "-90001.9001", cast(cast("-90001.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_91_strict "${const_sql_4_91}"
    testFoldConst("${const_sql_4_91}")
    def const_sql_4_92 = """select "90001.9998", cast(cast("90001.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_92_strict "${const_sql_4_92}"
    testFoldConst("${const_sql_4_92}")
    def const_sql_4_93 = """select "-90001.9998", cast(cast("-90001.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_93_strict "${const_sql_4_93}"
    testFoldConst("${const_sql_4_93}")
    def const_sql_4_94 = """select "90001.9999", cast(cast("90001.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_94_strict "${const_sql_4_94}"
    testFoldConst("${const_sql_4_94}")
    def const_sql_4_95 = """select "-90001.9999", cast(cast("-90001.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_95_strict "${const_sql_4_95}"
    testFoldConst("${const_sql_4_95}")
    def const_sql_4_96 = """select "99998.0000", cast(cast("99998.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_96_strict "${const_sql_4_96}"
    testFoldConst("${const_sql_4_96}")
    def const_sql_4_97 = """select "-99998.0000", cast(cast("-99998.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_97_strict "${const_sql_4_97}"
    testFoldConst("${const_sql_4_97}")
    def const_sql_4_98 = """select "99998.0001", cast(cast("99998.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_98_strict "${const_sql_4_98}"
    testFoldConst("${const_sql_4_98}")
    def const_sql_4_99 = """select "-99998.0001", cast(cast("-99998.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_99_strict "${const_sql_4_99}"
    testFoldConst("${const_sql_4_99}")
    def const_sql_4_100 = """select "99998.0009", cast(cast("99998.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_100_strict "${const_sql_4_100}"
    testFoldConst("${const_sql_4_100}")
    def const_sql_4_101 = """select "-99998.0009", cast(cast("-99998.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_101_strict "${const_sql_4_101}"
    testFoldConst("${const_sql_4_101}")
    def const_sql_4_102 = """select "99998.0999", cast(cast("99998.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_102_strict "${const_sql_4_102}"
    testFoldConst("${const_sql_4_102}")
    def const_sql_4_103 = """select "-99998.0999", cast(cast("-99998.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_103_strict "${const_sql_4_103}"
    testFoldConst("${const_sql_4_103}")
    def const_sql_4_104 = """select "99998.9000", cast(cast("99998.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_104_strict "${const_sql_4_104}"
    testFoldConst("${const_sql_4_104}")
    def const_sql_4_105 = """select "-99998.9000", cast(cast("-99998.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_105_strict "${const_sql_4_105}"
    testFoldConst("${const_sql_4_105}")
    def const_sql_4_106 = """select "99998.9001", cast(cast("99998.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_106_strict "${const_sql_4_106}"
    testFoldConst("${const_sql_4_106}")
    def const_sql_4_107 = """select "-99998.9001", cast(cast("-99998.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_107_strict "${const_sql_4_107}"
    testFoldConst("${const_sql_4_107}")
    def const_sql_4_108 = """select "99998.9998", cast(cast("99998.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_108_strict "${const_sql_4_108}"
    testFoldConst("${const_sql_4_108}")
    def const_sql_4_109 = """select "-99998.9998", cast(cast("-99998.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_109_strict "${const_sql_4_109}"
    testFoldConst("${const_sql_4_109}")
    def const_sql_4_110 = """select "99998.9999", cast(cast("99998.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_110_strict "${const_sql_4_110}"
    testFoldConst("${const_sql_4_110}")
    def const_sql_4_111 = """select "-99998.9999", cast(cast("-99998.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_111_strict "${const_sql_4_111}"
    testFoldConst("${const_sql_4_111}")
    def const_sql_4_112 = """select "99999.0000", cast(cast("99999.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_112_strict "${const_sql_4_112}"
    testFoldConst("${const_sql_4_112}")
    def const_sql_4_113 = """select "-99999.0000", cast(cast("-99999.0000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_113_strict "${const_sql_4_113}"
    testFoldConst("${const_sql_4_113}")
    def const_sql_4_114 = """select "99999.0001", cast(cast("99999.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_114_strict "${const_sql_4_114}"
    testFoldConst("${const_sql_4_114}")
    def const_sql_4_115 = """select "-99999.0001", cast(cast("-99999.0001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_115_strict "${const_sql_4_115}"
    testFoldConst("${const_sql_4_115}")
    def const_sql_4_116 = """select "99999.0009", cast(cast("99999.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_116_strict "${const_sql_4_116}"
    testFoldConst("${const_sql_4_116}")
    def const_sql_4_117 = """select "-99999.0009", cast(cast("-99999.0009" as decimalv3(9, 4)) as double);"""
    qt_sql_4_117_strict "${const_sql_4_117}"
    testFoldConst("${const_sql_4_117}")
    def const_sql_4_118 = """select "99999.0999", cast(cast("99999.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_118_strict "${const_sql_4_118}"
    testFoldConst("${const_sql_4_118}")
    def const_sql_4_119 = """select "-99999.0999", cast(cast("-99999.0999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_119_strict "${const_sql_4_119}"
    testFoldConst("${const_sql_4_119}")
    def const_sql_4_120 = """select "99999.9000", cast(cast("99999.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_120_strict "${const_sql_4_120}"
    testFoldConst("${const_sql_4_120}")
    def const_sql_4_121 = """select "-99999.9000", cast(cast("-99999.9000" as decimalv3(9, 4)) as double);"""
    qt_sql_4_121_strict "${const_sql_4_121}"
    testFoldConst("${const_sql_4_121}")
    def const_sql_4_122 = """select "99999.9001", cast(cast("99999.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_122_strict "${const_sql_4_122}"
    testFoldConst("${const_sql_4_122}")
    def const_sql_4_123 = """select "-99999.9001", cast(cast("-99999.9001" as decimalv3(9, 4)) as double);"""
    qt_sql_4_123_strict "${const_sql_4_123}"
    testFoldConst("${const_sql_4_123}")
    def const_sql_4_124 = """select "99999.9998", cast(cast("99999.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_124_strict "${const_sql_4_124}"
    testFoldConst("${const_sql_4_124}")
    def const_sql_4_125 = """select "-99999.9998", cast(cast("-99999.9998" as decimalv3(9, 4)) as double);"""
    qt_sql_4_125_strict "${const_sql_4_125}"
    testFoldConst("${const_sql_4_125}")
    def const_sql_4_126 = """select "99999.9999", cast(cast("99999.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_126_strict "${const_sql_4_126}"
    testFoldConst("${const_sql_4_126}")
    def const_sql_4_127 = """select "-99999.9999", cast(cast("-99999.9999" as decimalv3(9, 4)) as double);"""
    qt_sql_4_127_strict "${const_sql_4_127}"
    testFoldConst("${const_sql_4_127}")

    sql "set enable_strict_cast=false;"
    qt_sql_4_0_non_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    qt_sql_4_1_non_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    qt_sql_4_2_non_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    qt_sql_4_3_non_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    qt_sql_4_4_non_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    qt_sql_4_5_non_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    qt_sql_4_6_non_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    qt_sql_4_7_non_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    qt_sql_4_8_non_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    qt_sql_4_9_non_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    qt_sql_4_10_non_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    qt_sql_4_11_non_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    qt_sql_4_12_non_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    qt_sql_4_13_non_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    qt_sql_4_14_non_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    qt_sql_4_15_non_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    qt_sql_4_16_non_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    qt_sql_4_17_non_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    qt_sql_4_18_non_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    qt_sql_4_19_non_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    qt_sql_4_20_non_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    qt_sql_4_21_non_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    qt_sql_4_22_non_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    qt_sql_4_23_non_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    qt_sql_4_24_non_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    qt_sql_4_25_non_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    qt_sql_4_26_non_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    qt_sql_4_27_non_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    qt_sql_4_28_non_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    qt_sql_4_29_non_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")
    qt_sql_4_30_non_strict "${const_sql_4_30}"
    testFoldConst("${const_sql_4_30}")
    qt_sql_4_31_non_strict "${const_sql_4_31}"
    testFoldConst("${const_sql_4_31}")
    qt_sql_4_32_non_strict "${const_sql_4_32}"
    testFoldConst("${const_sql_4_32}")
    qt_sql_4_33_non_strict "${const_sql_4_33}"
    testFoldConst("${const_sql_4_33}")
    qt_sql_4_34_non_strict "${const_sql_4_34}"
    testFoldConst("${const_sql_4_34}")
    qt_sql_4_35_non_strict "${const_sql_4_35}"
    testFoldConst("${const_sql_4_35}")
    qt_sql_4_36_non_strict "${const_sql_4_36}"
    testFoldConst("${const_sql_4_36}")
    qt_sql_4_37_non_strict "${const_sql_4_37}"
    testFoldConst("${const_sql_4_37}")
    qt_sql_4_38_non_strict "${const_sql_4_38}"
    testFoldConst("${const_sql_4_38}")
    qt_sql_4_39_non_strict "${const_sql_4_39}"
    testFoldConst("${const_sql_4_39}")
    qt_sql_4_40_non_strict "${const_sql_4_40}"
    testFoldConst("${const_sql_4_40}")
    qt_sql_4_41_non_strict "${const_sql_4_41}"
    testFoldConst("${const_sql_4_41}")
    qt_sql_4_42_non_strict "${const_sql_4_42}"
    testFoldConst("${const_sql_4_42}")
    qt_sql_4_43_non_strict "${const_sql_4_43}"
    testFoldConst("${const_sql_4_43}")
    qt_sql_4_44_non_strict "${const_sql_4_44}"
    testFoldConst("${const_sql_4_44}")
    qt_sql_4_45_non_strict "${const_sql_4_45}"
    testFoldConst("${const_sql_4_45}")
    qt_sql_4_46_non_strict "${const_sql_4_46}"
    testFoldConst("${const_sql_4_46}")
    qt_sql_4_47_non_strict "${const_sql_4_47}"
    testFoldConst("${const_sql_4_47}")
    qt_sql_4_48_non_strict "${const_sql_4_48}"
    testFoldConst("${const_sql_4_48}")
    qt_sql_4_49_non_strict "${const_sql_4_49}"
    testFoldConst("${const_sql_4_49}")
    qt_sql_4_50_non_strict "${const_sql_4_50}"
    testFoldConst("${const_sql_4_50}")
    qt_sql_4_51_non_strict "${const_sql_4_51}"
    testFoldConst("${const_sql_4_51}")
    qt_sql_4_52_non_strict "${const_sql_4_52}"
    testFoldConst("${const_sql_4_52}")
    qt_sql_4_53_non_strict "${const_sql_4_53}"
    testFoldConst("${const_sql_4_53}")
    qt_sql_4_54_non_strict "${const_sql_4_54}"
    testFoldConst("${const_sql_4_54}")
    qt_sql_4_55_non_strict "${const_sql_4_55}"
    testFoldConst("${const_sql_4_55}")
    qt_sql_4_56_non_strict "${const_sql_4_56}"
    testFoldConst("${const_sql_4_56}")
    qt_sql_4_57_non_strict "${const_sql_4_57}"
    testFoldConst("${const_sql_4_57}")
    qt_sql_4_58_non_strict "${const_sql_4_58}"
    testFoldConst("${const_sql_4_58}")
    qt_sql_4_59_non_strict "${const_sql_4_59}"
    testFoldConst("${const_sql_4_59}")
    qt_sql_4_60_non_strict "${const_sql_4_60}"
    testFoldConst("${const_sql_4_60}")
    qt_sql_4_61_non_strict "${const_sql_4_61}"
    testFoldConst("${const_sql_4_61}")
    qt_sql_4_62_non_strict "${const_sql_4_62}"
    testFoldConst("${const_sql_4_62}")
    qt_sql_4_63_non_strict "${const_sql_4_63}"
    testFoldConst("${const_sql_4_63}")
    qt_sql_4_64_non_strict "${const_sql_4_64}"
    testFoldConst("${const_sql_4_64}")
    qt_sql_4_65_non_strict "${const_sql_4_65}"
    testFoldConst("${const_sql_4_65}")
    qt_sql_4_66_non_strict "${const_sql_4_66}"
    testFoldConst("${const_sql_4_66}")
    qt_sql_4_67_non_strict "${const_sql_4_67}"
    testFoldConst("${const_sql_4_67}")
    qt_sql_4_68_non_strict "${const_sql_4_68}"
    testFoldConst("${const_sql_4_68}")
    qt_sql_4_69_non_strict "${const_sql_4_69}"
    testFoldConst("${const_sql_4_69}")
    qt_sql_4_70_non_strict "${const_sql_4_70}"
    testFoldConst("${const_sql_4_70}")
    qt_sql_4_71_non_strict "${const_sql_4_71}"
    testFoldConst("${const_sql_4_71}")
    qt_sql_4_72_non_strict "${const_sql_4_72}"
    testFoldConst("${const_sql_4_72}")
    qt_sql_4_73_non_strict "${const_sql_4_73}"
    testFoldConst("${const_sql_4_73}")
    qt_sql_4_74_non_strict "${const_sql_4_74}"
    testFoldConst("${const_sql_4_74}")
    qt_sql_4_75_non_strict "${const_sql_4_75}"
    testFoldConst("${const_sql_4_75}")
    qt_sql_4_76_non_strict "${const_sql_4_76}"
    testFoldConst("${const_sql_4_76}")
    qt_sql_4_77_non_strict "${const_sql_4_77}"
    testFoldConst("${const_sql_4_77}")
    qt_sql_4_78_non_strict "${const_sql_4_78}"
    testFoldConst("${const_sql_4_78}")
    qt_sql_4_79_non_strict "${const_sql_4_79}"
    testFoldConst("${const_sql_4_79}")
    qt_sql_4_80_non_strict "${const_sql_4_80}"
    testFoldConst("${const_sql_4_80}")
    qt_sql_4_81_non_strict "${const_sql_4_81}"
    testFoldConst("${const_sql_4_81}")
    qt_sql_4_82_non_strict "${const_sql_4_82}"
    testFoldConst("${const_sql_4_82}")
    qt_sql_4_83_non_strict "${const_sql_4_83}"
    testFoldConst("${const_sql_4_83}")
    qt_sql_4_84_non_strict "${const_sql_4_84}"
    testFoldConst("${const_sql_4_84}")
    qt_sql_4_85_non_strict "${const_sql_4_85}"
    testFoldConst("${const_sql_4_85}")
    qt_sql_4_86_non_strict "${const_sql_4_86}"
    testFoldConst("${const_sql_4_86}")
    qt_sql_4_87_non_strict "${const_sql_4_87}"
    testFoldConst("${const_sql_4_87}")
    qt_sql_4_88_non_strict "${const_sql_4_88}"
    testFoldConst("${const_sql_4_88}")
    qt_sql_4_89_non_strict "${const_sql_4_89}"
    testFoldConst("${const_sql_4_89}")
    qt_sql_4_90_non_strict "${const_sql_4_90}"
    testFoldConst("${const_sql_4_90}")
    qt_sql_4_91_non_strict "${const_sql_4_91}"
    testFoldConst("${const_sql_4_91}")
    qt_sql_4_92_non_strict "${const_sql_4_92}"
    testFoldConst("${const_sql_4_92}")
    qt_sql_4_93_non_strict "${const_sql_4_93}"
    testFoldConst("${const_sql_4_93}")
    qt_sql_4_94_non_strict "${const_sql_4_94}"
    testFoldConst("${const_sql_4_94}")
    qt_sql_4_95_non_strict "${const_sql_4_95}"
    testFoldConst("${const_sql_4_95}")
    qt_sql_4_96_non_strict "${const_sql_4_96}"
    testFoldConst("${const_sql_4_96}")
    qt_sql_4_97_non_strict "${const_sql_4_97}"
    testFoldConst("${const_sql_4_97}")
    qt_sql_4_98_non_strict "${const_sql_4_98}"
    testFoldConst("${const_sql_4_98}")
    qt_sql_4_99_non_strict "${const_sql_4_99}"
    testFoldConst("${const_sql_4_99}")
    qt_sql_4_100_non_strict "${const_sql_4_100}"
    testFoldConst("${const_sql_4_100}")
    qt_sql_4_101_non_strict "${const_sql_4_101}"
    testFoldConst("${const_sql_4_101}")
    qt_sql_4_102_non_strict "${const_sql_4_102}"
    testFoldConst("${const_sql_4_102}")
    qt_sql_4_103_non_strict "${const_sql_4_103}"
    testFoldConst("${const_sql_4_103}")
    qt_sql_4_104_non_strict "${const_sql_4_104}"
    testFoldConst("${const_sql_4_104}")
    qt_sql_4_105_non_strict "${const_sql_4_105}"
    testFoldConst("${const_sql_4_105}")
    qt_sql_4_106_non_strict "${const_sql_4_106}"
    testFoldConst("${const_sql_4_106}")
    qt_sql_4_107_non_strict "${const_sql_4_107}"
    testFoldConst("${const_sql_4_107}")
    qt_sql_4_108_non_strict "${const_sql_4_108}"
    testFoldConst("${const_sql_4_108}")
    qt_sql_4_109_non_strict "${const_sql_4_109}"
    testFoldConst("${const_sql_4_109}")
    qt_sql_4_110_non_strict "${const_sql_4_110}"
    testFoldConst("${const_sql_4_110}")
    qt_sql_4_111_non_strict "${const_sql_4_111}"
    testFoldConst("${const_sql_4_111}")
    qt_sql_4_112_non_strict "${const_sql_4_112}"
    testFoldConst("${const_sql_4_112}")
    qt_sql_4_113_non_strict "${const_sql_4_113}"
    testFoldConst("${const_sql_4_113}")
    qt_sql_4_114_non_strict "${const_sql_4_114}"
    testFoldConst("${const_sql_4_114}")
    qt_sql_4_115_non_strict "${const_sql_4_115}"
    testFoldConst("${const_sql_4_115}")
    qt_sql_4_116_non_strict "${const_sql_4_116}"
    testFoldConst("${const_sql_4_116}")
    qt_sql_4_117_non_strict "${const_sql_4_117}"
    testFoldConst("${const_sql_4_117}")
    qt_sql_4_118_non_strict "${const_sql_4_118}"
    testFoldConst("${const_sql_4_118}")
    qt_sql_4_119_non_strict "${const_sql_4_119}"
    testFoldConst("${const_sql_4_119}")
    qt_sql_4_120_non_strict "${const_sql_4_120}"
    testFoldConst("${const_sql_4_120}")
    qt_sql_4_121_non_strict "${const_sql_4_121}"
    testFoldConst("${const_sql_4_121}")
    qt_sql_4_122_non_strict "${const_sql_4_122}"
    testFoldConst("${const_sql_4_122}")
    qt_sql_4_123_non_strict "${const_sql_4_123}"
    testFoldConst("${const_sql_4_123}")
    qt_sql_4_124_non_strict "${const_sql_4_124}"
    testFoldConst("${const_sql_4_124}")
    qt_sql_4_125_non_strict "${const_sql_4_125}"
    testFoldConst("${const_sql_4_125}")
    qt_sql_4_126_non_strict "${const_sql_4_126}"
    testFoldConst("${const_sql_4_126}")
    qt_sql_4_127_non_strict "${const_sql_4_127}"
    testFoldConst("${const_sql_4_127}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.00000000", cast(cast("0.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.00000001", cast(cast("0.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "-0.00000001", cast(cast("-0.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0.00000009", cast(cast("0.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "-0.00000009", cast(cast("-0.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "0.09999999", cast(cast("0.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "-0.09999999", cast(cast("-0.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "0.90000000", cast(cast("0.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    def const_sql_5_9 = """select "-0.90000000", cast(cast("-0.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_9_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    def const_sql_5_10 = """select "0.90000001", cast(cast("0.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_10_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    def const_sql_5_11 = """select "-0.90000001", cast(cast("-0.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_11_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    def const_sql_5_12 = """select "0.99999998", cast(cast("0.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_12_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    def const_sql_5_13 = """select "-0.99999998", cast(cast("-0.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_13_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    def const_sql_5_14 = """select "0.99999999", cast(cast("0.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_14_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    def const_sql_5_15 = """select "-0.99999999", cast(cast("-0.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_15_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    def const_sql_5_16 = """select "1.00000000", cast(cast("1.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_16_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    def const_sql_5_17 = """select "-1.00000000", cast(cast("-1.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_17_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    def const_sql_5_18 = """select "1.00000001", cast(cast("1.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_18_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    def const_sql_5_19 = """select "-1.00000001", cast(cast("-1.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_19_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    def const_sql_5_20 = """select "1.00000009", cast(cast("1.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_20_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    def const_sql_5_21 = """select "-1.00000009", cast(cast("-1.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_21_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    def const_sql_5_22 = """select "1.09999999", cast(cast("1.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_22_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    def const_sql_5_23 = """select "-1.09999999", cast(cast("-1.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_23_strict "${const_sql_5_23}"
    testFoldConst("${const_sql_5_23}")
    def const_sql_5_24 = """select "1.90000000", cast(cast("1.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_24_strict "${const_sql_5_24}"
    testFoldConst("${const_sql_5_24}")
    def const_sql_5_25 = """select "-1.90000000", cast(cast("-1.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_25_strict "${const_sql_5_25}"
    testFoldConst("${const_sql_5_25}")
    def const_sql_5_26 = """select "1.90000001", cast(cast("1.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_26_strict "${const_sql_5_26}"
    testFoldConst("${const_sql_5_26}")
    def const_sql_5_27 = """select "-1.90000001", cast(cast("-1.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_27_strict "${const_sql_5_27}"
    testFoldConst("${const_sql_5_27}")
    def const_sql_5_28 = """select "1.99999998", cast(cast("1.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_28_strict "${const_sql_5_28}"
    testFoldConst("${const_sql_5_28}")
    def const_sql_5_29 = """select "-1.99999998", cast(cast("-1.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_29_strict "${const_sql_5_29}"
    testFoldConst("${const_sql_5_29}")
    def const_sql_5_30 = """select "1.99999999", cast(cast("1.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_30_strict "${const_sql_5_30}"
    testFoldConst("${const_sql_5_30}")
    def const_sql_5_31 = """select "-1.99999999", cast(cast("-1.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_31_strict "${const_sql_5_31}"
    testFoldConst("${const_sql_5_31}")
    def const_sql_5_32 = """select "8.00000000", cast(cast("8.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_32_strict "${const_sql_5_32}"
    testFoldConst("${const_sql_5_32}")
    def const_sql_5_33 = """select "-8.00000000", cast(cast("-8.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_33_strict "${const_sql_5_33}"
    testFoldConst("${const_sql_5_33}")
    def const_sql_5_34 = """select "8.00000001", cast(cast("8.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_34_strict "${const_sql_5_34}"
    testFoldConst("${const_sql_5_34}")
    def const_sql_5_35 = """select "-8.00000001", cast(cast("-8.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_35_strict "${const_sql_5_35}"
    testFoldConst("${const_sql_5_35}")
    def const_sql_5_36 = """select "8.00000009", cast(cast("8.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_36_strict "${const_sql_5_36}"
    testFoldConst("${const_sql_5_36}")
    def const_sql_5_37 = """select "-8.00000009", cast(cast("-8.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_37_strict "${const_sql_5_37}"
    testFoldConst("${const_sql_5_37}")
    def const_sql_5_38 = """select "8.09999999", cast(cast("8.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_38_strict "${const_sql_5_38}"
    testFoldConst("${const_sql_5_38}")
    def const_sql_5_39 = """select "-8.09999999", cast(cast("-8.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_39_strict "${const_sql_5_39}"
    testFoldConst("${const_sql_5_39}")
    def const_sql_5_40 = """select "8.90000000", cast(cast("8.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_40_strict "${const_sql_5_40}"
    testFoldConst("${const_sql_5_40}")
    def const_sql_5_41 = """select "-8.90000000", cast(cast("-8.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_41_strict "${const_sql_5_41}"
    testFoldConst("${const_sql_5_41}")
    def const_sql_5_42 = """select "8.90000001", cast(cast("8.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_42_strict "${const_sql_5_42}"
    testFoldConst("${const_sql_5_42}")
    def const_sql_5_43 = """select "-8.90000001", cast(cast("-8.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_43_strict "${const_sql_5_43}"
    testFoldConst("${const_sql_5_43}")
    def const_sql_5_44 = """select "8.99999998", cast(cast("8.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_44_strict "${const_sql_5_44}"
    testFoldConst("${const_sql_5_44}")
    def const_sql_5_45 = """select "-8.99999998", cast(cast("-8.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_45_strict "${const_sql_5_45}"
    testFoldConst("${const_sql_5_45}")
    def const_sql_5_46 = """select "8.99999999", cast(cast("8.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_46_strict "${const_sql_5_46}"
    testFoldConst("${const_sql_5_46}")
    def const_sql_5_47 = """select "-8.99999999", cast(cast("-8.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_47_strict "${const_sql_5_47}"
    testFoldConst("${const_sql_5_47}")
    def const_sql_5_48 = """select "9.00000000", cast(cast("9.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_48_strict "${const_sql_5_48}"
    testFoldConst("${const_sql_5_48}")
    def const_sql_5_49 = """select "-9.00000000", cast(cast("-9.00000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_49_strict "${const_sql_5_49}"
    testFoldConst("${const_sql_5_49}")
    def const_sql_5_50 = """select "9.00000001", cast(cast("9.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_50_strict "${const_sql_5_50}"
    testFoldConst("${const_sql_5_50}")
    def const_sql_5_51 = """select "-9.00000001", cast(cast("-9.00000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_51_strict "${const_sql_5_51}"
    testFoldConst("${const_sql_5_51}")
    def const_sql_5_52 = """select "9.00000009", cast(cast("9.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_52_strict "${const_sql_5_52}"
    testFoldConst("${const_sql_5_52}")
    def const_sql_5_53 = """select "-9.00000009", cast(cast("-9.00000009" as decimalv3(9, 8)) as double);"""
    qt_sql_5_53_strict "${const_sql_5_53}"
    testFoldConst("${const_sql_5_53}")
    def const_sql_5_54 = """select "9.09999999", cast(cast("9.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_54_strict "${const_sql_5_54}"
    testFoldConst("${const_sql_5_54}")
    def const_sql_5_55 = """select "-9.09999999", cast(cast("-9.09999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_55_strict "${const_sql_5_55}"
    testFoldConst("${const_sql_5_55}")
    def const_sql_5_56 = """select "9.90000000", cast(cast("9.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_56_strict "${const_sql_5_56}"
    testFoldConst("${const_sql_5_56}")
    def const_sql_5_57 = """select "-9.90000000", cast(cast("-9.90000000" as decimalv3(9, 8)) as double);"""
    qt_sql_5_57_strict "${const_sql_5_57}"
    testFoldConst("${const_sql_5_57}")
    def const_sql_5_58 = """select "9.90000001", cast(cast("9.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_58_strict "${const_sql_5_58}"
    testFoldConst("${const_sql_5_58}")
    def const_sql_5_59 = """select "-9.90000001", cast(cast("-9.90000001" as decimalv3(9, 8)) as double);"""
    qt_sql_5_59_strict "${const_sql_5_59}"
    testFoldConst("${const_sql_5_59}")
    def const_sql_5_60 = """select "9.99999998", cast(cast("9.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_60_strict "${const_sql_5_60}"
    testFoldConst("${const_sql_5_60}")
    def const_sql_5_61 = """select "-9.99999998", cast(cast("-9.99999998" as decimalv3(9, 8)) as double);"""
    qt_sql_5_61_strict "${const_sql_5_61}"
    testFoldConst("${const_sql_5_61}")
    def const_sql_5_62 = """select "9.99999999", cast(cast("9.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_62_strict "${const_sql_5_62}"
    testFoldConst("${const_sql_5_62}")
    def const_sql_5_63 = """select "-9.99999999", cast(cast("-9.99999999" as decimalv3(9, 8)) as double);"""
    qt_sql_5_63_strict "${const_sql_5_63}"
    testFoldConst("${const_sql_5_63}")

    sql "set enable_strict_cast=false;"
    qt_sql_5_0_non_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    qt_sql_5_1_non_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    qt_sql_5_2_non_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    qt_sql_5_3_non_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    qt_sql_5_4_non_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    qt_sql_5_5_non_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    qt_sql_5_6_non_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    qt_sql_5_7_non_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    qt_sql_5_8_non_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    qt_sql_5_9_non_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    qt_sql_5_10_non_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    qt_sql_5_11_non_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    qt_sql_5_12_non_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    qt_sql_5_13_non_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    qt_sql_5_14_non_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    qt_sql_5_15_non_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    qt_sql_5_16_non_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    qt_sql_5_17_non_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    qt_sql_5_18_non_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    qt_sql_5_19_non_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    qt_sql_5_20_non_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    qt_sql_5_21_non_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    qt_sql_5_22_non_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    qt_sql_5_23_non_strict "${const_sql_5_23}"
    testFoldConst("${const_sql_5_23}")
    qt_sql_5_24_non_strict "${const_sql_5_24}"
    testFoldConst("${const_sql_5_24}")
    qt_sql_5_25_non_strict "${const_sql_5_25}"
    testFoldConst("${const_sql_5_25}")
    qt_sql_5_26_non_strict "${const_sql_5_26}"
    testFoldConst("${const_sql_5_26}")
    qt_sql_5_27_non_strict "${const_sql_5_27}"
    testFoldConst("${const_sql_5_27}")
    qt_sql_5_28_non_strict "${const_sql_5_28}"
    testFoldConst("${const_sql_5_28}")
    qt_sql_5_29_non_strict "${const_sql_5_29}"
    testFoldConst("${const_sql_5_29}")
    qt_sql_5_30_non_strict "${const_sql_5_30}"
    testFoldConst("${const_sql_5_30}")
    qt_sql_5_31_non_strict "${const_sql_5_31}"
    testFoldConst("${const_sql_5_31}")
    qt_sql_5_32_non_strict "${const_sql_5_32}"
    testFoldConst("${const_sql_5_32}")
    qt_sql_5_33_non_strict "${const_sql_5_33}"
    testFoldConst("${const_sql_5_33}")
    qt_sql_5_34_non_strict "${const_sql_5_34}"
    testFoldConst("${const_sql_5_34}")
    qt_sql_5_35_non_strict "${const_sql_5_35}"
    testFoldConst("${const_sql_5_35}")
    qt_sql_5_36_non_strict "${const_sql_5_36}"
    testFoldConst("${const_sql_5_36}")
    qt_sql_5_37_non_strict "${const_sql_5_37}"
    testFoldConst("${const_sql_5_37}")
    qt_sql_5_38_non_strict "${const_sql_5_38}"
    testFoldConst("${const_sql_5_38}")
    qt_sql_5_39_non_strict "${const_sql_5_39}"
    testFoldConst("${const_sql_5_39}")
    qt_sql_5_40_non_strict "${const_sql_5_40}"
    testFoldConst("${const_sql_5_40}")
    qt_sql_5_41_non_strict "${const_sql_5_41}"
    testFoldConst("${const_sql_5_41}")
    qt_sql_5_42_non_strict "${const_sql_5_42}"
    testFoldConst("${const_sql_5_42}")
    qt_sql_5_43_non_strict "${const_sql_5_43}"
    testFoldConst("${const_sql_5_43}")
    qt_sql_5_44_non_strict "${const_sql_5_44}"
    testFoldConst("${const_sql_5_44}")
    qt_sql_5_45_non_strict "${const_sql_5_45}"
    testFoldConst("${const_sql_5_45}")
    qt_sql_5_46_non_strict "${const_sql_5_46}"
    testFoldConst("${const_sql_5_46}")
    qt_sql_5_47_non_strict "${const_sql_5_47}"
    testFoldConst("${const_sql_5_47}")
    qt_sql_5_48_non_strict "${const_sql_5_48}"
    testFoldConst("${const_sql_5_48}")
    qt_sql_5_49_non_strict "${const_sql_5_49}"
    testFoldConst("${const_sql_5_49}")
    qt_sql_5_50_non_strict "${const_sql_5_50}"
    testFoldConst("${const_sql_5_50}")
    qt_sql_5_51_non_strict "${const_sql_5_51}"
    testFoldConst("${const_sql_5_51}")
    qt_sql_5_52_non_strict "${const_sql_5_52}"
    testFoldConst("${const_sql_5_52}")
    qt_sql_5_53_non_strict "${const_sql_5_53}"
    testFoldConst("${const_sql_5_53}")
    qt_sql_5_54_non_strict "${const_sql_5_54}"
    testFoldConst("${const_sql_5_54}")
    qt_sql_5_55_non_strict "${const_sql_5_55}"
    testFoldConst("${const_sql_5_55}")
    qt_sql_5_56_non_strict "${const_sql_5_56}"
    testFoldConst("${const_sql_5_56}")
    qt_sql_5_57_non_strict "${const_sql_5_57}"
    testFoldConst("${const_sql_5_57}")
    qt_sql_5_58_non_strict "${const_sql_5_58}"
    testFoldConst("${const_sql_5_58}")
    qt_sql_5_59_non_strict "${const_sql_5_59}"
    testFoldConst("${const_sql_5_59}")
    qt_sql_5_60_non_strict "${const_sql_5_60}"
    testFoldConst("${const_sql_5_60}")
    qt_sql_5_61_non_strict "${const_sql_5_61}"
    testFoldConst("${const_sql_5_61}")
    qt_sql_5_62_non_strict "${const_sql_5_62}"
    testFoldConst("${const_sql_5_62}")
    qt_sql_5_63_non_strict "${const_sql_5_63}"
    testFoldConst("${const_sql_5_63}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(9, 9)) as double);"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.000000000", cast(cast("0.000000000" as decimalv3(9, 9)) as double);"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.000000001", cast(cast("0.000000001" as decimalv3(9, 9)) as double);"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "-0.000000001", cast(cast("-0.000000001" as decimalv3(9, 9)) as double);"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.000000009", cast(cast("0.000000009" as decimalv3(9, 9)) as double);"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "-0.000000009", cast(cast("-0.000000009" as decimalv3(9, 9)) as double);"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.099999999", cast(cast("0.099999999" as decimalv3(9, 9)) as double);"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "-0.099999999", cast(cast("-0.099999999" as decimalv3(9, 9)) as double);"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "0.900000000", cast(cast("0.900000000" as decimalv3(9, 9)) as double);"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "-0.900000000", cast(cast("-0.900000000" as decimalv3(9, 9)) as double);"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "0.900000001", cast(cast("0.900000001" as decimalv3(9, 9)) as double);"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "-0.900000001", cast(cast("-0.900000001" as decimalv3(9, 9)) as double);"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    def const_sql_6_12 = """select "0.999999998", cast(cast("0.999999998" as decimalv3(9, 9)) as double);"""
    qt_sql_6_12_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    def const_sql_6_13 = """select "-0.999999998", cast(cast("-0.999999998" as decimalv3(9, 9)) as double);"""
    qt_sql_6_13_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    def const_sql_6_14 = """select "0.999999999", cast(cast("0.999999999" as decimalv3(9, 9)) as double);"""
    qt_sql_6_14_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    def const_sql_6_15 = """select "-0.999999999", cast(cast("-0.999999999" as decimalv3(9, 9)) as double);"""
    qt_sql_6_15_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")

    sql "set enable_strict_cast=false;"
    qt_sql_6_0_non_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    qt_sql_6_1_non_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    qt_sql_6_2_non_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    qt_sql_6_3_non_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    qt_sql_6_4_non_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    qt_sql_6_5_non_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    qt_sql_6_6_non_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    qt_sql_6_7_non_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    qt_sql_6_8_non_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    qt_sql_6_9_non_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    qt_sql_6_10_non_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    qt_sql_6_11_non_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    qt_sql_6_12_non_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    qt_sql_6_13_non_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    qt_sql_6_14_non_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    qt_sql_6_15_non_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
}