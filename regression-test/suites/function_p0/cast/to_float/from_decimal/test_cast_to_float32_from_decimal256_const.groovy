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


suite("test_cast_to_float32_from_decimal256_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set enable_decimal256 = true;"
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0", cast(cast("0" as decimalv3(39, 0)) as float);"""
    qt_sql_0_0_strict "${const_sql_0_0}"
    testFoldConst("${const_sql_0_0}")
    def const_sql_0_1 = """select "0", cast(cast("0" as decimalv3(39, 0)) as float);"""
    qt_sql_0_1_strict "${const_sql_0_1}"
    testFoldConst("${const_sql_0_1}")
    def const_sql_0_2 = """select "1", cast(cast("1" as decimalv3(39, 0)) as float);"""
    qt_sql_0_2_strict "${const_sql_0_2}"
    testFoldConst("${const_sql_0_2}")
    def const_sql_0_3 = """select "-1", cast(cast("-1" as decimalv3(39, 0)) as float);"""
    qt_sql_0_3_strict "${const_sql_0_3}"
    testFoldConst("${const_sql_0_3}")
    def const_sql_0_4 = """select "9", cast(cast("9" as decimalv3(39, 0)) as float);"""
    qt_sql_0_4_strict "${const_sql_0_4}"
    testFoldConst("${const_sql_0_4}")
    def const_sql_0_5 = """select "-9", cast(cast("-9" as decimalv3(39, 0)) as float);"""
    qt_sql_0_5_strict "${const_sql_0_5}"
    testFoldConst("${const_sql_0_5}")
    def const_sql_0_6 = """select "99999999999999999999999999999999999999", cast(cast("99999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_6_strict "${const_sql_0_6}"
    testFoldConst("${const_sql_0_6}")
    def const_sql_0_7 = """select "-99999999999999999999999999999999999999", cast(cast("-99999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_7_strict "${const_sql_0_7}"
    testFoldConst("${const_sql_0_7}")
    def const_sql_0_8 = """select "900000000000000000000000000000000000000", cast(cast("900000000000000000000000000000000000000" as decimalv3(39, 0)) as float);"""
    qt_sql_0_8_strict "${const_sql_0_8}"
    testFoldConst("${const_sql_0_8}")
    def const_sql_0_9 = """select "-900000000000000000000000000000000000000", cast(cast("-900000000000000000000000000000000000000" as decimalv3(39, 0)) as float);"""
    qt_sql_0_9_strict "${const_sql_0_9}"
    testFoldConst("${const_sql_0_9}")
    def const_sql_0_10 = """select "900000000000000000000000000000000000001", cast(cast("900000000000000000000000000000000000001" as decimalv3(39, 0)) as float);"""
    qt_sql_0_10_strict "${const_sql_0_10}"
    testFoldConst("${const_sql_0_10}")
    def const_sql_0_11 = """select "-900000000000000000000000000000000000001", cast(cast("-900000000000000000000000000000000000001" as decimalv3(39, 0)) as float);"""
    qt_sql_0_11_strict "${const_sql_0_11}"
    testFoldConst("${const_sql_0_11}")
    def const_sql_0_12 = """select "999999999999999999999999999999999999998", cast(cast("999999999999999999999999999999999999998" as decimalv3(39, 0)) as float);"""
    qt_sql_0_12_strict "${const_sql_0_12}"
    testFoldConst("${const_sql_0_12}")
    def const_sql_0_13 = """select "-999999999999999999999999999999999999998", cast(cast("-999999999999999999999999999999999999998" as decimalv3(39, 0)) as float);"""
    qt_sql_0_13_strict "${const_sql_0_13}"
    testFoldConst("${const_sql_0_13}")
    def const_sql_0_14 = """select "999999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_14_strict "${const_sql_0_14}"
    testFoldConst("${const_sql_0_14}")
    def const_sql_0_15 = """select "-999999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999" as decimalv3(39, 0)) as float);"""
    qt_sql_0_15_strict "${const_sql_0_15}"
    testFoldConst("${const_sql_0_15}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0.0000000000000000000", cast(cast("0.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0.0000000000000000000", cast(cast("0.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0.0000000000000000001", cast(cast("0.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "-0.0000000000000000001", cast(cast("-0.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "0.0000000000000000009", cast(cast("0.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "-0.0000000000000000009", cast(cast("-0.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "0.0999999999999999999", cast(cast("0.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "-0.0999999999999999999", cast(cast("-0.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "0.9000000000000000000", cast(cast("0.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select "-0.9000000000000000000", cast(cast("-0.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "0.9000000000000000001", cast(cast("0.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "-0.9000000000000000001", cast(cast("-0.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    def const_sql_1_12 = """select "0.9999999999999999998", cast(cast("0.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_12_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    def const_sql_1_13 = """select "-0.9999999999999999998", cast(cast("-0.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_13_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    def const_sql_1_14 = """select "0.9999999999999999999", cast(cast("0.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_14_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    def const_sql_1_15 = """select "-0.9999999999999999999", cast(cast("-0.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_15_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    def const_sql_1_16 = """select "1.0000000000000000000", cast(cast("1.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_16_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    def const_sql_1_17 = """select "-1.0000000000000000000", cast(cast("-1.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_17_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    def const_sql_1_18 = """select "1.0000000000000000001", cast(cast("1.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_18_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    def const_sql_1_19 = """select "-1.0000000000000000001", cast(cast("-1.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_19_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    def const_sql_1_20 = """select "1.0000000000000000009", cast(cast("1.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_20_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    def const_sql_1_21 = """select "-1.0000000000000000009", cast(cast("-1.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_21_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    def const_sql_1_22 = """select "1.0999999999999999999", cast(cast("1.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_22_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    def const_sql_1_23 = """select "-1.0999999999999999999", cast(cast("-1.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_23_strict "${const_sql_1_23}"
    testFoldConst("${const_sql_1_23}")
    def const_sql_1_24 = """select "1.9000000000000000000", cast(cast("1.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_24_strict "${const_sql_1_24}"
    testFoldConst("${const_sql_1_24}")
    def const_sql_1_25 = """select "-1.9000000000000000000", cast(cast("-1.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_25_strict "${const_sql_1_25}"
    testFoldConst("${const_sql_1_25}")
    def const_sql_1_26 = """select "1.9000000000000000001", cast(cast("1.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_26_strict "${const_sql_1_26}"
    testFoldConst("${const_sql_1_26}")
    def const_sql_1_27 = """select "-1.9000000000000000001", cast(cast("-1.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_27_strict "${const_sql_1_27}"
    testFoldConst("${const_sql_1_27}")
    def const_sql_1_28 = """select "1.9999999999999999998", cast(cast("1.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_28_strict "${const_sql_1_28}"
    testFoldConst("${const_sql_1_28}")
    def const_sql_1_29 = """select "-1.9999999999999999998", cast(cast("-1.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_29_strict "${const_sql_1_29}"
    testFoldConst("${const_sql_1_29}")
    def const_sql_1_30 = """select "1.9999999999999999999", cast(cast("1.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_30_strict "${const_sql_1_30}"
    testFoldConst("${const_sql_1_30}")
    def const_sql_1_31 = """select "-1.9999999999999999999", cast(cast("-1.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_31_strict "${const_sql_1_31}"
    testFoldConst("${const_sql_1_31}")
    def const_sql_1_32 = """select "9.0000000000000000000", cast(cast("9.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_32_strict "${const_sql_1_32}"
    testFoldConst("${const_sql_1_32}")
    def const_sql_1_33 = """select "-9.0000000000000000000", cast(cast("-9.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_33_strict "${const_sql_1_33}"
    testFoldConst("${const_sql_1_33}")
    def const_sql_1_34 = """select "9.0000000000000000001", cast(cast("9.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_34_strict "${const_sql_1_34}"
    testFoldConst("${const_sql_1_34}")
    def const_sql_1_35 = """select "-9.0000000000000000001", cast(cast("-9.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_35_strict "${const_sql_1_35}"
    testFoldConst("${const_sql_1_35}")
    def const_sql_1_36 = """select "9.0000000000000000009", cast(cast("9.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_36_strict "${const_sql_1_36}"
    testFoldConst("${const_sql_1_36}")
    def const_sql_1_37 = """select "-9.0000000000000000009", cast(cast("-9.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_37_strict "${const_sql_1_37}"
    testFoldConst("${const_sql_1_37}")
    def const_sql_1_38 = """select "9.0999999999999999999", cast(cast("9.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_38_strict "${const_sql_1_38}"
    testFoldConst("${const_sql_1_38}")
    def const_sql_1_39 = """select "-9.0999999999999999999", cast(cast("-9.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_39_strict "${const_sql_1_39}"
    testFoldConst("${const_sql_1_39}")
    def const_sql_1_40 = """select "9.9000000000000000000", cast(cast("9.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_40_strict "${const_sql_1_40}"
    testFoldConst("${const_sql_1_40}")
    def const_sql_1_41 = """select "-9.9000000000000000000", cast(cast("-9.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_41_strict "${const_sql_1_41}"
    testFoldConst("${const_sql_1_41}")
    def const_sql_1_42 = """select "9.9000000000000000001", cast(cast("9.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_42_strict "${const_sql_1_42}"
    testFoldConst("${const_sql_1_42}")
    def const_sql_1_43 = """select "-9.9000000000000000001", cast(cast("-9.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_43_strict "${const_sql_1_43}"
    testFoldConst("${const_sql_1_43}")
    def const_sql_1_44 = """select "9.9999999999999999998", cast(cast("9.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_44_strict "${const_sql_1_44}"
    testFoldConst("${const_sql_1_44}")
    def const_sql_1_45 = """select "-9.9999999999999999998", cast(cast("-9.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_45_strict "${const_sql_1_45}"
    testFoldConst("${const_sql_1_45}")
    def const_sql_1_46 = """select "9.9999999999999999999", cast(cast("9.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_46_strict "${const_sql_1_46}"
    testFoldConst("${const_sql_1_46}")
    def const_sql_1_47 = """select "-9.9999999999999999999", cast(cast("-9.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_47_strict "${const_sql_1_47}"
    testFoldConst("${const_sql_1_47}")
    def const_sql_1_48 = """select "9999999999999999999.0000000000000000000", cast(cast("9999999999999999999.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_48_strict "${const_sql_1_48}"
    testFoldConst("${const_sql_1_48}")
    def const_sql_1_49 = """select "-9999999999999999999.0000000000000000000", cast(cast("-9999999999999999999.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_49_strict "${const_sql_1_49}"
    testFoldConst("${const_sql_1_49}")
    def const_sql_1_50 = """select "9999999999999999999.0000000000000000001", cast(cast("9999999999999999999.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_50_strict "${const_sql_1_50}"
    testFoldConst("${const_sql_1_50}")
    def const_sql_1_51 = """select "-9999999999999999999.0000000000000000001", cast(cast("-9999999999999999999.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_51_strict "${const_sql_1_51}"
    testFoldConst("${const_sql_1_51}")
    def const_sql_1_52 = """select "9999999999999999999.0000000000000000009", cast(cast("9999999999999999999.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_52_strict "${const_sql_1_52}"
    testFoldConst("${const_sql_1_52}")
    def const_sql_1_53 = """select "-9999999999999999999.0000000000000000009", cast(cast("-9999999999999999999.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_53_strict "${const_sql_1_53}"
    testFoldConst("${const_sql_1_53}")
    def const_sql_1_54 = """select "9999999999999999999.0999999999999999999", cast(cast("9999999999999999999.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_54_strict "${const_sql_1_54}"
    testFoldConst("${const_sql_1_54}")
    def const_sql_1_55 = """select "-9999999999999999999.0999999999999999999", cast(cast("-9999999999999999999.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_55_strict "${const_sql_1_55}"
    testFoldConst("${const_sql_1_55}")
    def const_sql_1_56 = """select "9999999999999999999.9000000000000000000", cast(cast("9999999999999999999.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_56_strict "${const_sql_1_56}"
    testFoldConst("${const_sql_1_56}")
    def const_sql_1_57 = """select "-9999999999999999999.9000000000000000000", cast(cast("-9999999999999999999.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_57_strict "${const_sql_1_57}"
    testFoldConst("${const_sql_1_57}")
    def const_sql_1_58 = """select "9999999999999999999.9000000000000000001", cast(cast("9999999999999999999.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_58_strict "${const_sql_1_58}"
    testFoldConst("${const_sql_1_58}")
    def const_sql_1_59 = """select "-9999999999999999999.9000000000000000001", cast(cast("-9999999999999999999.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_59_strict "${const_sql_1_59}"
    testFoldConst("${const_sql_1_59}")
    def const_sql_1_60 = """select "9999999999999999999.9999999999999999998", cast(cast("9999999999999999999.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_60_strict "${const_sql_1_60}"
    testFoldConst("${const_sql_1_60}")
    def const_sql_1_61 = """select "-9999999999999999999.9999999999999999998", cast(cast("-9999999999999999999.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_61_strict "${const_sql_1_61}"
    testFoldConst("${const_sql_1_61}")
    def const_sql_1_62 = """select "9999999999999999999.9999999999999999999", cast(cast("9999999999999999999.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_62_strict "${const_sql_1_62}"
    testFoldConst("${const_sql_1_62}")
    def const_sql_1_63 = """select "-9999999999999999999.9999999999999999999", cast(cast("-9999999999999999999.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_63_strict "${const_sql_1_63}"
    testFoldConst("${const_sql_1_63}")
    def const_sql_1_64 = """select "90000000000000000000.0000000000000000000", cast(cast("90000000000000000000.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_64_strict "${const_sql_1_64}"
    testFoldConst("${const_sql_1_64}")
    def const_sql_1_65 = """select "-90000000000000000000.0000000000000000000", cast(cast("-90000000000000000000.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_65_strict "${const_sql_1_65}"
    testFoldConst("${const_sql_1_65}")
    def const_sql_1_66 = """select "90000000000000000000.0000000000000000001", cast(cast("90000000000000000000.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_66_strict "${const_sql_1_66}"
    testFoldConst("${const_sql_1_66}")
    def const_sql_1_67 = """select "-90000000000000000000.0000000000000000001", cast(cast("-90000000000000000000.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_67_strict "${const_sql_1_67}"
    testFoldConst("${const_sql_1_67}")
    def const_sql_1_68 = """select "90000000000000000000.0000000000000000009", cast(cast("90000000000000000000.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_68_strict "${const_sql_1_68}"
    testFoldConst("${const_sql_1_68}")
    def const_sql_1_69 = """select "-90000000000000000000.0000000000000000009", cast(cast("-90000000000000000000.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_69_strict "${const_sql_1_69}"
    testFoldConst("${const_sql_1_69}")
    def const_sql_1_70 = """select "90000000000000000000.0999999999999999999", cast(cast("90000000000000000000.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_70_strict "${const_sql_1_70}"
    testFoldConst("${const_sql_1_70}")
    def const_sql_1_71 = """select "-90000000000000000000.0999999999999999999", cast(cast("-90000000000000000000.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_71_strict "${const_sql_1_71}"
    testFoldConst("${const_sql_1_71}")
    def const_sql_1_72 = """select "90000000000000000000.9000000000000000000", cast(cast("90000000000000000000.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_72_strict "${const_sql_1_72}"
    testFoldConst("${const_sql_1_72}")
    def const_sql_1_73 = """select "-90000000000000000000.9000000000000000000", cast(cast("-90000000000000000000.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_73_strict "${const_sql_1_73}"
    testFoldConst("${const_sql_1_73}")
    def const_sql_1_74 = """select "90000000000000000000.9000000000000000001", cast(cast("90000000000000000000.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_74_strict "${const_sql_1_74}"
    testFoldConst("${const_sql_1_74}")
    def const_sql_1_75 = """select "-90000000000000000000.9000000000000000001", cast(cast("-90000000000000000000.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_75_strict "${const_sql_1_75}"
    testFoldConst("${const_sql_1_75}")
    def const_sql_1_76 = """select "90000000000000000000.9999999999999999998", cast(cast("90000000000000000000.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_76_strict "${const_sql_1_76}"
    testFoldConst("${const_sql_1_76}")
    def const_sql_1_77 = """select "-90000000000000000000.9999999999999999998", cast(cast("-90000000000000000000.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_77_strict "${const_sql_1_77}"
    testFoldConst("${const_sql_1_77}")
    def const_sql_1_78 = """select "90000000000000000000.9999999999999999999", cast(cast("90000000000000000000.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_78_strict "${const_sql_1_78}"
    testFoldConst("${const_sql_1_78}")
    def const_sql_1_79 = """select "-90000000000000000000.9999999999999999999", cast(cast("-90000000000000000000.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_79_strict "${const_sql_1_79}"
    testFoldConst("${const_sql_1_79}")
    def const_sql_1_80 = """select "90000000000000000001.0000000000000000000", cast(cast("90000000000000000001.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_80_strict "${const_sql_1_80}"
    testFoldConst("${const_sql_1_80}")
    def const_sql_1_81 = """select "-90000000000000000001.0000000000000000000", cast(cast("-90000000000000000001.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_81_strict "${const_sql_1_81}"
    testFoldConst("${const_sql_1_81}")
    def const_sql_1_82 = """select "90000000000000000001.0000000000000000001", cast(cast("90000000000000000001.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_82_strict "${const_sql_1_82}"
    testFoldConst("${const_sql_1_82}")
    def const_sql_1_83 = """select "-90000000000000000001.0000000000000000001", cast(cast("-90000000000000000001.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_83_strict "${const_sql_1_83}"
    testFoldConst("${const_sql_1_83}")
    def const_sql_1_84 = """select "90000000000000000001.0000000000000000009", cast(cast("90000000000000000001.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_84_strict "${const_sql_1_84}"
    testFoldConst("${const_sql_1_84}")
    def const_sql_1_85 = """select "-90000000000000000001.0000000000000000009", cast(cast("-90000000000000000001.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_85_strict "${const_sql_1_85}"
    testFoldConst("${const_sql_1_85}")
    def const_sql_1_86 = """select "90000000000000000001.0999999999999999999", cast(cast("90000000000000000001.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_86_strict "${const_sql_1_86}"
    testFoldConst("${const_sql_1_86}")
    def const_sql_1_87 = """select "-90000000000000000001.0999999999999999999", cast(cast("-90000000000000000001.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_87_strict "${const_sql_1_87}"
    testFoldConst("${const_sql_1_87}")
    def const_sql_1_88 = """select "90000000000000000001.9000000000000000000", cast(cast("90000000000000000001.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_88_strict "${const_sql_1_88}"
    testFoldConst("${const_sql_1_88}")
    def const_sql_1_89 = """select "-90000000000000000001.9000000000000000000", cast(cast("-90000000000000000001.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_89_strict "${const_sql_1_89}"
    testFoldConst("${const_sql_1_89}")
    def const_sql_1_90 = """select "90000000000000000001.9000000000000000001", cast(cast("90000000000000000001.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_90_strict "${const_sql_1_90}"
    testFoldConst("${const_sql_1_90}")
    def const_sql_1_91 = """select "-90000000000000000001.9000000000000000001", cast(cast("-90000000000000000001.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_91_strict "${const_sql_1_91}"
    testFoldConst("${const_sql_1_91}")
    def const_sql_1_92 = """select "90000000000000000001.9999999999999999998", cast(cast("90000000000000000001.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_92_strict "${const_sql_1_92}"
    testFoldConst("${const_sql_1_92}")
    def const_sql_1_93 = """select "-90000000000000000001.9999999999999999998", cast(cast("-90000000000000000001.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_93_strict "${const_sql_1_93}"
    testFoldConst("${const_sql_1_93}")
    def const_sql_1_94 = """select "90000000000000000001.9999999999999999999", cast(cast("90000000000000000001.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_94_strict "${const_sql_1_94}"
    testFoldConst("${const_sql_1_94}")
    def const_sql_1_95 = """select "-90000000000000000001.9999999999999999999", cast(cast("-90000000000000000001.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_95_strict "${const_sql_1_95}"
    testFoldConst("${const_sql_1_95}")
    def const_sql_1_96 = """select "99999999999999999998.0000000000000000000", cast(cast("99999999999999999998.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_96_strict "${const_sql_1_96}"
    testFoldConst("${const_sql_1_96}")
    def const_sql_1_97 = """select "-99999999999999999998.0000000000000000000", cast(cast("-99999999999999999998.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_97_strict "${const_sql_1_97}"
    testFoldConst("${const_sql_1_97}")
    def const_sql_1_98 = """select "99999999999999999998.0000000000000000001", cast(cast("99999999999999999998.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_98_strict "${const_sql_1_98}"
    testFoldConst("${const_sql_1_98}")
    def const_sql_1_99 = """select "-99999999999999999998.0000000000000000001", cast(cast("-99999999999999999998.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_99_strict "${const_sql_1_99}"
    testFoldConst("${const_sql_1_99}")
    def const_sql_1_100 = """select "99999999999999999998.0000000000000000009", cast(cast("99999999999999999998.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_100_strict "${const_sql_1_100}"
    testFoldConst("${const_sql_1_100}")
    def const_sql_1_101 = """select "-99999999999999999998.0000000000000000009", cast(cast("-99999999999999999998.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_101_strict "${const_sql_1_101}"
    testFoldConst("${const_sql_1_101}")
    def const_sql_1_102 = """select "99999999999999999998.0999999999999999999", cast(cast("99999999999999999998.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_102_strict "${const_sql_1_102}"
    testFoldConst("${const_sql_1_102}")
    def const_sql_1_103 = """select "-99999999999999999998.0999999999999999999", cast(cast("-99999999999999999998.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_103_strict "${const_sql_1_103}"
    testFoldConst("${const_sql_1_103}")
    def const_sql_1_104 = """select "99999999999999999998.9000000000000000000", cast(cast("99999999999999999998.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_104_strict "${const_sql_1_104}"
    testFoldConst("${const_sql_1_104}")
    def const_sql_1_105 = """select "-99999999999999999998.9000000000000000000", cast(cast("-99999999999999999998.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_105_strict "${const_sql_1_105}"
    testFoldConst("${const_sql_1_105}")
    def const_sql_1_106 = """select "99999999999999999998.9000000000000000001", cast(cast("99999999999999999998.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_106_strict "${const_sql_1_106}"
    testFoldConst("${const_sql_1_106}")
    def const_sql_1_107 = """select "-99999999999999999998.9000000000000000001", cast(cast("-99999999999999999998.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_107_strict "${const_sql_1_107}"
    testFoldConst("${const_sql_1_107}")
    def const_sql_1_108 = """select "99999999999999999998.9999999999999999998", cast(cast("99999999999999999998.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_108_strict "${const_sql_1_108}"
    testFoldConst("${const_sql_1_108}")
    def const_sql_1_109 = """select "-99999999999999999998.9999999999999999998", cast(cast("-99999999999999999998.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_109_strict "${const_sql_1_109}"
    testFoldConst("${const_sql_1_109}")
    def const_sql_1_110 = """select "99999999999999999998.9999999999999999999", cast(cast("99999999999999999998.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_110_strict "${const_sql_1_110}"
    testFoldConst("${const_sql_1_110}")
    def const_sql_1_111 = """select "-99999999999999999998.9999999999999999999", cast(cast("-99999999999999999998.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_111_strict "${const_sql_1_111}"
    testFoldConst("${const_sql_1_111}")
    def const_sql_1_112 = """select "99999999999999999999.0000000000000000000", cast(cast("99999999999999999999.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_112_strict "${const_sql_1_112}"
    testFoldConst("${const_sql_1_112}")
    def const_sql_1_113 = """select "-99999999999999999999.0000000000000000000", cast(cast("-99999999999999999999.0000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_113_strict "${const_sql_1_113}"
    testFoldConst("${const_sql_1_113}")
    def const_sql_1_114 = """select "99999999999999999999.0000000000000000001", cast(cast("99999999999999999999.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_114_strict "${const_sql_1_114}"
    testFoldConst("${const_sql_1_114}")
    def const_sql_1_115 = """select "-99999999999999999999.0000000000000000001", cast(cast("-99999999999999999999.0000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_115_strict "${const_sql_1_115}"
    testFoldConst("${const_sql_1_115}")
    def const_sql_1_116 = """select "99999999999999999999.0000000000000000009", cast(cast("99999999999999999999.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_116_strict "${const_sql_1_116}"
    testFoldConst("${const_sql_1_116}")
    def const_sql_1_117 = """select "-99999999999999999999.0000000000000000009", cast(cast("-99999999999999999999.0000000000000000009" as decimalv3(39, 19)) as float);"""
    qt_sql_1_117_strict "${const_sql_1_117}"
    testFoldConst("${const_sql_1_117}")
    def const_sql_1_118 = """select "99999999999999999999.0999999999999999999", cast(cast("99999999999999999999.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_118_strict "${const_sql_1_118}"
    testFoldConst("${const_sql_1_118}")
    def const_sql_1_119 = """select "-99999999999999999999.0999999999999999999", cast(cast("-99999999999999999999.0999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_119_strict "${const_sql_1_119}"
    testFoldConst("${const_sql_1_119}")
    def const_sql_1_120 = """select "99999999999999999999.9000000000000000000", cast(cast("99999999999999999999.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_120_strict "${const_sql_1_120}"
    testFoldConst("${const_sql_1_120}")
    def const_sql_1_121 = """select "-99999999999999999999.9000000000000000000", cast(cast("-99999999999999999999.9000000000000000000" as decimalv3(39, 19)) as float);"""
    qt_sql_1_121_strict "${const_sql_1_121}"
    testFoldConst("${const_sql_1_121}")
    def const_sql_1_122 = """select "99999999999999999999.9000000000000000001", cast(cast("99999999999999999999.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_122_strict "${const_sql_1_122}"
    testFoldConst("${const_sql_1_122}")
    def const_sql_1_123 = """select "-99999999999999999999.9000000000000000001", cast(cast("-99999999999999999999.9000000000000000001" as decimalv3(39, 19)) as float);"""
    qt_sql_1_123_strict "${const_sql_1_123}"
    testFoldConst("${const_sql_1_123}")
    def const_sql_1_124 = """select "99999999999999999999.9999999999999999998", cast(cast("99999999999999999999.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_124_strict "${const_sql_1_124}"
    testFoldConst("${const_sql_1_124}")
    def const_sql_1_125 = """select "-99999999999999999999.9999999999999999998", cast(cast("-99999999999999999999.9999999999999999998" as decimalv3(39, 19)) as float);"""
    qt_sql_1_125_strict "${const_sql_1_125}"
    testFoldConst("${const_sql_1_125}")
    def const_sql_1_126 = """select "99999999999999999999.9999999999999999999", cast(cast("99999999999999999999.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_126_strict "${const_sql_1_126}"
    testFoldConst("${const_sql_1_126}")
    def const_sql_1_127 = """select "-99999999999999999999.9999999999999999999", cast(cast("-99999999999999999999.9999999999999999999" as decimalv3(39, 19)) as float);"""
    qt_sql_1_127_strict "${const_sql_1_127}"
    testFoldConst("${const_sql_1_127}")

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
    qt_sql_1_8_non_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    qt_sql_1_9_non_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    qt_sql_1_10_non_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    qt_sql_1_11_non_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    qt_sql_1_12_non_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    qt_sql_1_13_non_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    qt_sql_1_14_non_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    qt_sql_1_15_non_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    qt_sql_1_16_non_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    qt_sql_1_17_non_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    qt_sql_1_18_non_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    qt_sql_1_19_non_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    qt_sql_1_20_non_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    qt_sql_1_21_non_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    qt_sql_1_22_non_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    qt_sql_1_23_non_strict "${const_sql_1_23}"
    testFoldConst("${const_sql_1_23}")
    qt_sql_1_24_non_strict "${const_sql_1_24}"
    testFoldConst("${const_sql_1_24}")
    qt_sql_1_25_non_strict "${const_sql_1_25}"
    testFoldConst("${const_sql_1_25}")
    qt_sql_1_26_non_strict "${const_sql_1_26}"
    testFoldConst("${const_sql_1_26}")
    qt_sql_1_27_non_strict "${const_sql_1_27}"
    testFoldConst("${const_sql_1_27}")
    qt_sql_1_28_non_strict "${const_sql_1_28}"
    testFoldConst("${const_sql_1_28}")
    qt_sql_1_29_non_strict "${const_sql_1_29}"
    testFoldConst("${const_sql_1_29}")
    qt_sql_1_30_non_strict "${const_sql_1_30}"
    testFoldConst("${const_sql_1_30}")
    qt_sql_1_31_non_strict "${const_sql_1_31}"
    testFoldConst("${const_sql_1_31}")
    qt_sql_1_32_non_strict "${const_sql_1_32}"
    testFoldConst("${const_sql_1_32}")
    qt_sql_1_33_non_strict "${const_sql_1_33}"
    testFoldConst("${const_sql_1_33}")
    qt_sql_1_34_non_strict "${const_sql_1_34}"
    testFoldConst("${const_sql_1_34}")
    qt_sql_1_35_non_strict "${const_sql_1_35}"
    testFoldConst("${const_sql_1_35}")
    qt_sql_1_36_non_strict "${const_sql_1_36}"
    testFoldConst("${const_sql_1_36}")
    qt_sql_1_37_non_strict "${const_sql_1_37}"
    testFoldConst("${const_sql_1_37}")
    qt_sql_1_38_non_strict "${const_sql_1_38}"
    testFoldConst("${const_sql_1_38}")
    qt_sql_1_39_non_strict "${const_sql_1_39}"
    testFoldConst("${const_sql_1_39}")
    qt_sql_1_40_non_strict "${const_sql_1_40}"
    testFoldConst("${const_sql_1_40}")
    qt_sql_1_41_non_strict "${const_sql_1_41}"
    testFoldConst("${const_sql_1_41}")
    qt_sql_1_42_non_strict "${const_sql_1_42}"
    testFoldConst("${const_sql_1_42}")
    qt_sql_1_43_non_strict "${const_sql_1_43}"
    testFoldConst("${const_sql_1_43}")
    qt_sql_1_44_non_strict "${const_sql_1_44}"
    testFoldConst("${const_sql_1_44}")
    qt_sql_1_45_non_strict "${const_sql_1_45}"
    testFoldConst("${const_sql_1_45}")
    qt_sql_1_46_non_strict "${const_sql_1_46}"
    testFoldConst("${const_sql_1_46}")
    qt_sql_1_47_non_strict "${const_sql_1_47}"
    testFoldConst("${const_sql_1_47}")
    qt_sql_1_48_non_strict "${const_sql_1_48}"
    testFoldConst("${const_sql_1_48}")
    qt_sql_1_49_non_strict "${const_sql_1_49}"
    testFoldConst("${const_sql_1_49}")
    qt_sql_1_50_non_strict "${const_sql_1_50}"
    testFoldConst("${const_sql_1_50}")
    qt_sql_1_51_non_strict "${const_sql_1_51}"
    testFoldConst("${const_sql_1_51}")
    qt_sql_1_52_non_strict "${const_sql_1_52}"
    testFoldConst("${const_sql_1_52}")
    qt_sql_1_53_non_strict "${const_sql_1_53}"
    testFoldConst("${const_sql_1_53}")
    qt_sql_1_54_non_strict "${const_sql_1_54}"
    testFoldConst("${const_sql_1_54}")
    qt_sql_1_55_non_strict "${const_sql_1_55}"
    testFoldConst("${const_sql_1_55}")
    qt_sql_1_56_non_strict "${const_sql_1_56}"
    testFoldConst("${const_sql_1_56}")
    qt_sql_1_57_non_strict "${const_sql_1_57}"
    testFoldConst("${const_sql_1_57}")
    qt_sql_1_58_non_strict "${const_sql_1_58}"
    testFoldConst("${const_sql_1_58}")
    qt_sql_1_59_non_strict "${const_sql_1_59}"
    testFoldConst("${const_sql_1_59}")
    qt_sql_1_60_non_strict "${const_sql_1_60}"
    testFoldConst("${const_sql_1_60}")
    qt_sql_1_61_non_strict "${const_sql_1_61}"
    testFoldConst("${const_sql_1_61}")
    qt_sql_1_62_non_strict "${const_sql_1_62}"
    testFoldConst("${const_sql_1_62}")
    qt_sql_1_63_non_strict "${const_sql_1_63}"
    testFoldConst("${const_sql_1_63}")
    qt_sql_1_64_non_strict "${const_sql_1_64}"
    testFoldConst("${const_sql_1_64}")
    qt_sql_1_65_non_strict "${const_sql_1_65}"
    testFoldConst("${const_sql_1_65}")
    qt_sql_1_66_non_strict "${const_sql_1_66}"
    testFoldConst("${const_sql_1_66}")
    qt_sql_1_67_non_strict "${const_sql_1_67}"
    testFoldConst("${const_sql_1_67}")
    qt_sql_1_68_non_strict "${const_sql_1_68}"
    testFoldConst("${const_sql_1_68}")
    qt_sql_1_69_non_strict "${const_sql_1_69}"
    testFoldConst("${const_sql_1_69}")
    qt_sql_1_70_non_strict "${const_sql_1_70}"
    testFoldConst("${const_sql_1_70}")
    qt_sql_1_71_non_strict "${const_sql_1_71}"
    testFoldConst("${const_sql_1_71}")
    qt_sql_1_72_non_strict "${const_sql_1_72}"
    testFoldConst("${const_sql_1_72}")
    qt_sql_1_73_non_strict "${const_sql_1_73}"
    testFoldConst("${const_sql_1_73}")
    qt_sql_1_74_non_strict "${const_sql_1_74}"
    testFoldConst("${const_sql_1_74}")
    qt_sql_1_75_non_strict "${const_sql_1_75}"
    testFoldConst("${const_sql_1_75}")
    qt_sql_1_76_non_strict "${const_sql_1_76}"
    testFoldConst("${const_sql_1_76}")
    qt_sql_1_77_non_strict "${const_sql_1_77}"
    testFoldConst("${const_sql_1_77}")
    qt_sql_1_78_non_strict "${const_sql_1_78}"
    testFoldConst("${const_sql_1_78}")
    qt_sql_1_79_non_strict "${const_sql_1_79}"
    testFoldConst("${const_sql_1_79}")
    qt_sql_1_80_non_strict "${const_sql_1_80}"
    testFoldConst("${const_sql_1_80}")
    qt_sql_1_81_non_strict "${const_sql_1_81}"
    testFoldConst("${const_sql_1_81}")
    qt_sql_1_82_non_strict "${const_sql_1_82}"
    testFoldConst("${const_sql_1_82}")
    qt_sql_1_83_non_strict "${const_sql_1_83}"
    testFoldConst("${const_sql_1_83}")
    qt_sql_1_84_non_strict "${const_sql_1_84}"
    testFoldConst("${const_sql_1_84}")
    qt_sql_1_85_non_strict "${const_sql_1_85}"
    testFoldConst("${const_sql_1_85}")
    qt_sql_1_86_non_strict "${const_sql_1_86}"
    testFoldConst("${const_sql_1_86}")
    qt_sql_1_87_non_strict "${const_sql_1_87}"
    testFoldConst("${const_sql_1_87}")
    qt_sql_1_88_non_strict "${const_sql_1_88}"
    testFoldConst("${const_sql_1_88}")
    qt_sql_1_89_non_strict "${const_sql_1_89}"
    testFoldConst("${const_sql_1_89}")
    qt_sql_1_90_non_strict "${const_sql_1_90}"
    testFoldConst("${const_sql_1_90}")
    qt_sql_1_91_non_strict "${const_sql_1_91}"
    testFoldConst("${const_sql_1_91}")
    qt_sql_1_92_non_strict "${const_sql_1_92}"
    testFoldConst("${const_sql_1_92}")
    qt_sql_1_93_non_strict "${const_sql_1_93}"
    testFoldConst("${const_sql_1_93}")
    qt_sql_1_94_non_strict "${const_sql_1_94}"
    testFoldConst("${const_sql_1_94}")
    qt_sql_1_95_non_strict "${const_sql_1_95}"
    testFoldConst("${const_sql_1_95}")
    qt_sql_1_96_non_strict "${const_sql_1_96}"
    testFoldConst("${const_sql_1_96}")
    qt_sql_1_97_non_strict "${const_sql_1_97}"
    testFoldConst("${const_sql_1_97}")
    qt_sql_1_98_non_strict "${const_sql_1_98}"
    testFoldConst("${const_sql_1_98}")
    qt_sql_1_99_non_strict "${const_sql_1_99}"
    testFoldConst("${const_sql_1_99}")
    qt_sql_1_100_non_strict "${const_sql_1_100}"
    testFoldConst("${const_sql_1_100}")
    qt_sql_1_101_non_strict "${const_sql_1_101}"
    testFoldConst("${const_sql_1_101}")
    qt_sql_1_102_non_strict "${const_sql_1_102}"
    testFoldConst("${const_sql_1_102}")
    qt_sql_1_103_non_strict "${const_sql_1_103}"
    testFoldConst("${const_sql_1_103}")
    qt_sql_1_104_non_strict "${const_sql_1_104}"
    testFoldConst("${const_sql_1_104}")
    qt_sql_1_105_non_strict "${const_sql_1_105}"
    testFoldConst("${const_sql_1_105}")
    qt_sql_1_106_non_strict "${const_sql_1_106}"
    testFoldConst("${const_sql_1_106}")
    qt_sql_1_107_non_strict "${const_sql_1_107}"
    testFoldConst("${const_sql_1_107}")
    qt_sql_1_108_non_strict "${const_sql_1_108}"
    testFoldConst("${const_sql_1_108}")
    qt_sql_1_109_non_strict "${const_sql_1_109}"
    testFoldConst("${const_sql_1_109}")
    qt_sql_1_110_non_strict "${const_sql_1_110}"
    testFoldConst("${const_sql_1_110}")
    qt_sql_1_111_non_strict "${const_sql_1_111}"
    testFoldConst("${const_sql_1_111}")
    qt_sql_1_112_non_strict "${const_sql_1_112}"
    testFoldConst("${const_sql_1_112}")
    qt_sql_1_113_non_strict "${const_sql_1_113}"
    testFoldConst("${const_sql_1_113}")
    qt_sql_1_114_non_strict "${const_sql_1_114}"
    testFoldConst("${const_sql_1_114}")
    qt_sql_1_115_non_strict "${const_sql_1_115}"
    testFoldConst("${const_sql_1_115}")
    qt_sql_1_116_non_strict "${const_sql_1_116}"
    testFoldConst("${const_sql_1_116}")
    qt_sql_1_117_non_strict "${const_sql_1_117}"
    testFoldConst("${const_sql_1_117}")
    qt_sql_1_118_non_strict "${const_sql_1_118}"
    testFoldConst("${const_sql_1_118}")
    qt_sql_1_119_non_strict "${const_sql_1_119}"
    testFoldConst("${const_sql_1_119}")
    qt_sql_1_120_non_strict "${const_sql_1_120}"
    testFoldConst("${const_sql_1_120}")
    qt_sql_1_121_non_strict "${const_sql_1_121}"
    testFoldConst("${const_sql_1_121}")
    qt_sql_1_122_non_strict "${const_sql_1_122}"
    testFoldConst("${const_sql_1_122}")
    qt_sql_1_123_non_strict "${const_sql_1_123}"
    testFoldConst("${const_sql_1_123}")
    qt_sql_1_124_non_strict "${const_sql_1_124}"
    testFoldConst("${const_sql_1_124}")
    qt_sql_1_125_non_strict "${const_sql_1_125}"
    testFoldConst("${const_sql_1_125}")
    qt_sql_1_126_non_strict "${const_sql_1_126}"
    testFoldConst("${const_sql_1_126}")
    qt_sql_1_127_non_strict "${const_sql_1_127}"
    testFoldConst("${const_sql_1_127}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_0_strict "${const_sql_2_0}"
    testFoldConst("${const_sql_2_0}")
    def const_sql_2_1 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_1_strict "${const_sql_2_1}"
    testFoldConst("${const_sql_2_1}")
    def const_sql_2_2 = """select "0.00000000000000000000000000000000000001", cast(cast("0.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_2_strict "${const_sql_2_2}"
    testFoldConst("${const_sql_2_2}")
    def const_sql_2_3 = """select "-0.00000000000000000000000000000000000001", cast(cast("-0.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_3_strict "${const_sql_2_3}"
    testFoldConst("${const_sql_2_3}")
    def const_sql_2_4 = """select "0.00000000000000000000000000000000000009", cast(cast("0.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_4_strict "${const_sql_2_4}"
    testFoldConst("${const_sql_2_4}")
    def const_sql_2_5 = """select "-0.00000000000000000000000000000000000009", cast(cast("-0.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_5_strict "${const_sql_2_5}"
    testFoldConst("${const_sql_2_5}")
    def const_sql_2_6 = """select "0.09999999999999999999999999999999999999", cast(cast("0.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_6_strict "${const_sql_2_6}"
    testFoldConst("${const_sql_2_6}")
    def const_sql_2_7 = """select "-0.09999999999999999999999999999999999999", cast(cast("-0.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_7_strict "${const_sql_2_7}"
    testFoldConst("${const_sql_2_7}")
    def const_sql_2_8 = """select "0.90000000000000000000000000000000000000", cast(cast("0.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_8_strict "${const_sql_2_8}"
    testFoldConst("${const_sql_2_8}")
    def const_sql_2_9 = """select "-0.90000000000000000000000000000000000000", cast(cast("-0.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_9_strict "${const_sql_2_9}"
    testFoldConst("${const_sql_2_9}")
    def const_sql_2_10 = """select "0.90000000000000000000000000000000000001", cast(cast("0.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_10_strict "${const_sql_2_10}"
    testFoldConst("${const_sql_2_10}")
    def const_sql_2_11 = """select "-0.90000000000000000000000000000000000001", cast(cast("-0.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_11_strict "${const_sql_2_11}"
    testFoldConst("${const_sql_2_11}")
    def const_sql_2_12 = """select "0.99999999999999999999999999999999999998", cast(cast("0.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_12_strict "${const_sql_2_12}"
    testFoldConst("${const_sql_2_12}")
    def const_sql_2_13 = """select "-0.99999999999999999999999999999999999998", cast(cast("-0.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_13_strict "${const_sql_2_13}"
    testFoldConst("${const_sql_2_13}")
    def const_sql_2_14 = """select "0.99999999999999999999999999999999999999", cast(cast("0.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_14_strict "${const_sql_2_14}"
    testFoldConst("${const_sql_2_14}")
    def const_sql_2_15 = """select "-0.99999999999999999999999999999999999999", cast(cast("-0.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_15_strict "${const_sql_2_15}"
    testFoldConst("${const_sql_2_15}")
    def const_sql_2_16 = """select "1.00000000000000000000000000000000000000", cast(cast("1.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_16_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    def const_sql_2_17 = """select "-1.00000000000000000000000000000000000000", cast(cast("-1.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_17_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    def const_sql_2_18 = """select "1.00000000000000000000000000000000000001", cast(cast("1.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_18_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    def const_sql_2_19 = """select "-1.00000000000000000000000000000000000001", cast(cast("-1.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_19_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    def const_sql_2_20 = """select "1.00000000000000000000000000000000000009", cast(cast("1.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_20_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    def const_sql_2_21 = """select "-1.00000000000000000000000000000000000009", cast(cast("-1.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_21_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    def const_sql_2_22 = """select "1.09999999999999999999999999999999999999", cast(cast("1.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_22_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    def const_sql_2_23 = """select "-1.09999999999999999999999999999999999999", cast(cast("-1.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_23_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    def const_sql_2_24 = """select "1.90000000000000000000000000000000000000", cast(cast("1.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_24_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    def const_sql_2_25 = """select "-1.90000000000000000000000000000000000000", cast(cast("-1.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_25_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    def const_sql_2_26 = """select "1.90000000000000000000000000000000000001", cast(cast("1.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_26_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    def const_sql_2_27 = """select "-1.90000000000000000000000000000000000001", cast(cast("-1.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_27_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    def const_sql_2_28 = """select "1.99999999999999999999999999999999999998", cast(cast("1.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_28_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    def const_sql_2_29 = """select "-1.99999999999999999999999999999999999998", cast(cast("-1.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_29_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    def const_sql_2_30 = """select "1.99999999999999999999999999999999999999", cast(cast("1.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_30_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    def const_sql_2_31 = """select "-1.99999999999999999999999999999999999999", cast(cast("-1.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_31_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    def const_sql_2_32 = """select "8.00000000000000000000000000000000000000", cast(cast("8.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_32_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    def const_sql_2_33 = """select "-8.00000000000000000000000000000000000000", cast(cast("-8.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_33_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    def const_sql_2_34 = """select "8.00000000000000000000000000000000000001", cast(cast("8.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_34_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    def const_sql_2_35 = """select "-8.00000000000000000000000000000000000001", cast(cast("-8.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_35_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    def const_sql_2_36 = """select "8.00000000000000000000000000000000000009", cast(cast("8.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_36_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    def const_sql_2_37 = """select "-8.00000000000000000000000000000000000009", cast(cast("-8.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_37_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    def const_sql_2_38 = """select "8.09999999999999999999999999999999999999", cast(cast("8.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_38_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    def const_sql_2_39 = """select "-8.09999999999999999999999999999999999999", cast(cast("-8.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_39_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    def const_sql_2_40 = """select "8.90000000000000000000000000000000000000", cast(cast("8.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_40_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    def const_sql_2_41 = """select "-8.90000000000000000000000000000000000000", cast(cast("-8.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_41_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    def const_sql_2_42 = """select "8.90000000000000000000000000000000000001", cast(cast("8.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_42_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    def const_sql_2_43 = """select "-8.90000000000000000000000000000000000001", cast(cast("-8.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_43_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    def const_sql_2_44 = """select "8.99999999999999999999999999999999999998", cast(cast("8.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_44_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    def const_sql_2_45 = """select "-8.99999999999999999999999999999999999998", cast(cast("-8.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_45_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    def const_sql_2_46 = """select "8.99999999999999999999999999999999999999", cast(cast("8.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_46_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    def const_sql_2_47 = """select "-8.99999999999999999999999999999999999999", cast(cast("-8.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_47_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")
    def const_sql_2_48 = """select "9.00000000000000000000000000000000000000", cast(cast("9.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_48_strict "${const_sql_2_48}"
    testFoldConst("${const_sql_2_48}")
    def const_sql_2_49 = """select "-9.00000000000000000000000000000000000000", cast(cast("-9.00000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_49_strict "${const_sql_2_49}"
    testFoldConst("${const_sql_2_49}")
    def const_sql_2_50 = """select "9.00000000000000000000000000000000000001", cast(cast("9.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_50_strict "${const_sql_2_50}"
    testFoldConst("${const_sql_2_50}")
    def const_sql_2_51 = """select "-9.00000000000000000000000000000000000001", cast(cast("-9.00000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_51_strict "${const_sql_2_51}"
    testFoldConst("${const_sql_2_51}")
    def const_sql_2_52 = """select "9.00000000000000000000000000000000000009", cast(cast("9.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_52_strict "${const_sql_2_52}"
    testFoldConst("${const_sql_2_52}")
    def const_sql_2_53 = """select "-9.00000000000000000000000000000000000009", cast(cast("-9.00000000000000000000000000000000000009" as decimalv3(39, 38)) as float);"""
    qt_sql_2_53_strict "${const_sql_2_53}"
    testFoldConst("${const_sql_2_53}")
    def const_sql_2_54 = """select "9.09999999999999999999999999999999999999", cast(cast("9.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_54_strict "${const_sql_2_54}"
    testFoldConst("${const_sql_2_54}")
    def const_sql_2_55 = """select "-9.09999999999999999999999999999999999999", cast(cast("-9.09999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_55_strict "${const_sql_2_55}"
    testFoldConst("${const_sql_2_55}")
    def const_sql_2_56 = """select "9.90000000000000000000000000000000000000", cast(cast("9.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_56_strict "${const_sql_2_56}"
    testFoldConst("${const_sql_2_56}")
    def const_sql_2_57 = """select "-9.90000000000000000000000000000000000000", cast(cast("-9.90000000000000000000000000000000000000" as decimalv3(39, 38)) as float);"""
    qt_sql_2_57_strict "${const_sql_2_57}"
    testFoldConst("${const_sql_2_57}")
    def const_sql_2_58 = """select "9.90000000000000000000000000000000000001", cast(cast("9.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_58_strict "${const_sql_2_58}"
    testFoldConst("${const_sql_2_58}")
    def const_sql_2_59 = """select "-9.90000000000000000000000000000000000001", cast(cast("-9.90000000000000000000000000000000000001" as decimalv3(39, 38)) as float);"""
    qt_sql_2_59_strict "${const_sql_2_59}"
    testFoldConst("${const_sql_2_59}")
    def const_sql_2_60 = """select "9.99999999999999999999999999999999999998", cast(cast("9.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_60_strict "${const_sql_2_60}"
    testFoldConst("${const_sql_2_60}")
    def const_sql_2_61 = """select "-9.99999999999999999999999999999999999998", cast(cast("-9.99999999999999999999999999999999999998" as decimalv3(39, 38)) as float);"""
    qt_sql_2_61_strict "${const_sql_2_61}"
    testFoldConst("${const_sql_2_61}")
    def const_sql_2_62 = """select "9.99999999999999999999999999999999999999", cast(cast("9.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_62_strict "${const_sql_2_62}"
    testFoldConst("${const_sql_2_62}")
    def const_sql_2_63 = """select "-9.99999999999999999999999999999999999999", cast(cast("-9.99999999999999999999999999999999999999" as decimalv3(39, 38)) as float);"""
    qt_sql_2_63_strict "${const_sql_2_63}"
    testFoldConst("${const_sql_2_63}")

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
    qt_sql_2_16_non_strict "${const_sql_2_16}"
    testFoldConst("${const_sql_2_16}")
    qt_sql_2_17_non_strict "${const_sql_2_17}"
    testFoldConst("${const_sql_2_17}")
    qt_sql_2_18_non_strict "${const_sql_2_18}"
    testFoldConst("${const_sql_2_18}")
    qt_sql_2_19_non_strict "${const_sql_2_19}"
    testFoldConst("${const_sql_2_19}")
    qt_sql_2_20_non_strict "${const_sql_2_20}"
    testFoldConst("${const_sql_2_20}")
    qt_sql_2_21_non_strict "${const_sql_2_21}"
    testFoldConst("${const_sql_2_21}")
    qt_sql_2_22_non_strict "${const_sql_2_22}"
    testFoldConst("${const_sql_2_22}")
    qt_sql_2_23_non_strict "${const_sql_2_23}"
    testFoldConst("${const_sql_2_23}")
    qt_sql_2_24_non_strict "${const_sql_2_24}"
    testFoldConst("${const_sql_2_24}")
    qt_sql_2_25_non_strict "${const_sql_2_25}"
    testFoldConst("${const_sql_2_25}")
    qt_sql_2_26_non_strict "${const_sql_2_26}"
    testFoldConst("${const_sql_2_26}")
    qt_sql_2_27_non_strict "${const_sql_2_27}"
    testFoldConst("${const_sql_2_27}")
    qt_sql_2_28_non_strict "${const_sql_2_28}"
    testFoldConst("${const_sql_2_28}")
    qt_sql_2_29_non_strict "${const_sql_2_29}"
    testFoldConst("${const_sql_2_29}")
    qt_sql_2_30_non_strict "${const_sql_2_30}"
    testFoldConst("${const_sql_2_30}")
    qt_sql_2_31_non_strict "${const_sql_2_31}"
    testFoldConst("${const_sql_2_31}")
    qt_sql_2_32_non_strict "${const_sql_2_32}"
    testFoldConst("${const_sql_2_32}")
    qt_sql_2_33_non_strict "${const_sql_2_33}"
    testFoldConst("${const_sql_2_33}")
    qt_sql_2_34_non_strict "${const_sql_2_34}"
    testFoldConst("${const_sql_2_34}")
    qt_sql_2_35_non_strict "${const_sql_2_35}"
    testFoldConst("${const_sql_2_35}")
    qt_sql_2_36_non_strict "${const_sql_2_36}"
    testFoldConst("${const_sql_2_36}")
    qt_sql_2_37_non_strict "${const_sql_2_37}"
    testFoldConst("${const_sql_2_37}")
    qt_sql_2_38_non_strict "${const_sql_2_38}"
    testFoldConst("${const_sql_2_38}")
    qt_sql_2_39_non_strict "${const_sql_2_39}"
    testFoldConst("${const_sql_2_39}")
    qt_sql_2_40_non_strict "${const_sql_2_40}"
    testFoldConst("${const_sql_2_40}")
    qt_sql_2_41_non_strict "${const_sql_2_41}"
    testFoldConst("${const_sql_2_41}")
    qt_sql_2_42_non_strict "${const_sql_2_42}"
    testFoldConst("${const_sql_2_42}")
    qt_sql_2_43_non_strict "${const_sql_2_43}"
    testFoldConst("${const_sql_2_43}")
    qt_sql_2_44_non_strict "${const_sql_2_44}"
    testFoldConst("${const_sql_2_44}")
    qt_sql_2_45_non_strict "${const_sql_2_45}"
    testFoldConst("${const_sql_2_45}")
    qt_sql_2_46_non_strict "${const_sql_2_46}"
    testFoldConst("${const_sql_2_46}")
    qt_sql_2_47_non_strict "${const_sql_2_47}"
    testFoldConst("${const_sql_2_47}")
    qt_sql_2_48_non_strict "${const_sql_2_48}"
    testFoldConst("${const_sql_2_48}")
    qt_sql_2_49_non_strict "${const_sql_2_49}"
    testFoldConst("${const_sql_2_49}")
    qt_sql_2_50_non_strict "${const_sql_2_50}"
    testFoldConst("${const_sql_2_50}")
    qt_sql_2_51_non_strict "${const_sql_2_51}"
    testFoldConst("${const_sql_2_51}")
    qt_sql_2_52_non_strict "${const_sql_2_52}"
    testFoldConst("${const_sql_2_52}")
    qt_sql_2_53_non_strict "${const_sql_2_53}"
    testFoldConst("${const_sql_2_53}")
    qt_sql_2_54_non_strict "${const_sql_2_54}"
    testFoldConst("${const_sql_2_54}")
    qt_sql_2_55_non_strict "${const_sql_2_55}"
    testFoldConst("${const_sql_2_55}")
    qt_sql_2_56_non_strict "${const_sql_2_56}"
    testFoldConst("${const_sql_2_56}")
    qt_sql_2_57_non_strict "${const_sql_2_57}"
    testFoldConst("${const_sql_2_57}")
    qt_sql_2_58_non_strict "${const_sql_2_58}"
    testFoldConst("${const_sql_2_58}")
    qt_sql_2_59_non_strict "${const_sql_2_59}"
    testFoldConst("${const_sql_2_59}")
    qt_sql_2_60_non_strict "${const_sql_2_60}"
    testFoldConst("${const_sql_2_60}")
    qt_sql_2_61_non_strict "${const_sql_2_61}"
    testFoldConst("${const_sql_2_61}")
    qt_sql_2_62_non_strict "${const_sql_2_62}"
    testFoldConst("${const_sql_2_62}")
    qt_sql_2_63_non_strict "${const_sql_2_63}"
    testFoldConst("${const_sql_2_63}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0.000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000" as decimalv3(39, 39)) as float);"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0.000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000" as decimalv3(39, 39)) as float);"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0.000000000000000000000000000000000000001", cast(cast("0.000000000000000000000000000000000000001" as decimalv3(39, 39)) as float);"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "-0.000000000000000000000000000000000000001", cast(cast("-0.000000000000000000000000000000000000001" as decimalv3(39, 39)) as float);"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0.000000000000000000000000000000000000009", cast(cast("0.000000000000000000000000000000000000009" as decimalv3(39, 39)) as float);"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "-0.000000000000000000000000000000000000009", cast(cast("-0.000000000000000000000000000000000000009" as decimalv3(39, 39)) as float);"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0.099999999999999999999999999999999999999", cast(cast("0.099999999999999999999999999999999999999" as decimalv3(39, 39)) as float);"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "-0.099999999999999999999999999999999999999", cast(cast("-0.099999999999999999999999999999999999999" as decimalv3(39, 39)) as float);"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "0.900000000000000000000000000000000000000", cast(cast("0.900000000000000000000000000000000000000" as decimalv3(39, 39)) as float);"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "-0.900000000000000000000000000000000000000", cast(cast("-0.900000000000000000000000000000000000000" as decimalv3(39, 39)) as float);"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "0.900000000000000000000000000000000000001", cast(cast("0.900000000000000000000000000000000000001" as decimalv3(39, 39)) as float);"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "-0.900000000000000000000000000000000000001", cast(cast("-0.900000000000000000000000000000000000001" as decimalv3(39, 39)) as float);"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "0.999999999999999999999999999999999999998", cast(cast("0.999999999999999999999999999999999999998" as decimalv3(39, 39)) as float);"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "-0.999999999999999999999999999999999999998", cast(cast("-0.999999999999999999999999999999999999998" as decimalv3(39, 39)) as float);"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "0.999999999999999999999999999999999999999", cast(cast("0.999999999999999999999999999999999999999" as decimalv3(39, 39)) as float);"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "-0.999999999999999999999999999999999999999", cast(cast("-0.999999999999999999999999999999999999999" as decimalv3(39, 39)) as float);"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0", cast(cast("0" as decimalv3(76, 0)) as float);"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0", cast(cast("0" as decimalv3(76, 0)) as float);"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "1", cast(cast("1" as decimalv3(76, 0)) as float);"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "-1", cast(cast("-1" as decimalv3(76, 0)) as float);"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "9", cast(cast("9" as decimalv3(76, 0)) as float);"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "-9", cast(cast("-9" as decimalv3(76, 0)) as float);"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 0)) as float);"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "-9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 0)) as float);"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 0)) as float);"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "-9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 0)) as float);"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "9999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("9999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 0)) as float);"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "-9999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-9999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 0)) as float);"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "-9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 0)) as float);"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")

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
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0.0", cast(cast("0.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_0_strict "${const_sql_5_0}"
    testFoldConst("${const_sql_5_0}")
    def const_sql_5_1 = """select "0.0", cast(cast("0.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_1_strict "${const_sql_5_1}"
    testFoldConst("${const_sql_5_1}")
    def const_sql_5_2 = """select "0.1", cast(cast("0.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_2_strict "${const_sql_5_2}"
    testFoldConst("${const_sql_5_2}")
    def const_sql_5_3 = """select "-0.1", cast(cast("-0.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_3_strict "${const_sql_5_3}"
    testFoldConst("${const_sql_5_3}")
    def const_sql_5_4 = """select "0.8", cast(cast("0.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_4_strict "${const_sql_5_4}"
    testFoldConst("${const_sql_5_4}")
    def const_sql_5_5 = """select "-0.8", cast(cast("-0.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_5_strict "${const_sql_5_5}"
    testFoldConst("${const_sql_5_5}")
    def const_sql_5_6 = """select "0.9", cast(cast("0.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_6_strict "${const_sql_5_6}"
    testFoldConst("${const_sql_5_6}")
    def const_sql_5_7 = """select "-0.9", cast(cast("-0.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_7_strict "${const_sql_5_7}"
    testFoldConst("${const_sql_5_7}")
    def const_sql_5_8 = """select "1.0", cast(cast("1.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_8_strict "${const_sql_5_8}"
    testFoldConst("${const_sql_5_8}")
    def const_sql_5_9 = """select "-1.0", cast(cast("-1.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_9_strict "${const_sql_5_9}"
    testFoldConst("${const_sql_5_9}")
    def const_sql_5_10 = """select "1.1", cast(cast("1.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_10_strict "${const_sql_5_10}"
    testFoldConst("${const_sql_5_10}")
    def const_sql_5_11 = """select "-1.1", cast(cast("-1.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_11_strict "${const_sql_5_11}"
    testFoldConst("${const_sql_5_11}")
    def const_sql_5_12 = """select "1.8", cast(cast("1.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_12_strict "${const_sql_5_12}"
    testFoldConst("${const_sql_5_12}")
    def const_sql_5_13 = """select "-1.8", cast(cast("-1.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_13_strict "${const_sql_5_13}"
    testFoldConst("${const_sql_5_13}")
    def const_sql_5_14 = """select "1.9", cast(cast("1.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_14_strict "${const_sql_5_14}"
    testFoldConst("${const_sql_5_14}")
    def const_sql_5_15 = """select "-1.9", cast(cast("-1.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_15_strict "${const_sql_5_15}"
    testFoldConst("${const_sql_5_15}")
    def const_sql_5_16 = """select "9.0", cast(cast("9.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_16_strict "${const_sql_5_16}"
    testFoldConst("${const_sql_5_16}")
    def const_sql_5_17 = """select "-9.0", cast(cast("-9.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_17_strict "${const_sql_5_17}"
    testFoldConst("${const_sql_5_17}")
    def const_sql_5_18 = """select "9.1", cast(cast("9.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_18_strict "${const_sql_5_18}"
    testFoldConst("${const_sql_5_18}")
    def const_sql_5_19 = """select "-9.1", cast(cast("-9.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_19_strict "${const_sql_5_19}"
    testFoldConst("${const_sql_5_19}")
    def const_sql_5_20 = """select "9.8", cast(cast("9.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_20_strict "${const_sql_5_20}"
    testFoldConst("${const_sql_5_20}")
    def const_sql_5_21 = """select "-9.8", cast(cast("-9.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_21_strict "${const_sql_5_21}"
    testFoldConst("${const_sql_5_21}")
    def const_sql_5_22 = """select "9.9", cast(cast("9.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_22_strict "${const_sql_5_22}"
    testFoldConst("${const_sql_5_22}")
    def const_sql_5_23 = """select "-9.9", cast(cast("-9.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_23_strict "${const_sql_5_23}"
    testFoldConst("${const_sql_5_23}")
    def const_sql_5_24 = """select "99999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("99999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_24_strict "${const_sql_5_24}"
    testFoldConst("${const_sql_5_24}")
    def const_sql_5_25 = """select "-99999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("-99999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_25_strict "${const_sql_5_25}"
    testFoldConst("${const_sql_5_25}")
    def const_sql_5_26 = """select "99999999999999999999999999999999999999999999999999999999999999999999999999.1", cast(cast("99999999999999999999999999999999999999999999999999999999999999999999999999.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_26_strict "${const_sql_5_26}"
    testFoldConst("${const_sql_5_26}")
    def const_sql_5_27 = """select "-99999999999999999999999999999999999999999999999999999999999999999999999999.1", cast(cast("-99999999999999999999999999999999999999999999999999999999999999999999999999.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_27_strict "${const_sql_5_27}"
    testFoldConst("${const_sql_5_27}")
    def const_sql_5_28 = """select "99999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("99999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_28_strict "${const_sql_5_28}"
    testFoldConst("${const_sql_5_28}")
    def const_sql_5_29 = """select "-99999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("-99999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_29_strict "${const_sql_5_29}"
    testFoldConst("${const_sql_5_29}")
    def const_sql_5_30 = """select "99999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("99999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_30_strict "${const_sql_5_30}"
    testFoldConst("${const_sql_5_30}")
    def const_sql_5_31 = """select "-99999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("-99999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_31_strict "${const_sql_5_31}"
    testFoldConst("${const_sql_5_31}")
    def const_sql_5_32 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.0", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_32_strict "${const_sql_5_32}"
    testFoldConst("${const_sql_5_32}")
    def const_sql_5_33 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.0", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_33_strict "${const_sql_5_33}"
    testFoldConst("${const_sql_5_33}")
    def const_sql_5_34 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.1", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_34_strict "${const_sql_5_34}"
    testFoldConst("${const_sql_5_34}")
    def const_sql_5_35 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.1", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_35_strict "${const_sql_5_35}"
    testFoldConst("${const_sql_5_35}")
    def const_sql_5_36 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.8", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_36_strict "${const_sql_5_36}"
    testFoldConst("${const_sql_5_36}")
    def const_sql_5_37 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.8", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_37_strict "${const_sql_5_37}"
    testFoldConst("${const_sql_5_37}")
    def const_sql_5_38 = """select "900000000000000000000000000000000000000000000000000000000000000000000000000.9", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000000.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_38_strict "${const_sql_5_38}"
    testFoldConst("${const_sql_5_38}")
    def const_sql_5_39 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000000.9", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000000.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_39_strict "${const_sql_5_39}"
    testFoldConst("${const_sql_5_39}")
    def const_sql_5_40 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.0", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_40_strict "${const_sql_5_40}"
    testFoldConst("${const_sql_5_40}")
    def const_sql_5_41 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.0", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_41_strict "${const_sql_5_41}"
    testFoldConst("${const_sql_5_41}")
    def const_sql_5_42 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.1", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_42_strict "${const_sql_5_42}"
    testFoldConst("${const_sql_5_42}")
    def const_sql_5_43 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.1", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_43_strict "${const_sql_5_43}"
    testFoldConst("${const_sql_5_43}")
    def const_sql_5_44 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.8", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_44_strict "${const_sql_5_44}"
    testFoldConst("${const_sql_5_44}")
    def const_sql_5_45 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.8", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_45_strict "${const_sql_5_45}"
    testFoldConst("${const_sql_5_45}")
    def const_sql_5_46 = """select "900000000000000000000000000000000000000000000000000000000000000000000000001.9", cast(cast("900000000000000000000000000000000000000000000000000000000000000000000000001.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_46_strict "${const_sql_5_46}"
    testFoldConst("${const_sql_5_46}")
    def const_sql_5_47 = """select "-900000000000000000000000000000000000000000000000000000000000000000000000001.9", cast(cast("-900000000000000000000000000000000000000000000000000000000000000000000000001.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_47_strict "${const_sql_5_47}"
    testFoldConst("${const_sql_5_47}")
    def const_sql_5_48 = """select "999999999999999999999999999999999999999999999999999999999999999999999999998.0", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999998.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_48_strict "${const_sql_5_48}"
    testFoldConst("${const_sql_5_48}")
    def const_sql_5_49 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999998.0", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999998.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_49_strict "${const_sql_5_49}"
    testFoldConst("${const_sql_5_49}")
    def const_sql_5_50 = """select "999999999999999999999999999999999999999999999999999999999999999999999999998.1", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999998.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_50_strict "${const_sql_5_50}"
    testFoldConst("${const_sql_5_50}")
    def const_sql_5_51 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999998.1", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999998.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_51_strict "${const_sql_5_51}"
    testFoldConst("${const_sql_5_51}")
    def const_sql_5_52 = """select "999999999999999999999999999999999999999999999999999999999999999999999999998.8", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999998.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_52_strict "${const_sql_5_52}"
    testFoldConst("${const_sql_5_52}")
    def const_sql_5_53 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999998.8", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999998.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_53_strict "${const_sql_5_53}"
    testFoldConst("${const_sql_5_53}")
    def const_sql_5_54 = """select "999999999999999999999999999999999999999999999999999999999999999999999999998.9", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999998.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_54_strict "${const_sql_5_54}"
    testFoldConst("${const_sql_5_54}")
    def const_sql_5_55 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999998.9", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999998.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_55_strict "${const_sql_5_55}"
    testFoldConst("${const_sql_5_55}")
    def const_sql_5_56 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_56_strict "${const_sql_5_56}"
    testFoldConst("${const_sql_5_56}")
    def const_sql_5_57 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.0", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.0" as decimalv3(76, 1)) as float);"""
    qt_sql_5_57_strict "${const_sql_5_57}"
    testFoldConst("${const_sql_5_57}")
    def const_sql_5_58 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.1", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_58_strict "${const_sql_5_58}"
    testFoldConst("${const_sql_5_58}")
    def const_sql_5_59 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.1", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.1" as decimalv3(76, 1)) as float);"""
    qt_sql_5_59_strict "${const_sql_5_59}"
    testFoldConst("${const_sql_5_59}")
    def const_sql_5_60 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_60_strict "${const_sql_5_60}"
    testFoldConst("${const_sql_5_60}")
    def const_sql_5_61 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.8", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.8" as decimalv3(76, 1)) as float);"""
    qt_sql_5_61_strict "${const_sql_5_61}"
    testFoldConst("${const_sql_5_61}")
    def const_sql_5_62 = """select "999999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("999999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
    qt_sql_5_62_strict "${const_sql_5_62}"
    testFoldConst("${const_sql_5_62}")
    def const_sql_5_63 = """select "-999999999999999999999999999999999999999999999999999999999999999999999999999.9", cast(cast("-999999999999999999999999999999999999999999999999999999999999999999999999999.9" as decimalv3(76, 1)) as float);"""
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
    def const_sql_6_0 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_0_strict "${const_sql_6_0}"
    testFoldConst("${const_sql_6_0}")
    def const_sql_6_1 = """select "0.00000000000000000000000000000000000000", cast(cast("0.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_1_strict "${const_sql_6_1}"
    testFoldConst("${const_sql_6_1}")
    def const_sql_6_2 = """select "0.00000000000000000000000000000000000001", cast(cast("0.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_2_strict "${const_sql_6_2}"
    testFoldConst("${const_sql_6_2}")
    def const_sql_6_3 = """select "-0.00000000000000000000000000000000000001", cast(cast("-0.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_3_strict "${const_sql_6_3}"
    testFoldConst("${const_sql_6_3}")
    def const_sql_6_4 = """select "0.00000000000000000000000000000000000009", cast(cast("0.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_4_strict "${const_sql_6_4}"
    testFoldConst("${const_sql_6_4}")
    def const_sql_6_5 = """select "-0.00000000000000000000000000000000000009", cast(cast("-0.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_5_strict "${const_sql_6_5}"
    testFoldConst("${const_sql_6_5}")
    def const_sql_6_6 = """select "0.09999999999999999999999999999999999999", cast(cast("0.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_6_strict "${const_sql_6_6}"
    testFoldConst("${const_sql_6_6}")
    def const_sql_6_7 = """select "-0.09999999999999999999999999999999999999", cast(cast("-0.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_7_strict "${const_sql_6_7}"
    testFoldConst("${const_sql_6_7}")
    def const_sql_6_8 = """select "0.90000000000000000000000000000000000000", cast(cast("0.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_8_strict "${const_sql_6_8}"
    testFoldConst("${const_sql_6_8}")
    def const_sql_6_9 = """select "-0.90000000000000000000000000000000000000", cast(cast("-0.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_9_strict "${const_sql_6_9}"
    testFoldConst("${const_sql_6_9}")
    def const_sql_6_10 = """select "0.90000000000000000000000000000000000001", cast(cast("0.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_10_strict "${const_sql_6_10}"
    testFoldConst("${const_sql_6_10}")
    def const_sql_6_11 = """select "-0.90000000000000000000000000000000000001", cast(cast("-0.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_11_strict "${const_sql_6_11}"
    testFoldConst("${const_sql_6_11}")
    def const_sql_6_12 = """select "0.99999999999999999999999999999999999998", cast(cast("0.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_12_strict "${const_sql_6_12}"
    testFoldConst("${const_sql_6_12}")
    def const_sql_6_13 = """select "-0.99999999999999999999999999999999999998", cast(cast("-0.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_13_strict "${const_sql_6_13}"
    testFoldConst("${const_sql_6_13}")
    def const_sql_6_14 = """select "0.99999999999999999999999999999999999999", cast(cast("0.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_14_strict "${const_sql_6_14}"
    testFoldConst("${const_sql_6_14}")
    def const_sql_6_15 = """select "-0.99999999999999999999999999999999999999", cast(cast("-0.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_15_strict "${const_sql_6_15}"
    testFoldConst("${const_sql_6_15}")
    def const_sql_6_16 = """select "1.00000000000000000000000000000000000000", cast(cast("1.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_16_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    def const_sql_6_17 = """select "-1.00000000000000000000000000000000000000", cast(cast("-1.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_17_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")
    def const_sql_6_18 = """select "1.00000000000000000000000000000000000001", cast(cast("1.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_18_strict "${const_sql_6_18}"
    testFoldConst("${const_sql_6_18}")
    def const_sql_6_19 = """select "-1.00000000000000000000000000000000000001", cast(cast("-1.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_19_strict "${const_sql_6_19}"
    testFoldConst("${const_sql_6_19}")
    def const_sql_6_20 = """select "1.00000000000000000000000000000000000009", cast(cast("1.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_20_strict "${const_sql_6_20}"
    testFoldConst("${const_sql_6_20}")
    def const_sql_6_21 = """select "-1.00000000000000000000000000000000000009", cast(cast("-1.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_21_strict "${const_sql_6_21}"
    testFoldConst("${const_sql_6_21}")
    def const_sql_6_22 = """select "1.09999999999999999999999999999999999999", cast(cast("1.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_22_strict "${const_sql_6_22}"
    testFoldConst("${const_sql_6_22}")
    def const_sql_6_23 = """select "-1.09999999999999999999999999999999999999", cast(cast("-1.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_23_strict "${const_sql_6_23}"
    testFoldConst("${const_sql_6_23}")
    def const_sql_6_24 = """select "1.90000000000000000000000000000000000000", cast(cast("1.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_24_strict "${const_sql_6_24}"
    testFoldConst("${const_sql_6_24}")
    def const_sql_6_25 = """select "-1.90000000000000000000000000000000000000", cast(cast("-1.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_25_strict "${const_sql_6_25}"
    testFoldConst("${const_sql_6_25}")
    def const_sql_6_26 = """select "1.90000000000000000000000000000000000001", cast(cast("1.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_26_strict "${const_sql_6_26}"
    testFoldConst("${const_sql_6_26}")
    def const_sql_6_27 = """select "-1.90000000000000000000000000000000000001", cast(cast("-1.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_27_strict "${const_sql_6_27}"
    testFoldConst("${const_sql_6_27}")
    def const_sql_6_28 = """select "1.99999999999999999999999999999999999998", cast(cast("1.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_28_strict "${const_sql_6_28}"
    testFoldConst("${const_sql_6_28}")
    def const_sql_6_29 = """select "-1.99999999999999999999999999999999999998", cast(cast("-1.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_29_strict "${const_sql_6_29}"
    testFoldConst("${const_sql_6_29}")
    def const_sql_6_30 = """select "1.99999999999999999999999999999999999999", cast(cast("1.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_30_strict "${const_sql_6_30}"
    testFoldConst("${const_sql_6_30}")
    def const_sql_6_31 = """select "-1.99999999999999999999999999999999999999", cast(cast("-1.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_31_strict "${const_sql_6_31}"
    testFoldConst("${const_sql_6_31}")
    def const_sql_6_32 = """select "9.00000000000000000000000000000000000000", cast(cast("9.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_32_strict "${const_sql_6_32}"
    testFoldConst("${const_sql_6_32}")
    def const_sql_6_33 = """select "-9.00000000000000000000000000000000000000", cast(cast("-9.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_33_strict "${const_sql_6_33}"
    testFoldConst("${const_sql_6_33}")
    def const_sql_6_34 = """select "9.00000000000000000000000000000000000001", cast(cast("9.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_34_strict "${const_sql_6_34}"
    testFoldConst("${const_sql_6_34}")
    def const_sql_6_35 = """select "-9.00000000000000000000000000000000000001", cast(cast("-9.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_35_strict "${const_sql_6_35}"
    testFoldConst("${const_sql_6_35}")
    def const_sql_6_36 = """select "9.00000000000000000000000000000000000009", cast(cast("9.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_36_strict "${const_sql_6_36}"
    testFoldConst("${const_sql_6_36}")
    def const_sql_6_37 = """select "-9.00000000000000000000000000000000000009", cast(cast("-9.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_37_strict "${const_sql_6_37}"
    testFoldConst("${const_sql_6_37}")
    def const_sql_6_38 = """select "9.09999999999999999999999999999999999999", cast(cast("9.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_38_strict "${const_sql_6_38}"
    testFoldConst("${const_sql_6_38}")
    def const_sql_6_39 = """select "-9.09999999999999999999999999999999999999", cast(cast("-9.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_39_strict "${const_sql_6_39}"
    testFoldConst("${const_sql_6_39}")
    def const_sql_6_40 = """select "9.90000000000000000000000000000000000000", cast(cast("9.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_40_strict "${const_sql_6_40}"
    testFoldConst("${const_sql_6_40}")
    def const_sql_6_41 = """select "-9.90000000000000000000000000000000000000", cast(cast("-9.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_41_strict "${const_sql_6_41}"
    testFoldConst("${const_sql_6_41}")
    def const_sql_6_42 = """select "9.90000000000000000000000000000000000001", cast(cast("9.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_42_strict "${const_sql_6_42}"
    testFoldConst("${const_sql_6_42}")
    def const_sql_6_43 = """select "-9.90000000000000000000000000000000000001", cast(cast("-9.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_43_strict "${const_sql_6_43}"
    testFoldConst("${const_sql_6_43}")
    def const_sql_6_44 = """select "9.99999999999999999999999999999999999998", cast(cast("9.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_44_strict "${const_sql_6_44}"
    testFoldConst("${const_sql_6_44}")
    def const_sql_6_45 = """select "-9.99999999999999999999999999999999999998", cast(cast("-9.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_45_strict "${const_sql_6_45}"
    testFoldConst("${const_sql_6_45}")
    def const_sql_6_46 = """select "9.99999999999999999999999999999999999999", cast(cast("9.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_46_strict "${const_sql_6_46}"
    testFoldConst("${const_sql_6_46}")
    def const_sql_6_47 = """select "-9.99999999999999999999999999999999999999", cast(cast("-9.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_47_strict "${const_sql_6_47}"
    testFoldConst("${const_sql_6_47}")
    def const_sql_6_48 = """select "9999999999999999999999999999999999999.00000000000000000000000000000000000000", cast(cast("9999999999999999999999999999999999999.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_48_strict "${const_sql_6_48}"
    testFoldConst("${const_sql_6_48}")
    def const_sql_6_49 = """select "-9999999999999999999999999999999999999.00000000000000000000000000000000000000", cast(cast("-9999999999999999999999999999999999999.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_49_strict "${const_sql_6_49}"
    testFoldConst("${const_sql_6_49}")
    def const_sql_6_50 = """select "9999999999999999999999999999999999999.00000000000000000000000000000000000001", cast(cast("9999999999999999999999999999999999999.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_50_strict "${const_sql_6_50}"
    testFoldConst("${const_sql_6_50}")
    def const_sql_6_51 = """select "-9999999999999999999999999999999999999.00000000000000000000000000000000000001", cast(cast("-9999999999999999999999999999999999999.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_51_strict "${const_sql_6_51}"
    testFoldConst("${const_sql_6_51}")
    def const_sql_6_52 = """select "9999999999999999999999999999999999999.00000000000000000000000000000000000009", cast(cast("9999999999999999999999999999999999999.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_52_strict "${const_sql_6_52}"
    testFoldConst("${const_sql_6_52}")
    def const_sql_6_53 = """select "-9999999999999999999999999999999999999.00000000000000000000000000000000000009", cast(cast("-9999999999999999999999999999999999999.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_53_strict "${const_sql_6_53}"
    testFoldConst("${const_sql_6_53}")
    def const_sql_6_54 = """select "9999999999999999999999999999999999999.09999999999999999999999999999999999999", cast(cast("9999999999999999999999999999999999999.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_54_strict "${const_sql_6_54}"
    testFoldConst("${const_sql_6_54}")
    def const_sql_6_55 = """select "-9999999999999999999999999999999999999.09999999999999999999999999999999999999", cast(cast("-9999999999999999999999999999999999999.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_55_strict "${const_sql_6_55}"
    testFoldConst("${const_sql_6_55}")
    def const_sql_6_56 = """select "9999999999999999999999999999999999999.90000000000000000000000000000000000000", cast(cast("9999999999999999999999999999999999999.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_56_strict "${const_sql_6_56}"
    testFoldConst("${const_sql_6_56}")
    def const_sql_6_57 = """select "-9999999999999999999999999999999999999.90000000000000000000000000000000000000", cast(cast("-9999999999999999999999999999999999999.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_57_strict "${const_sql_6_57}"
    testFoldConst("${const_sql_6_57}")
    def const_sql_6_58 = """select "9999999999999999999999999999999999999.90000000000000000000000000000000000001", cast(cast("9999999999999999999999999999999999999.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_58_strict "${const_sql_6_58}"
    testFoldConst("${const_sql_6_58}")
    def const_sql_6_59 = """select "-9999999999999999999999999999999999999.90000000000000000000000000000000000001", cast(cast("-9999999999999999999999999999999999999.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_59_strict "${const_sql_6_59}"
    testFoldConst("${const_sql_6_59}")
    def const_sql_6_60 = """select "9999999999999999999999999999999999999.99999999999999999999999999999999999998", cast(cast("9999999999999999999999999999999999999.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_60_strict "${const_sql_6_60}"
    testFoldConst("${const_sql_6_60}")
    def const_sql_6_61 = """select "-9999999999999999999999999999999999999.99999999999999999999999999999999999998", cast(cast("-9999999999999999999999999999999999999.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_61_strict "${const_sql_6_61}"
    testFoldConst("${const_sql_6_61}")
    def const_sql_6_62 = """select "9999999999999999999999999999999999999.99999999999999999999999999999999999999", cast(cast("9999999999999999999999999999999999999.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_62_strict "${const_sql_6_62}"
    testFoldConst("${const_sql_6_62}")
    def const_sql_6_63 = """select "-9999999999999999999999999999999999999.99999999999999999999999999999999999999", cast(cast("-9999999999999999999999999999999999999.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_63_strict "${const_sql_6_63}"
    testFoldConst("${const_sql_6_63}")
    def const_sql_6_64 = """select "90000000000000000000000000000000000000.00000000000000000000000000000000000000", cast(cast("90000000000000000000000000000000000000.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_64_strict "${const_sql_6_64}"
    testFoldConst("${const_sql_6_64}")
    def const_sql_6_65 = """select "-90000000000000000000000000000000000000.00000000000000000000000000000000000000", cast(cast("-90000000000000000000000000000000000000.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_65_strict "${const_sql_6_65}"
    testFoldConst("${const_sql_6_65}")
    def const_sql_6_66 = """select "90000000000000000000000000000000000000.00000000000000000000000000000000000001", cast(cast("90000000000000000000000000000000000000.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_66_strict "${const_sql_6_66}"
    testFoldConst("${const_sql_6_66}")
    def const_sql_6_67 = """select "-90000000000000000000000000000000000000.00000000000000000000000000000000000001", cast(cast("-90000000000000000000000000000000000000.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_67_strict "${const_sql_6_67}"
    testFoldConst("${const_sql_6_67}")
    def const_sql_6_68 = """select "90000000000000000000000000000000000000.00000000000000000000000000000000000009", cast(cast("90000000000000000000000000000000000000.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_68_strict "${const_sql_6_68}"
    testFoldConst("${const_sql_6_68}")
    def const_sql_6_69 = """select "-90000000000000000000000000000000000000.00000000000000000000000000000000000009", cast(cast("-90000000000000000000000000000000000000.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_69_strict "${const_sql_6_69}"
    testFoldConst("${const_sql_6_69}")
    def const_sql_6_70 = """select "90000000000000000000000000000000000000.09999999999999999999999999999999999999", cast(cast("90000000000000000000000000000000000000.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_70_strict "${const_sql_6_70}"
    testFoldConst("${const_sql_6_70}")
    def const_sql_6_71 = """select "-90000000000000000000000000000000000000.09999999999999999999999999999999999999", cast(cast("-90000000000000000000000000000000000000.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_71_strict "${const_sql_6_71}"
    testFoldConst("${const_sql_6_71}")
    def const_sql_6_72 = """select "90000000000000000000000000000000000000.90000000000000000000000000000000000000", cast(cast("90000000000000000000000000000000000000.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_72_strict "${const_sql_6_72}"
    testFoldConst("${const_sql_6_72}")
    def const_sql_6_73 = """select "-90000000000000000000000000000000000000.90000000000000000000000000000000000000", cast(cast("-90000000000000000000000000000000000000.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_73_strict "${const_sql_6_73}"
    testFoldConst("${const_sql_6_73}")
    def const_sql_6_74 = """select "90000000000000000000000000000000000000.90000000000000000000000000000000000001", cast(cast("90000000000000000000000000000000000000.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_74_strict "${const_sql_6_74}"
    testFoldConst("${const_sql_6_74}")
    def const_sql_6_75 = """select "-90000000000000000000000000000000000000.90000000000000000000000000000000000001", cast(cast("-90000000000000000000000000000000000000.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_75_strict "${const_sql_6_75}"
    testFoldConst("${const_sql_6_75}")
    def const_sql_6_76 = """select "90000000000000000000000000000000000000.99999999999999999999999999999999999998", cast(cast("90000000000000000000000000000000000000.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_76_strict "${const_sql_6_76}"
    testFoldConst("${const_sql_6_76}")
    def const_sql_6_77 = """select "-90000000000000000000000000000000000000.99999999999999999999999999999999999998", cast(cast("-90000000000000000000000000000000000000.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_77_strict "${const_sql_6_77}"
    testFoldConst("${const_sql_6_77}")
    def const_sql_6_78 = """select "90000000000000000000000000000000000000.99999999999999999999999999999999999999", cast(cast("90000000000000000000000000000000000000.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_78_strict "${const_sql_6_78}"
    testFoldConst("${const_sql_6_78}")
    def const_sql_6_79 = """select "-90000000000000000000000000000000000000.99999999999999999999999999999999999999", cast(cast("-90000000000000000000000000000000000000.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_79_strict "${const_sql_6_79}"
    testFoldConst("${const_sql_6_79}")
    def const_sql_6_80 = """select "90000000000000000000000000000000000001.00000000000000000000000000000000000000", cast(cast("90000000000000000000000000000000000001.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_80_strict "${const_sql_6_80}"
    testFoldConst("${const_sql_6_80}")
    def const_sql_6_81 = """select "-90000000000000000000000000000000000001.00000000000000000000000000000000000000", cast(cast("-90000000000000000000000000000000000001.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_81_strict "${const_sql_6_81}"
    testFoldConst("${const_sql_6_81}")
    def const_sql_6_82 = """select "90000000000000000000000000000000000001.00000000000000000000000000000000000001", cast(cast("90000000000000000000000000000000000001.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_82_strict "${const_sql_6_82}"
    testFoldConst("${const_sql_6_82}")
    def const_sql_6_83 = """select "-90000000000000000000000000000000000001.00000000000000000000000000000000000001", cast(cast("-90000000000000000000000000000000000001.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_83_strict "${const_sql_6_83}"
    testFoldConst("${const_sql_6_83}")
    def const_sql_6_84 = """select "90000000000000000000000000000000000001.00000000000000000000000000000000000009", cast(cast("90000000000000000000000000000000000001.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_84_strict "${const_sql_6_84}"
    testFoldConst("${const_sql_6_84}")
    def const_sql_6_85 = """select "-90000000000000000000000000000000000001.00000000000000000000000000000000000009", cast(cast("-90000000000000000000000000000000000001.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_85_strict "${const_sql_6_85}"
    testFoldConst("${const_sql_6_85}")
    def const_sql_6_86 = """select "90000000000000000000000000000000000001.09999999999999999999999999999999999999", cast(cast("90000000000000000000000000000000000001.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_86_strict "${const_sql_6_86}"
    testFoldConst("${const_sql_6_86}")
    def const_sql_6_87 = """select "-90000000000000000000000000000000000001.09999999999999999999999999999999999999", cast(cast("-90000000000000000000000000000000000001.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_87_strict "${const_sql_6_87}"
    testFoldConst("${const_sql_6_87}")
    def const_sql_6_88 = """select "90000000000000000000000000000000000001.90000000000000000000000000000000000000", cast(cast("90000000000000000000000000000000000001.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_88_strict "${const_sql_6_88}"
    testFoldConst("${const_sql_6_88}")
    def const_sql_6_89 = """select "-90000000000000000000000000000000000001.90000000000000000000000000000000000000", cast(cast("-90000000000000000000000000000000000001.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_89_strict "${const_sql_6_89}"
    testFoldConst("${const_sql_6_89}")
    def const_sql_6_90 = """select "90000000000000000000000000000000000001.90000000000000000000000000000000000001", cast(cast("90000000000000000000000000000000000001.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_90_strict "${const_sql_6_90}"
    testFoldConst("${const_sql_6_90}")
    def const_sql_6_91 = """select "-90000000000000000000000000000000000001.90000000000000000000000000000000000001", cast(cast("-90000000000000000000000000000000000001.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_91_strict "${const_sql_6_91}"
    testFoldConst("${const_sql_6_91}")
    def const_sql_6_92 = """select "90000000000000000000000000000000000001.99999999999999999999999999999999999998", cast(cast("90000000000000000000000000000000000001.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_92_strict "${const_sql_6_92}"
    testFoldConst("${const_sql_6_92}")
    def const_sql_6_93 = """select "-90000000000000000000000000000000000001.99999999999999999999999999999999999998", cast(cast("-90000000000000000000000000000000000001.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_93_strict "${const_sql_6_93}"
    testFoldConst("${const_sql_6_93}")
    def const_sql_6_94 = """select "90000000000000000000000000000000000001.99999999999999999999999999999999999999", cast(cast("90000000000000000000000000000000000001.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_94_strict "${const_sql_6_94}"
    testFoldConst("${const_sql_6_94}")
    def const_sql_6_95 = """select "-90000000000000000000000000000000000001.99999999999999999999999999999999999999", cast(cast("-90000000000000000000000000000000000001.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_95_strict "${const_sql_6_95}"
    testFoldConst("${const_sql_6_95}")
    def const_sql_6_96 = """select "99999999999999999999999999999999999998.00000000000000000000000000000000000000", cast(cast("99999999999999999999999999999999999998.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_96_strict "${const_sql_6_96}"
    testFoldConst("${const_sql_6_96}")
    def const_sql_6_97 = """select "-99999999999999999999999999999999999998.00000000000000000000000000000000000000", cast(cast("-99999999999999999999999999999999999998.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_97_strict "${const_sql_6_97}"
    testFoldConst("${const_sql_6_97}")
    def const_sql_6_98 = """select "99999999999999999999999999999999999998.00000000000000000000000000000000000001", cast(cast("99999999999999999999999999999999999998.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_98_strict "${const_sql_6_98}"
    testFoldConst("${const_sql_6_98}")
    def const_sql_6_99 = """select "-99999999999999999999999999999999999998.00000000000000000000000000000000000001", cast(cast("-99999999999999999999999999999999999998.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_99_strict "${const_sql_6_99}"
    testFoldConst("${const_sql_6_99}")
    def const_sql_6_100 = """select "99999999999999999999999999999999999998.00000000000000000000000000000000000009", cast(cast("99999999999999999999999999999999999998.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_100_strict "${const_sql_6_100}"
    testFoldConst("${const_sql_6_100}")
    def const_sql_6_101 = """select "-99999999999999999999999999999999999998.00000000000000000000000000000000000009", cast(cast("-99999999999999999999999999999999999998.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_101_strict "${const_sql_6_101}"
    testFoldConst("${const_sql_6_101}")
    def const_sql_6_102 = """select "99999999999999999999999999999999999998.09999999999999999999999999999999999999", cast(cast("99999999999999999999999999999999999998.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_102_strict "${const_sql_6_102}"
    testFoldConst("${const_sql_6_102}")
    def const_sql_6_103 = """select "-99999999999999999999999999999999999998.09999999999999999999999999999999999999", cast(cast("-99999999999999999999999999999999999998.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_103_strict "${const_sql_6_103}"
    testFoldConst("${const_sql_6_103}")
    def const_sql_6_104 = """select "99999999999999999999999999999999999998.90000000000000000000000000000000000000", cast(cast("99999999999999999999999999999999999998.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_104_strict "${const_sql_6_104}"
    testFoldConst("${const_sql_6_104}")
    def const_sql_6_105 = """select "-99999999999999999999999999999999999998.90000000000000000000000000000000000000", cast(cast("-99999999999999999999999999999999999998.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_105_strict "${const_sql_6_105}"
    testFoldConst("${const_sql_6_105}")
    def const_sql_6_106 = """select "99999999999999999999999999999999999998.90000000000000000000000000000000000001", cast(cast("99999999999999999999999999999999999998.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_106_strict "${const_sql_6_106}"
    testFoldConst("${const_sql_6_106}")
    def const_sql_6_107 = """select "-99999999999999999999999999999999999998.90000000000000000000000000000000000001", cast(cast("-99999999999999999999999999999999999998.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_107_strict "${const_sql_6_107}"
    testFoldConst("${const_sql_6_107}")
    def const_sql_6_108 = """select "99999999999999999999999999999999999998.99999999999999999999999999999999999998", cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_108_strict "${const_sql_6_108}"
    testFoldConst("${const_sql_6_108}")
    def const_sql_6_109 = """select "-99999999999999999999999999999999999998.99999999999999999999999999999999999998", cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_109_strict "${const_sql_6_109}"
    testFoldConst("${const_sql_6_109}")
    def const_sql_6_110 = """select "99999999999999999999999999999999999998.99999999999999999999999999999999999999", cast(cast("99999999999999999999999999999999999998.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_110_strict "${const_sql_6_110}"
    testFoldConst("${const_sql_6_110}")
    def const_sql_6_111 = """select "-99999999999999999999999999999999999998.99999999999999999999999999999999999999", cast(cast("-99999999999999999999999999999999999998.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_111_strict "${const_sql_6_111}"
    testFoldConst("${const_sql_6_111}")
    def const_sql_6_112 = """select "99999999999999999999999999999999999999.00000000000000000000000000000000000000", cast(cast("99999999999999999999999999999999999999.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_112_strict "${const_sql_6_112}"
    testFoldConst("${const_sql_6_112}")
    def const_sql_6_113 = """select "-99999999999999999999999999999999999999.00000000000000000000000000000000000000", cast(cast("-99999999999999999999999999999999999999.00000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_113_strict "${const_sql_6_113}"
    testFoldConst("${const_sql_6_113}")
    def const_sql_6_114 = """select "99999999999999999999999999999999999999.00000000000000000000000000000000000001", cast(cast("99999999999999999999999999999999999999.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_114_strict "${const_sql_6_114}"
    testFoldConst("${const_sql_6_114}")
    def const_sql_6_115 = """select "-99999999999999999999999999999999999999.00000000000000000000000000000000000001", cast(cast("-99999999999999999999999999999999999999.00000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_115_strict "${const_sql_6_115}"
    testFoldConst("${const_sql_6_115}")
    def const_sql_6_116 = """select "99999999999999999999999999999999999999.00000000000000000000000000000000000009", cast(cast("99999999999999999999999999999999999999.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_116_strict "${const_sql_6_116}"
    testFoldConst("${const_sql_6_116}")
    def const_sql_6_117 = """select "-99999999999999999999999999999999999999.00000000000000000000000000000000000009", cast(cast("-99999999999999999999999999999999999999.00000000000000000000000000000000000009" as decimalv3(76, 38)) as float);"""
    qt_sql_6_117_strict "${const_sql_6_117}"
    testFoldConst("${const_sql_6_117}")
    def const_sql_6_118 = """select "99999999999999999999999999999999999999.09999999999999999999999999999999999999", cast(cast("99999999999999999999999999999999999999.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_118_strict "${const_sql_6_118}"
    testFoldConst("${const_sql_6_118}")
    def const_sql_6_119 = """select "-99999999999999999999999999999999999999.09999999999999999999999999999999999999", cast(cast("-99999999999999999999999999999999999999.09999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_119_strict "${const_sql_6_119}"
    testFoldConst("${const_sql_6_119}")
    def const_sql_6_120 = """select "99999999999999999999999999999999999999.90000000000000000000000000000000000000", cast(cast("99999999999999999999999999999999999999.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_120_strict "${const_sql_6_120}"
    testFoldConst("${const_sql_6_120}")
    def const_sql_6_121 = """select "-99999999999999999999999999999999999999.90000000000000000000000000000000000000", cast(cast("-99999999999999999999999999999999999999.90000000000000000000000000000000000000" as decimalv3(76, 38)) as float);"""
    qt_sql_6_121_strict "${const_sql_6_121}"
    testFoldConst("${const_sql_6_121}")
    def const_sql_6_122 = """select "99999999999999999999999999999999999999.90000000000000000000000000000000000001", cast(cast("99999999999999999999999999999999999999.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_122_strict "${const_sql_6_122}"
    testFoldConst("${const_sql_6_122}")
    def const_sql_6_123 = """select "-99999999999999999999999999999999999999.90000000000000000000000000000000000001", cast(cast("-99999999999999999999999999999999999999.90000000000000000000000000000000000001" as decimalv3(76, 38)) as float);"""
    qt_sql_6_123_strict "${const_sql_6_123}"
    testFoldConst("${const_sql_6_123}")
    def const_sql_6_124 = """select "99999999999999999999999999999999999999.99999999999999999999999999999999999998", cast(cast("99999999999999999999999999999999999999.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_124_strict "${const_sql_6_124}"
    testFoldConst("${const_sql_6_124}")
    def const_sql_6_125 = """select "-99999999999999999999999999999999999999.99999999999999999999999999999999999998", cast(cast("-99999999999999999999999999999999999999.99999999999999999999999999999999999998" as decimalv3(76, 38)) as float);"""
    qt_sql_6_125_strict "${const_sql_6_125}"
    testFoldConst("${const_sql_6_125}")
    def const_sql_6_126 = """select "99999999999999999999999999999999999999.99999999999999999999999999999999999999", cast(cast("99999999999999999999999999999999999999.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_126_strict "${const_sql_6_126}"
    testFoldConst("${const_sql_6_126}")
    def const_sql_6_127 = """select "-99999999999999999999999999999999999999.99999999999999999999999999999999999999", cast(cast("-99999999999999999999999999999999999999.99999999999999999999999999999999999999" as decimalv3(76, 38)) as float);"""
    qt_sql_6_127_strict "${const_sql_6_127}"
    testFoldConst("${const_sql_6_127}")

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
    qt_sql_6_16_non_strict "${const_sql_6_16}"
    testFoldConst("${const_sql_6_16}")
    qt_sql_6_17_non_strict "${const_sql_6_17}"
    testFoldConst("${const_sql_6_17}")
    qt_sql_6_18_non_strict "${const_sql_6_18}"
    testFoldConst("${const_sql_6_18}")
    qt_sql_6_19_non_strict "${const_sql_6_19}"
    testFoldConst("${const_sql_6_19}")
    qt_sql_6_20_non_strict "${const_sql_6_20}"
    testFoldConst("${const_sql_6_20}")
    qt_sql_6_21_non_strict "${const_sql_6_21}"
    testFoldConst("${const_sql_6_21}")
    qt_sql_6_22_non_strict "${const_sql_6_22}"
    testFoldConst("${const_sql_6_22}")
    qt_sql_6_23_non_strict "${const_sql_6_23}"
    testFoldConst("${const_sql_6_23}")
    qt_sql_6_24_non_strict "${const_sql_6_24}"
    testFoldConst("${const_sql_6_24}")
    qt_sql_6_25_non_strict "${const_sql_6_25}"
    testFoldConst("${const_sql_6_25}")
    qt_sql_6_26_non_strict "${const_sql_6_26}"
    testFoldConst("${const_sql_6_26}")
    qt_sql_6_27_non_strict "${const_sql_6_27}"
    testFoldConst("${const_sql_6_27}")
    qt_sql_6_28_non_strict "${const_sql_6_28}"
    testFoldConst("${const_sql_6_28}")
    qt_sql_6_29_non_strict "${const_sql_6_29}"
    testFoldConst("${const_sql_6_29}")
    qt_sql_6_30_non_strict "${const_sql_6_30}"
    testFoldConst("${const_sql_6_30}")
    qt_sql_6_31_non_strict "${const_sql_6_31}"
    testFoldConst("${const_sql_6_31}")
    qt_sql_6_32_non_strict "${const_sql_6_32}"
    testFoldConst("${const_sql_6_32}")
    qt_sql_6_33_non_strict "${const_sql_6_33}"
    testFoldConst("${const_sql_6_33}")
    qt_sql_6_34_non_strict "${const_sql_6_34}"
    testFoldConst("${const_sql_6_34}")
    qt_sql_6_35_non_strict "${const_sql_6_35}"
    testFoldConst("${const_sql_6_35}")
    qt_sql_6_36_non_strict "${const_sql_6_36}"
    testFoldConst("${const_sql_6_36}")
    qt_sql_6_37_non_strict "${const_sql_6_37}"
    testFoldConst("${const_sql_6_37}")
    qt_sql_6_38_non_strict "${const_sql_6_38}"
    testFoldConst("${const_sql_6_38}")
    qt_sql_6_39_non_strict "${const_sql_6_39}"
    testFoldConst("${const_sql_6_39}")
    qt_sql_6_40_non_strict "${const_sql_6_40}"
    testFoldConst("${const_sql_6_40}")
    qt_sql_6_41_non_strict "${const_sql_6_41}"
    testFoldConst("${const_sql_6_41}")
    qt_sql_6_42_non_strict "${const_sql_6_42}"
    testFoldConst("${const_sql_6_42}")
    qt_sql_6_43_non_strict "${const_sql_6_43}"
    testFoldConst("${const_sql_6_43}")
    qt_sql_6_44_non_strict "${const_sql_6_44}"
    testFoldConst("${const_sql_6_44}")
    qt_sql_6_45_non_strict "${const_sql_6_45}"
    testFoldConst("${const_sql_6_45}")
    qt_sql_6_46_non_strict "${const_sql_6_46}"
    testFoldConst("${const_sql_6_46}")
    qt_sql_6_47_non_strict "${const_sql_6_47}"
    testFoldConst("${const_sql_6_47}")
    qt_sql_6_48_non_strict "${const_sql_6_48}"
    testFoldConst("${const_sql_6_48}")
    qt_sql_6_49_non_strict "${const_sql_6_49}"
    testFoldConst("${const_sql_6_49}")
    qt_sql_6_50_non_strict "${const_sql_6_50}"
    testFoldConst("${const_sql_6_50}")
    qt_sql_6_51_non_strict "${const_sql_6_51}"
    testFoldConst("${const_sql_6_51}")
    qt_sql_6_52_non_strict "${const_sql_6_52}"
    testFoldConst("${const_sql_6_52}")
    qt_sql_6_53_non_strict "${const_sql_6_53}"
    testFoldConst("${const_sql_6_53}")
    qt_sql_6_54_non_strict "${const_sql_6_54}"
    testFoldConst("${const_sql_6_54}")
    qt_sql_6_55_non_strict "${const_sql_6_55}"
    testFoldConst("${const_sql_6_55}")
    qt_sql_6_56_non_strict "${const_sql_6_56}"
    testFoldConst("${const_sql_6_56}")
    qt_sql_6_57_non_strict "${const_sql_6_57}"
    testFoldConst("${const_sql_6_57}")
    qt_sql_6_58_non_strict "${const_sql_6_58}"
    testFoldConst("${const_sql_6_58}")
    qt_sql_6_59_non_strict "${const_sql_6_59}"
    testFoldConst("${const_sql_6_59}")
    qt_sql_6_60_non_strict "${const_sql_6_60}"
    testFoldConst("${const_sql_6_60}")
    qt_sql_6_61_non_strict "${const_sql_6_61}"
    testFoldConst("${const_sql_6_61}")
    qt_sql_6_62_non_strict "${const_sql_6_62}"
    testFoldConst("${const_sql_6_62}")
    qt_sql_6_63_non_strict "${const_sql_6_63}"
    testFoldConst("${const_sql_6_63}")
    qt_sql_6_64_non_strict "${const_sql_6_64}"
    testFoldConst("${const_sql_6_64}")
    qt_sql_6_65_non_strict "${const_sql_6_65}"
    testFoldConst("${const_sql_6_65}")
    qt_sql_6_66_non_strict "${const_sql_6_66}"
    testFoldConst("${const_sql_6_66}")
    qt_sql_6_67_non_strict "${const_sql_6_67}"
    testFoldConst("${const_sql_6_67}")
    qt_sql_6_68_non_strict "${const_sql_6_68}"
    testFoldConst("${const_sql_6_68}")
    qt_sql_6_69_non_strict "${const_sql_6_69}"
    testFoldConst("${const_sql_6_69}")
    qt_sql_6_70_non_strict "${const_sql_6_70}"
    testFoldConst("${const_sql_6_70}")
    qt_sql_6_71_non_strict "${const_sql_6_71}"
    testFoldConst("${const_sql_6_71}")
    qt_sql_6_72_non_strict "${const_sql_6_72}"
    testFoldConst("${const_sql_6_72}")
    qt_sql_6_73_non_strict "${const_sql_6_73}"
    testFoldConst("${const_sql_6_73}")
    qt_sql_6_74_non_strict "${const_sql_6_74}"
    testFoldConst("${const_sql_6_74}")
    qt_sql_6_75_non_strict "${const_sql_6_75}"
    testFoldConst("${const_sql_6_75}")
    qt_sql_6_76_non_strict "${const_sql_6_76}"
    testFoldConst("${const_sql_6_76}")
    qt_sql_6_77_non_strict "${const_sql_6_77}"
    testFoldConst("${const_sql_6_77}")
    qt_sql_6_78_non_strict "${const_sql_6_78}"
    testFoldConst("${const_sql_6_78}")
    qt_sql_6_79_non_strict "${const_sql_6_79}"
    testFoldConst("${const_sql_6_79}")
    qt_sql_6_80_non_strict "${const_sql_6_80}"
    testFoldConst("${const_sql_6_80}")
    qt_sql_6_81_non_strict "${const_sql_6_81}"
    testFoldConst("${const_sql_6_81}")
    qt_sql_6_82_non_strict "${const_sql_6_82}"
    testFoldConst("${const_sql_6_82}")
    qt_sql_6_83_non_strict "${const_sql_6_83}"
    testFoldConst("${const_sql_6_83}")
    qt_sql_6_84_non_strict "${const_sql_6_84}"
    testFoldConst("${const_sql_6_84}")
    qt_sql_6_85_non_strict "${const_sql_6_85}"
    testFoldConst("${const_sql_6_85}")
    qt_sql_6_86_non_strict "${const_sql_6_86}"
    testFoldConst("${const_sql_6_86}")
    qt_sql_6_87_non_strict "${const_sql_6_87}"
    testFoldConst("${const_sql_6_87}")
    qt_sql_6_88_non_strict "${const_sql_6_88}"
    testFoldConst("${const_sql_6_88}")
    qt_sql_6_89_non_strict "${const_sql_6_89}"
    testFoldConst("${const_sql_6_89}")
    qt_sql_6_90_non_strict "${const_sql_6_90}"
    testFoldConst("${const_sql_6_90}")
    qt_sql_6_91_non_strict "${const_sql_6_91}"
    testFoldConst("${const_sql_6_91}")
    qt_sql_6_92_non_strict "${const_sql_6_92}"
    testFoldConst("${const_sql_6_92}")
    qt_sql_6_93_non_strict "${const_sql_6_93}"
    testFoldConst("${const_sql_6_93}")
    qt_sql_6_94_non_strict "${const_sql_6_94}"
    testFoldConst("${const_sql_6_94}")
    qt_sql_6_95_non_strict "${const_sql_6_95}"
    testFoldConst("${const_sql_6_95}")
    qt_sql_6_96_non_strict "${const_sql_6_96}"
    testFoldConst("${const_sql_6_96}")
    qt_sql_6_97_non_strict "${const_sql_6_97}"
    testFoldConst("${const_sql_6_97}")
    qt_sql_6_98_non_strict "${const_sql_6_98}"
    testFoldConst("${const_sql_6_98}")
    qt_sql_6_99_non_strict "${const_sql_6_99}"
    testFoldConst("${const_sql_6_99}")
    qt_sql_6_100_non_strict "${const_sql_6_100}"
    testFoldConst("${const_sql_6_100}")
    qt_sql_6_101_non_strict "${const_sql_6_101}"
    testFoldConst("${const_sql_6_101}")
    qt_sql_6_102_non_strict "${const_sql_6_102}"
    testFoldConst("${const_sql_6_102}")
    qt_sql_6_103_non_strict "${const_sql_6_103}"
    testFoldConst("${const_sql_6_103}")
    qt_sql_6_104_non_strict "${const_sql_6_104}"
    testFoldConst("${const_sql_6_104}")
    qt_sql_6_105_non_strict "${const_sql_6_105}"
    testFoldConst("${const_sql_6_105}")
    qt_sql_6_106_non_strict "${const_sql_6_106}"
    testFoldConst("${const_sql_6_106}")
    qt_sql_6_107_non_strict "${const_sql_6_107}"
    testFoldConst("${const_sql_6_107}")
    qt_sql_6_108_non_strict "${const_sql_6_108}"
    testFoldConst("${const_sql_6_108}")
    qt_sql_6_109_non_strict "${const_sql_6_109}"
    testFoldConst("${const_sql_6_109}")
    qt_sql_6_110_non_strict "${const_sql_6_110}"
    testFoldConst("${const_sql_6_110}")
    qt_sql_6_111_non_strict "${const_sql_6_111}"
    testFoldConst("${const_sql_6_111}")
    qt_sql_6_112_non_strict "${const_sql_6_112}"
    testFoldConst("${const_sql_6_112}")
    qt_sql_6_113_non_strict "${const_sql_6_113}"
    testFoldConst("${const_sql_6_113}")
    qt_sql_6_114_non_strict "${const_sql_6_114}"
    testFoldConst("${const_sql_6_114}")
    qt_sql_6_115_non_strict "${const_sql_6_115}"
    testFoldConst("${const_sql_6_115}")
    qt_sql_6_116_non_strict "${const_sql_6_116}"
    testFoldConst("${const_sql_6_116}")
    qt_sql_6_117_non_strict "${const_sql_6_117}"
    testFoldConst("${const_sql_6_117}")
    qt_sql_6_118_non_strict "${const_sql_6_118}"
    testFoldConst("${const_sql_6_118}")
    qt_sql_6_119_non_strict "${const_sql_6_119}"
    testFoldConst("${const_sql_6_119}")
    qt_sql_6_120_non_strict "${const_sql_6_120}"
    testFoldConst("${const_sql_6_120}")
    qt_sql_6_121_non_strict "${const_sql_6_121}"
    testFoldConst("${const_sql_6_121}")
    qt_sql_6_122_non_strict "${const_sql_6_122}"
    testFoldConst("${const_sql_6_122}")
    qt_sql_6_123_non_strict "${const_sql_6_123}"
    testFoldConst("${const_sql_6_123}")
    qt_sql_6_124_non_strict "${const_sql_6_124}"
    testFoldConst("${const_sql_6_124}")
    qt_sql_6_125_non_strict "${const_sql_6_125}"
    testFoldConst("${const_sql_6_125}")
    qt_sql_6_126_non_strict "${const_sql_6_126}"
    testFoldConst("${const_sql_6_126}")
    qt_sql_6_127_non_strict "${const_sql_6_127}"
    testFoldConst("${const_sql_6_127}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_0_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    def const_sql_7_1 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_1_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    def const_sql_7_2 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_2_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    def const_sql_7_3 = """select "-0.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-0.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_3_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    def const_sql_7_4 = """select "0.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("0.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_4_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    def const_sql_7_5 = """select "-0.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("-0.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_5_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    def const_sql_7_6 = """select "0.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_6_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    def const_sql_7_7 = """select "-0.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-0.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_7_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    def const_sql_7_8 = """select "0.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_8_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    def const_sql_7_9 = """select "-0.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-0.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_9_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    def const_sql_7_10 = """select "0.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_10_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    def const_sql_7_11 = """select "-0.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-0.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_11_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    def const_sql_7_12 = """select "0.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("0.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_12_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    def const_sql_7_13 = """select "-0.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-0.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_13_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    def const_sql_7_14 = """select "0.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_14_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    def const_sql_7_15 = """select "-0.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-0.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_15_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    def const_sql_7_16 = """select "1.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("1.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_16_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    def const_sql_7_17 = """select "-1.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-1.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_17_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    def const_sql_7_18 = """select "1.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("1.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_18_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    def const_sql_7_19 = """select "-1.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-1.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_19_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    def const_sql_7_20 = """select "1.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("1.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_20_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    def const_sql_7_21 = """select "-1.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("-1.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_21_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    def const_sql_7_22 = """select "1.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("1.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_22_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    def const_sql_7_23 = """select "-1.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-1.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_23_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")
    def const_sql_7_24 = """select "1.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("1.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_24_strict "${const_sql_7_24}"
    testFoldConst("${const_sql_7_24}")
    def const_sql_7_25 = """select "-1.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-1.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_25_strict "${const_sql_7_25}"
    testFoldConst("${const_sql_7_25}")
    def const_sql_7_26 = """select "1.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("1.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_26_strict "${const_sql_7_26}"
    testFoldConst("${const_sql_7_26}")
    def const_sql_7_27 = """select "-1.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-1.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_27_strict "${const_sql_7_27}"
    testFoldConst("${const_sql_7_27}")
    def const_sql_7_28 = """select "1.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("1.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_28_strict "${const_sql_7_28}"
    testFoldConst("${const_sql_7_28}")
    def const_sql_7_29 = """select "-1.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-1.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_29_strict "${const_sql_7_29}"
    testFoldConst("${const_sql_7_29}")
    def const_sql_7_30 = """select "1.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("1.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_30_strict "${const_sql_7_30}"
    testFoldConst("${const_sql_7_30}")
    def const_sql_7_31 = """select "-1.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-1.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_31_strict "${const_sql_7_31}"
    testFoldConst("${const_sql_7_31}")
    def const_sql_7_32 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_32_strict "${const_sql_7_32}"
    testFoldConst("${const_sql_7_32}")
    def const_sql_7_33 = """select "-8.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-8.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_33_strict "${const_sql_7_33}"
    testFoldConst("${const_sql_7_33}")
    def const_sql_7_34 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_34_strict "${const_sql_7_34}"
    testFoldConst("${const_sql_7_34}")
    def const_sql_7_35 = """select "-8.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-8.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_35_strict "${const_sql_7_35}"
    testFoldConst("${const_sql_7_35}")
    def const_sql_7_36 = """select "8.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("8.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_36_strict "${const_sql_7_36}"
    testFoldConst("${const_sql_7_36}")
    def const_sql_7_37 = """select "-8.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("-8.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_37_strict "${const_sql_7_37}"
    testFoldConst("${const_sql_7_37}")
    def const_sql_7_38 = """select "8.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("8.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_38_strict "${const_sql_7_38}"
    testFoldConst("${const_sql_7_38}")
    def const_sql_7_39 = """select "-8.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-8.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_39_strict "${const_sql_7_39}"
    testFoldConst("${const_sql_7_39}")
    def const_sql_7_40 = """select "8.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("8.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_40_strict "${const_sql_7_40}"
    testFoldConst("${const_sql_7_40}")
    def const_sql_7_41 = """select "-8.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-8.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_41_strict "${const_sql_7_41}"
    testFoldConst("${const_sql_7_41}")
    def const_sql_7_42 = """select "8.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("8.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_42_strict "${const_sql_7_42}"
    testFoldConst("${const_sql_7_42}")
    def const_sql_7_43 = """select "-8.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-8.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_43_strict "${const_sql_7_43}"
    testFoldConst("${const_sql_7_43}")
    def const_sql_7_44 = """select "8.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("8.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_44_strict "${const_sql_7_44}"
    testFoldConst("${const_sql_7_44}")
    def const_sql_7_45 = """select "-8.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-8.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_45_strict "${const_sql_7_45}"
    testFoldConst("${const_sql_7_45}")
    def const_sql_7_46 = """select "8.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("8.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_46_strict "${const_sql_7_46}"
    testFoldConst("${const_sql_7_46}")
    def const_sql_7_47 = """select "-8.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-8.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_47_strict "${const_sql_7_47}"
    testFoldConst("${const_sql_7_47}")
    def const_sql_7_48 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_48_strict "${const_sql_7_48}"
    testFoldConst("${const_sql_7_48}")
    def const_sql_7_49 = """select "-9.000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-9.000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_49_strict "${const_sql_7_49}"
    testFoldConst("${const_sql_7_49}")
    def const_sql_7_50 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_50_strict "${const_sql_7_50}"
    testFoldConst("${const_sql_7_50}")
    def const_sql_7_51 = """select "-9.000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-9.000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_51_strict "${const_sql_7_51}"
    testFoldConst("${const_sql_7_51}")
    def const_sql_7_52 = """select "9.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("9.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_52_strict "${const_sql_7_52}"
    testFoldConst("${const_sql_7_52}")
    def const_sql_7_53 = """select "-9.000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("-9.000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 75)) as float);"""
    qt_sql_7_53_strict "${const_sql_7_53}"
    testFoldConst("${const_sql_7_53}")
    def const_sql_7_54 = """select "9.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_54_strict "${const_sql_7_54}"
    testFoldConst("${const_sql_7_54}")
    def const_sql_7_55 = """select "-9.099999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-9.099999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_55_strict "${const_sql_7_55}"
    testFoldConst("${const_sql_7_55}")
    def const_sql_7_56 = """select "9.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("9.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_56_strict "${const_sql_7_56}"
    testFoldConst("${const_sql_7_56}")
    def const_sql_7_57 = """select "-9.900000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-9.900000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 75)) as float);"""
    qt_sql_7_57_strict "${const_sql_7_57}"
    testFoldConst("${const_sql_7_57}")
    def const_sql_7_58 = """select "9.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("9.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_58_strict "${const_sql_7_58}"
    testFoldConst("${const_sql_7_58}")
    def const_sql_7_59 = """select "-9.900000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-9.900000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 75)) as float);"""
    qt_sql_7_59_strict "${const_sql_7_59}"
    testFoldConst("${const_sql_7_59}")
    def const_sql_7_60 = """select "9.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("9.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_60_strict "${const_sql_7_60}"
    testFoldConst("${const_sql_7_60}")
    def const_sql_7_61 = """select "-9.999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-9.999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 75)) as float);"""
    qt_sql_7_61_strict "${const_sql_7_61}"
    testFoldConst("${const_sql_7_61}")
    def const_sql_7_62 = """select "9.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("9.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_62_strict "${const_sql_7_62}"
    testFoldConst("${const_sql_7_62}")
    def const_sql_7_63 = """select "-9.999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-9.999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 75)) as float);"""
    qt_sql_7_63_strict "${const_sql_7_63}"
    testFoldConst("${const_sql_7_63}")

    sql "set enable_strict_cast=false;"
    qt_sql_7_0_non_strict "${const_sql_7_0}"
    testFoldConst("${const_sql_7_0}")
    qt_sql_7_1_non_strict "${const_sql_7_1}"
    testFoldConst("${const_sql_7_1}")
    qt_sql_7_2_non_strict "${const_sql_7_2}"
    testFoldConst("${const_sql_7_2}")
    qt_sql_7_3_non_strict "${const_sql_7_3}"
    testFoldConst("${const_sql_7_3}")
    qt_sql_7_4_non_strict "${const_sql_7_4}"
    testFoldConst("${const_sql_7_4}")
    qt_sql_7_5_non_strict "${const_sql_7_5}"
    testFoldConst("${const_sql_7_5}")
    qt_sql_7_6_non_strict "${const_sql_7_6}"
    testFoldConst("${const_sql_7_6}")
    qt_sql_7_7_non_strict "${const_sql_7_7}"
    testFoldConst("${const_sql_7_7}")
    qt_sql_7_8_non_strict "${const_sql_7_8}"
    testFoldConst("${const_sql_7_8}")
    qt_sql_7_9_non_strict "${const_sql_7_9}"
    testFoldConst("${const_sql_7_9}")
    qt_sql_7_10_non_strict "${const_sql_7_10}"
    testFoldConst("${const_sql_7_10}")
    qt_sql_7_11_non_strict "${const_sql_7_11}"
    testFoldConst("${const_sql_7_11}")
    qt_sql_7_12_non_strict "${const_sql_7_12}"
    testFoldConst("${const_sql_7_12}")
    qt_sql_7_13_non_strict "${const_sql_7_13}"
    testFoldConst("${const_sql_7_13}")
    qt_sql_7_14_non_strict "${const_sql_7_14}"
    testFoldConst("${const_sql_7_14}")
    qt_sql_7_15_non_strict "${const_sql_7_15}"
    testFoldConst("${const_sql_7_15}")
    qt_sql_7_16_non_strict "${const_sql_7_16}"
    testFoldConst("${const_sql_7_16}")
    qt_sql_7_17_non_strict "${const_sql_7_17}"
    testFoldConst("${const_sql_7_17}")
    qt_sql_7_18_non_strict "${const_sql_7_18}"
    testFoldConst("${const_sql_7_18}")
    qt_sql_7_19_non_strict "${const_sql_7_19}"
    testFoldConst("${const_sql_7_19}")
    qt_sql_7_20_non_strict "${const_sql_7_20}"
    testFoldConst("${const_sql_7_20}")
    qt_sql_7_21_non_strict "${const_sql_7_21}"
    testFoldConst("${const_sql_7_21}")
    qt_sql_7_22_non_strict "${const_sql_7_22}"
    testFoldConst("${const_sql_7_22}")
    qt_sql_7_23_non_strict "${const_sql_7_23}"
    testFoldConst("${const_sql_7_23}")
    qt_sql_7_24_non_strict "${const_sql_7_24}"
    testFoldConst("${const_sql_7_24}")
    qt_sql_7_25_non_strict "${const_sql_7_25}"
    testFoldConst("${const_sql_7_25}")
    qt_sql_7_26_non_strict "${const_sql_7_26}"
    testFoldConst("${const_sql_7_26}")
    qt_sql_7_27_non_strict "${const_sql_7_27}"
    testFoldConst("${const_sql_7_27}")
    qt_sql_7_28_non_strict "${const_sql_7_28}"
    testFoldConst("${const_sql_7_28}")
    qt_sql_7_29_non_strict "${const_sql_7_29}"
    testFoldConst("${const_sql_7_29}")
    qt_sql_7_30_non_strict "${const_sql_7_30}"
    testFoldConst("${const_sql_7_30}")
    qt_sql_7_31_non_strict "${const_sql_7_31}"
    testFoldConst("${const_sql_7_31}")
    qt_sql_7_32_non_strict "${const_sql_7_32}"
    testFoldConst("${const_sql_7_32}")
    qt_sql_7_33_non_strict "${const_sql_7_33}"
    testFoldConst("${const_sql_7_33}")
    qt_sql_7_34_non_strict "${const_sql_7_34}"
    testFoldConst("${const_sql_7_34}")
    qt_sql_7_35_non_strict "${const_sql_7_35}"
    testFoldConst("${const_sql_7_35}")
    qt_sql_7_36_non_strict "${const_sql_7_36}"
    testFoldConst("${const_sql_7_36}")
    qt_sql_7_37_non_strict "${const_sql_7_37}"
    testFoldConst("${const_sql_7_37}")
    qt_sql_7_38_non_strict "${const_sql_7_38}"
    testFoldConst("${const_sql_7_38}")
    qt_sql_7_39_non_strict "${const_sql_7_39}"
    testFoldConst("${const_sql_7_39}")
    qt_sql_7_40_non_strict "${const_sql_7_40}"
    testFoldConst("${const_sql_7_40}")
    qt_sql_7_41_non_strict "${const_sql_7_41}"
    testFoldConst("${const_sql_7_41}")
    qt_sql_7_42_non_strict "${const_sql_7_42}"
    testFoldConst("${const_sql_7_42}")
    qt_sql_7_43_non_strict "${const_sql_7_43}"
    testFoldConst("${const_sql_7_43}")
    qt_sql_7_44_non_strict "${const_sql_7_44}"
    testFoldConst("${const_sql_7_44}")
    qt_sql_7_45_non_strict "${const_sql_7_45}"
    testFoldConst("${const_sql_7_45}")
    qt_sql_7_46_non_strict "${const_sql_7_46}"
    testFoldConst("${const_sql_7_46}")
    qt_sql_7_47_non_strict "${const_sql_7_47}"
    testFoldConst("${const_sql_7_47}")
    qt_sql_7_48_non_strict "${const_sql_7_48}"
    testFoldConst("${const_sql_7_48}")
    qt_sql_7_49_non_strict "${const_sql_7_49}"
    testFoldConst("${const_sql_7_49}")
    qt_sql_7_50_non_strict "${const_sql_7_50}"
    testFoldConst("${const_sql_7_50}")
    qt_sql_7_51_non_strict "${const_sql_7_51}"
    testFoldConst("${const_sql_7_51}")
    qt_sql_7_52_non_strict "${const_sql_7_52}"
    testFoldConst("${const_sql_7_52}")
    qt_sql_7_53_non_strict "${const_sql_7_53}"
    testFoldConst("${const_sql_7_53}")
    qt_sql_7_54_non_strict "${const_sql_7_54}"
    testFoldConst("${const_sql_7_54}")
    qt_sql_7_55_non_strict "${const_sql_7_55}"
    testFoldConst("${const_sql_7_55}")
    qt_sql_7_56_non_strict "${const_sql_7_56}"
    testFoldConst("${const_sql_7_56}")
    qt_sql_7_57_non_strict "${const_sql_7_57}"
    testFoldConst("${const_sql_7_57}")
    qt_sql_7_58_non_strict "${const_sql_7_58}"
    testFoldConst("${const_sql_7_58}")
    qt_sql_7_59_non_strict "${const_sql_7_59}"
    testFoldConst("${const_sql_7_59}")
    qt_sql_7_60_non_strict "${const_sql_7_60}"
    testFoldConst("${const_sql_7_60}")
    qt_sql_7_61_non_strict "${const_sql_7_61}"
    testFoldConst("${const_sql_7_61}")
    qt_sql_7_62_non_strict "${const_sql_7_62}"
    testFoldConst("${const_sql_7_62}")
    qt_sql_7_63_non_strict "${const_sql_7_63}"
    testFoldConst("${const_sql_7_63}")
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as float);"""
    qt_sql_8_0_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    def const_sql_8_1 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as float);"""
    qt_sql_8_1_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    def const_sql_8_2 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as float);"""
    qt_sql_8_2_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    def const_sql_8_3 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as float);"""
    qt_sql_8_3_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    def const_sql_8_4 = """select "0.0000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("0.0000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 76)) as float);"""
    qt_sql_8_4_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    def const_sql_8_5 = """select "-0.0000000000000000000000000000000000000000000000000000000000000000000000000009", cast(cast("-0.0000000000000000000000000000000000000000000000000000000000000000000000000009" as decimalv3(76, 76)) as float);"""
    qt_sql_8_5_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    def const_sql_8_6 = """select "0.0999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.0999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as float);"""
    qt_sql_8_6_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    def const_sql_8_7 = """select "-0.0999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-0.0999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as float);"""
    qt_sql_8_7_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    def const_sql_8_8 = """select "0.9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("0.9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as float);"""
    qt_sql_8_8_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    def const_sql_8_9 = """select "-0.9000000000000000000000000000000000000000000000000000000000000000000000000000", cast(cast("-0.9000000000000000000000000000000000000000000000000000000000000000000000000000" as decimalv3(76, 76)) as float);"""
    qt_sql_8_9_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    def const_sql_8_10 = """select "0.9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("0.9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as float);"""
    qt_sql_8_10_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    def const_sql_8_11 = """select "-0.9000000000000000000000000000000000000000000000000000000000000000000000000001", cast(cast("-0.9000000000000000000000000000000000000000000000000000000000000000000000000001" as decimalv3(76, 76)) as float);"""
    qt_sql_8_11_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    def const_sql_8_12 = """select "0.9999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("0.9999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 76)) as float);"""
    qt_sql_8_12_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    def const_sql_8_13 = """select "-0.9999999999999999999999999999999999999999999999999999999999999999999999999998", cast(cast("-0.9999999999999999999999999999999999999999999999999999999999999999999999999998" as decimalv3(76, 76)) as float);"""
    qt_sql_8_13_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    def const_sql_8_14 = """select "0.9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("0.9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as float);"""
    qt_sql_8_14_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    def const_sql_8_15 = """select "-0.9999999999999999999999999999999999999999999999999999999999999999999999999999", cast(cast("-0.9999999999999999999999999999999999999999999999999999999999999999999999999999" as decimalv3(76, 76)) as float);"""
    qt_sql_8_15_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")

    sql "set enable_strict_cast=false;"
    qt_sql_8_0_non_strict "${const_sql_8_0}"
    testFoldConst("${const_sql_8_0}")
    qt_sql_8_1_non_strict "${const_sql_8_1}"
    testFoldConst("${const_sql_8_1}")
    qt_sql_8_2_non_strict "${const_sql_8_2}"
    testFoldConst("${const_sql_8_2}")
    qt_sql_8_3_non_strict "${const_sql_8_3}"
    testFoldConst("${const_sql_8_3}")
    qt_sql_8_4_non_strict "${const_sql_8_4}"
    testFoldConst("${const_sql_8_4}")
    qt_sql_8_5_non_strict "${const_sql_8_5}"
    testFoldConst("${const_sql_8_5}")
    qt_sql_8_6_non_strict "${const_sql_8_6}"
    testFoldConst("${const_sql_8_6}")
    qt_sql_8_7_non_strict "${const_sql_8_7}"
    testFoldConst("${const_sql_8_7}")
    qt_sql_8_8_non_strict "${const_sql_8_8}"
    testFoldConst("${const_sql_8_8}")
    qt_sql_8_9_non_strict "${const_sql_8_9}"
    testFoldConst("${const_sql_8_9}")
    qt_sql_8_10_non_strict "${const_sql_8_10}"
    testFoldConst("${const_sql_8_10}")
    qt_sql_8_11_non_strict "${const_sql_8_11}"
    testFoldConst("${const_sql_8_11}")
    qt_sql_8_12_non_strict "${const_sql_8_12}"
    testFoldConst("${const_sql_8_12}")
    qt_sql_8_13_non_strict "${const_sql_8_13}"
    testFoldConst("${const_sql_8_13}")
    qt_sql_8_14_non_strict "${const_sql_8_14}"
    testFoldConst("${const_sql_8_14}")
    qt_sql_8_15_non_strict "${const_sql_8_15}"
    testFoldConst("${const_sql_8_15}")
}