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


suite("test_cast_to_largeint_from_datetimev2_6_part1_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0000-12-01 01:00:59.000000", cast(cast("0000-12-01 01:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_0_strict "${const_sql_1_0}"
    testFoldConst("${const_sql_1_0}")
    def const_sql_1_1 = """select "0000-12-01 01:00:59.000001", cast(cast("0000-12-01 01:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_1_strict "${const_sql_1_1}"
    testFoldConst("${const_sql_1_1}")
    def const_sql_1_2 = """select "0000-12-01 01:00:59.999999", cast(cast("0000-12-01 01:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_2_strict "${const_sql_1_2}"
    testFoldConst("${const_sql_1_2}")
    def const_sql_1_3 = """select "0000-12-01 01:01:00.000000", cast(cast("0000-12-01 01:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_3_strict "${const_sql_1_3}"
    testFoldConst("${const_sql_1_3}")
    def const_sql_1_4 = """select "0000-12-01 01:01:00.000001", cast(cast("0000-12-01 01:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_4_strict "${const_sql_1_4}"
    testFoldConst("${const_sql_1_4}")
    def const_sql_1_5 = """select "0000-12-01 01:01:00.999999", cast(cast("0000-12-01 01:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_5_strict "${const_sql_1_5}"
    testFoldConst("${const_sql_1_5}")
    def const_sql_1_6 = """select "0000-12-01 01:01:01.000000", cast(cast("0000-12-01 01:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_6_strict "${const_sql_1_6}"
    testFoldConst("${const_sql_1_6}")
    def const_sql_1_7 = """select "0000-12-01 01:01:01.000001", cast(cast("0000-12-01 01:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_7_strict "${const_sql_1_7}"
    testFoldConst("${const_sql_1_7}")
    def const_sql_1_8 = """select "0000-12-01 01:01:01.999999", cast(cast("0000-12-01 01:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_8_strict "${const_sql_1_8}"
    testFoldConst("${const_sql_1_8}")
    def const_sql_1_9 = """select "0000-12-01 01:01:59.000000", cast(cast("0000-12-01 01:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_9_strict "${const_sql_1_9}"
    testFoldConst("${const_sql_1_9}")
    def const_sql_1_10 = """select "0000-12-01 01:01:59.000001", cast(cast("0000-12-01 01:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_10_strict "${const_sql_1_10}"
    testFoldConst("${const_sql_1_10}")
    def const_sql_1_11 = """select "0000-12-01 01:01:59.999999", cast(cast("0000-12-01 01:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_11_strict "${const_sql_1_11}"
    testFoldConst("${const_sql_1_11}")
    def const_sql_1_12 = """select "0000-12-01 01:59:00.000000", cast(cast("0000-12-01 01:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_12_strict "${const_sql_1_12}"
    testFoldConst("${const_sql_1_12}")
    def const_sql_1_13 = """select "0000-12-01 01:59:00.000001", cast(cast("0000-12-01 01:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_13_strict "${const_sql_1_13}"
    testFoldConst("${const_sql_1_13}")
    def const_sql_1_14 = """select "0000-12-01 01:59:00.999999", cast(cast("0000-12-01 01:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_14_strict "${const_sql_1_14}"
    testFoldConst("${const_sql_1_14}")
    def const_sql_1_15 = """select "0000-12-01 01:59:01.000000", cast(cast("0000-12-01 01:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_15_strict "${const_sql_1_15}"
    testFoldConst("${const_sql_1_15}")
    def const_sql_1_16 = """select "0000-12-01 01:59:01.000001", cast(cast("0000-12-01 01:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_16_strict "${const_sql_1_16}"
    testFoldConst("${const_sql_1_16}")
    def const_sql_1_17 = """select "0000-12-01 01:59:01.999999", cast(cast("0000-12-01 01:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_17_strict "${const_sql_1_17}"
    testFoldConst("${const_sql_1_17}")
    def const_sql_1_18 = """select "0000-12-01 01:59:59.000000", cast(cast("0000-12-01 01:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_18_strict "${const_sql_1_18}"
    testFoldConst("${const_sql_1_18}")
    def const_sql_1_19 = """select "0000-12-01 01:59:59.000001", cast(cast("0000-12-01 01:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_19_strict "${const_sql_1_19}"
    testFoldConst("${const_sql_1_19}")
    def const_sql_1_20 = """select "0000-12-01 01:59:59.999999", cast(cast("0000-12-01 01:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_20_strict "${const_sql_1_20}"
    testFoldConst("${const_sql_1_20}")
    def const_sql_1_21 = """select "0000-12-01 23:00:00.000000", cast(cast("0000-12-01 23:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_21_strict "${const_sql_1_21}"
    testFoldConst("${const_sql_1_21}")
    def const_sql_1_22 = """select "0000-12-01 23:00:00.000001", cast(cast("0000-12-01 23:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_22_strict "${const_sql_1_22}"
    testFoldConst("${const_sql_1_22}")
    def const_sql_1_23 = """select "0000-12-01 23:00:00.999999", cast(cast("0000-12-01 23:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_23_strict "${const_sql_1_23}"
    testFoldConst("${const_sql_1_23}")
    def const_sql_1_24 = """select "0000-12-01 23:00:01.000000", cast(cast("0000-12-01 23:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_24_strict "${const_sql_1_24}"
    testFoldConst("${const_sql_1_24}")
    def const_sql_1_25 = """select "0000-12-01 23:00:01.000001", cast(cast("0000-12-01 23:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_25_strict "${const_sql_1_25}"
    testFoldConst("${const_sql_1_25}")
    def const_sql_1_26 = """select "0000-12-01 23:00:01.999999", cast(cast("0000-12-01 23:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_26_strict "${const_sql_1_26}"
    testFoldConst("${const_sql_1_26}")
    def const_sql_1_27 = """select "0000-12-01 23:00:59.000000", cast(cast("0000-12-01 23:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_27_strict "${const_sql_1_27}"
    testFoldConst("${const_sql_1_27}")
    def const_sql_1_28 = """select "0000-12-01 23:00:59.000001", cast(cast("0000-12-01 23:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_28_strict "${const_sql_1_28}"
    testFoldConst("${const_sql_1_28}")
    def const_sql_1_29 = """select "0000-12-01 23:00:59.999999", cast(cast("0000-12-01 23:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_29_strict "${const_sql_1_29}"
    testFoldConst("${const_sql_1_29}")
    def const_sql_1_30 = """select "0000-12-01 23:01:00.000000", cast(cast("0000-12-01 23:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_30_strict "${const_sql_1_30}"
    testFoldConst("${const_sql_1_30}")
    def const_sql_1_31 = """select "0000-12-01 23:01:00.000001", cast(cast("0000-12-01 23:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_31_strict "${const_sql_1_31}"
    testFoldConst("${const_sql_1_31}")
    def const_sql_1_32 = """select "0000-12-01 23:01:00.999999", cast(cast("0000-12-01 23:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_32_strict "${const_sql_1_32}"
    testFoldConst("${const_sql_1_32}")
    def const_sql_1_33 = """select "0000-12-01 23:01:01.000000", cast(cast("0000-12-01 23:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_33_strict "${const_sql_1_33}"
    testFoldConst("${const_sql_1_33}")
    def const_sql_1_34 = """select "0000-12-01 23:01:01.000001", cast(cast("0000-12-01 23:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_34_strict "${const_sql_1_34}"
    testFoldConst("${const_sql_1_34}")
    def const_sql_1_35 = """select "0000-12-01 23:01:01.999999", cast(cast("0000-12-01 23:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_35_strict "${const_sql_1_35}"
    testFoldConst("${const_sql_1_35}")
    def const_sql_1_36 = """select "0000-12-01 23:01:59.000000", cast(cast("0000-12-01 23:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_36_strict "${const_sql_1_36}"
    testFoldConst("${const_sql_1_36}")
    def const_sql_1_37 = """select "0000-12-01 23:01:59.000001", cast(cast("0000-12-01 23:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_37_strict "${const_sql_1_37}"
    testFoldConst("${const_sql_1_37}")
    def const_sql_1_38 = """select "0000-12-01 23:01:59.999999", cast(cast("0000-12-01 23:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_38_strict "${const_sql_1_38}"
    testFoldConst("${const_sql_1_38}")
    def const_sql_1_39 = """select "0000-12-01 23:59:00.000000", cast(cast("0000-12-01 23:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_39_strict "${const_sql_1_39}"
    testFoldConst("${const_sql_1_39}")
    def const_sql_1_40 = """select "0000-12-01 23:59:00.000001", cast(cast("0000-12-01 23:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_40_strict "${const_sql_1_40}"
    testFoldConst("${const_sql_1_40}")
    def const_sql_1_41 = """select "0000-12-01 23:59:00.999999", cast(cast("0000-12-01 23:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_41_strict "${const_sql_1_41}"
    testFoldConst("${const_sql_1_41}")
    def const_sql_1_42 = """select "0000-12-01 23:59:01.000000", cast(cast("0000-12-01 23:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_42_strict "${const_sql_1_42}"
    testFoldConst("${const_sql_1_42}")
    def const_sql_1_43 = """select "0000-12-01 23:59:01.000001", cast(cast("0000-12-01 23:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_43_strict "${const_sql_1_43}"
    testFoldConst("${const_sql_1_43}")
    def const_sql_1_44 = """select "0000-12-01 23:59:01.999999", cast(cast("0000-12-01 23:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_44_strict "${const_sql_1_44}"
    testFoldConst("${const_sql_1_44}")
    def const_sql_1_45 = """select "0000-12-01 23:59:59.000000", cast(cast("0000-12-01 23:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_45_strict "${const_sql_1_45}"
    testFoldConst("${const_sql_1_45}")
    def const_sql_1_46 = """select "0000-12-01 23:59:59.000001", cast(cast("0000-12-01 23:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_46_strict "${const_sql_1_46}"
    testFoldConst("${const_sql_1_46}")
    def const_sql_1_47 = """select "0000-12-01 23:59:59.999999", cast(cast("0000-12-01 23:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_47_strict "${const_sql_1_47}"
    testFoldConst("${const_sql_1_47}")
    def const_sql_1_48 = """select "0000-12-28 00:00:00.000000", cast(cast("0000-12-28 00:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_48_strict "${const_sql_1_48}"
    testFoldConst("${const_sql_1_48}")
    def const_sql_1_49 = """select "0000-12-28 00:00:00.000001", cast(cast("0000-12-28 00:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_49_strict "${const_sql_1_49}"
    testFoldConst("${const_sql_1_49}")
    def const_sql_1_50 = """select "0000-12-28 00:00:00.999999", cast(cast("0000-12-28 00:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_50_strict "${const_sql_1_50}"
    testFoldConst("${const_sql_1_50}")
    def const_sql_1_51 = """select "0000-12-28 00:00:01.000000", cast(cast("0000-12-28 00:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_51_strict "${const_sql_1_51}"
    testFoldConst("${const_sql_1_51}")
    def const_sql_1_52 = """select "0000-12-28 00:00:01.000001", cast(cast("0000-12-28 00:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_52_strict "${const_sql_1_52}"
    testFoldConst("${const_sql_1_52}")
    def const_sql_1_53 = """select "0000-12-28 00:00:01.999999", cast(cast("0000-12-28 00:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_53_strict "${const_sql_1_53}"
    testFoldConst("${const_sql_1_53}")
    def const_sql_1_54 = """select "0000-12-28 00:00:59.000000", cast(cast("0000-12-28 00:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_54_strict "${const_sql_1_54}"
    testFoldConst("${const_sql_1_54}")
    def const_sql_1_55 = """select "0000-12-28 00:00:59.000001", cast(cast("0000-12-28 00:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_55_strict "${const_sql_1_55}"
    testFoldConst("${const_sql_1_55}")
    def const_sql_1_56 = """select "0000-12-28 00:00:59.999999", cast(cast("0000-12-28 00:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_56_strict "${const_sql_1_56}"
    testFoldConst("${const_sql_1_56}")
    def const_sql_1_57 = """select "0000-12-28 00:01:00.000000", cast(cast("0000-12-28 00:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_57_strict "${const_sql_1_57}"
    testFoldConst("${const_sql_1_57}")
    def const_sql_1_58 = """select "0000-12-28 00:01:00.000001", cast(cast("0000-12-28 00:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_58_strict "${const_sql_1_58}"
    testFoldConst("${const_sql_1_58}")
    def const_sql_1_59 = """select "0000-12-28 00:01:00.999999", cast(cast("0000-12-28 00:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_59_strict "${const_sql_1_59}"
    testFoldConst("${const_sql_1_59}")
    def const_sql_1_60 = """select "0000-12-28 00:01:01.000000", cast(cast("0000-12-28 00:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_60_strict "${const_sql_1_60}"
    testFoldConst("${const_sql_1_60}")
    def const_sql_1_61 = """select "0000-12-28 00:01:01.000001", cast(cast("0000-12-28 00:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_61_strict "${const_sql_1_61}"
    testFoldConst("${const_sql_1_61}")
    def const_sql_1_62 = """select "0000-12-28 00:01:01.999999", cast(cast("0000-12-28 00:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_62_strict "${const_sql_1_62}"
    testFoldConst("${const_sql_1_62}")
    def const_sql_1_63 = """select "0000-12-28 00:01:59.000000", cast(cast("0000-12-28 00:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_63_strict "${const_sql_1_63}"
    testFoldConst("${const_sql_1_63}")
    def const_sql_1_64 = """select "0000-12-28 00:01:59.000001", cast(cast("0000-12-28 00:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_64_strict "${const_sql_1_64}"
    testFoldConst("${const_sql_1_64}")
    def const_sql_1_65 = """select "0000-12-28 00:01:59.999999", cast(cast("0000-12-28 00:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_65_strict "${const_sql_1_65}"
    testFoldConst("${const_sql_1_65}")
    def const_sql_1_66 = """select "0000-12-28 00:59:00.000000", cast(cast("0000-12-28 00:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_66_strict "${const_sql_1_66}"
    testFoldConst("${const_sql_1_66}")
    def const_sql_1_67 = """select "0000-12-28 00:59:00.000001", cast(cast("0000-12-28 00:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_67_strict "${const_sql_1_67}"
    testFoldConst("${const_sql_1_67}")
    def const_sql_1_68 = """select "0000-12-28 00:59:00.999999", cast(cast("0000-12-28 00:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_68_strict "${const_sql_1_68}"
    testFoldConst("${const_sql_1_68}")
    def const_sql_1_69 = """select "0000-12-28 00:59:01.000000", cast(cast("0000-12-28 00:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_69_strict "${const_sql_1_69}"
    testFoldConst("${const_sql_1_69}")
    def const_sql_1_70 = """select "0000-12-28 00:59:01.000001", cast(cast("0000-12-28 00:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_70_strict "${const_sql_1_70}"
    testFoldConst("${const_sql_1_70}")
    def const_sql_1_71 = """select "0000-12-28 00:59:01.999999", cast(cast("0000-12-28 00:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_71_strict "${const_sql_1_71}"
    testFoldConst("${const_sql_1_71}")
    def const_sql_1_72 = """select "0000-12-28 00:59:59.000000", cast(cast("0000-12-28 00:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_72_strict "${const_sql_1_72}"
    testFoldConst("${const_sql_1_72}")
    def const_sql_1_73 = """select "0000-12-28 00:59:59.000001", cast(cast("0000-12-28 00:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_73_strict "${const_sql_1_73}"
    testFoldConst("${const_sql_1_73}")
    def const_sql_1_74 = """select "0000-12-28 00:59:59.999999", cast(cast("0000-12-28 00:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_74_strict "${const_sql_1_74}"
    testFoldConst("${const_sql_1_74}")
    def const_sql_1_75 = """select "0000-12-28 01:00:00.000000", cast(cast("0000-12-28 01:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_75_strict "${const_sql_1_75}"
    testFoldConst("${const_sql_1_75}")
    def const_sql_1_76 = """select "0000-12-28 01:00:00.000001", cast(cast("0000-12-28 01:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_76_strict "${const_sql_1_76}"
    testFoldConst("${const_sql_1_76}")
    def const_sql_1_77 = """select "0000-12-28 01:00:00.999999", cast(cast("0000-12-28 01:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_77_strict "${const_sql_1_77}"
    testFoldConst("${const_sql_1_77}")
    def const_sql_1_78 = """select "0000-12-28 01:00:01.000000", cast(cast("0000-12-28 01:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_78_strict "${const_sql_1_78}"
    testFoldConst("${const_sql_1_78}")
    def const_sql_1_79 = """select "0000-12-28 01:00:01.000001", cast(cast("0000-12-28 01:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_79_strict "${const_sql_1_79}"
    testFoldConst("${const_sql_1_79}")
    def const_sql_1_80 = """select "0000-12-28 01:00:01.999999", cast(cast("0000-12-28 01:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_80_strict "${const_sql_1_80}"
    testFoldConst("${const_sql_1_80}")
    def const_sql_1_81 = """select "0000-12-28 01:00:59.000000", cast(cast("0000-12-28 01:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_81_strict "${const_sql_1_81}"
    testFoldConst("${const_sql_1_81}")
    def const_sql_1_82 = """select "0000-12-28 01:00:59.000001", cast(cast("0000-12-28 01:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_82_strict "${const_sql_1_82}"
    testFoldConst("${const_sql_1_82}")
    def const_sql_1_83 = """select "0000-12-28 01:00:59.999999", cast(cast("0000-12-28 01:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_83_strict "${const_sql_1_83}"
    testFoldConst("${const_sql_1_83}")
    def const_sql_1_84 = """select "0000-12-28 01:01:00.000000", cast(cast("0000-12-28 01:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_84_strict "${const_sql_1_84}"
    testFoldConst("${const_sql_1_84}")
    def const_sql_1_85 = """select "0000-12-28 01:01:00.000001", cast(cast("0000-12-28 01:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_85_strict "${const_sql_1_85}"
    testFoldConst("${const_sql_1_85}")
    def const_sql_1_86 = """select "0000-12-28 01:01:00.999999", cast(cast("0000-12-28 01:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_86_strict "${const_sql_1_86}"
    testFoldConst("${const_sql_1_86}")
    def const_sql_1_87 = """select "0000-12-28 01:01:01.000000", cast(cast("0000-12-28 01:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_87_strict "${const_sql_1_87}"
    testFoldConst("${const_sql_1_87}")
    def const_sql_1_88 = """select "0000-12-28 01:01:01.000001", cast(cast("0000-12-28 01:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_88_strict "${const_sql_1_88}"
    testFoldConst("${const_sql_1_88}")
    def const_sql_1_89 = """select "0000-12-28 01:01:01.999999", cast(cast("0000-12-28 01:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_89_strict "${const_sql_1_89}"
    testFoldConst("${const_sql_1_89}")
    def const_sql_1_90 = """select "0000-12-28 01:01:59.000000", cast(cast("0000-12-28 01:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_90_strict "${const_sql_1_90}"
    testFoldConst("${const_sql_1_90}")
    def const_sql_1_91 = """select "0000-12-28 01:01:59.000001", cast(cast("0000-12-28 01:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_91_strict "${const_sql_1_91}"
    testFoldConst("${const_sql_1_91}")
    def const_sql_1_92 = """select "0000-12-28 01:01:59.999999", cast(cast("0000-12-28 01:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_92_strict "${const_sql_1_92}"
    testFoldConst("${const_sql_1_92}")
    def const_sql_1_93 = """select "0000-12-28 01:59:00.000000", cast(cast("0000-12-28 01:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_93_strict "${const_sql_1_93}"
    testFoldConst("${const_sql_1_93}")
    def const_sql_1_94 = """select "0000-12-28 01:59:00.000001", cast(cast("0000-12-28 01:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_94_strict "${const_sql_1_94}"
    testFoldConst("${const_sql_1_94}")
    def const_sql_1_95 = """select "0000-12-28 01:59:00.999999", cast(cast("0000-12-28 01:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_95_strict "${const_sql_1_95}"
    testFoldConst("${const_sql_1_95}")
    def const_sql_1_96 = """select "0000-12-28 01:59:01.000000", cast(cast("0000-12-28 01:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_96_strict "${const_sql_1_96}"
    testFoldConst("${const_sql_1_96}")
    def const_sql_1_97 = """select "0000-12-28 01:59:01.000001", cast(cast("0000-12-28 01:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_97_strict "${const_sql_1_97}"
    testFoldConst("${const_sql_1_97}")
    def const_sql_1_98 = """select "0000-12-28 01:59:01.999999", cast(cast("0000-12-28 01:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_98_strict "${const_sql_1_98}"
    testFoldConst("${const_sql_1_98}")
    def const_sql_1_99 = """select "0000-12-28 01:59:59.000000", cast(cast("0000-12-28 01:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_99_strict "${const_sql_1_99}"
    testFoldConst("${const_sql_1_99}")
    def const_sql_1_100 = """select "0000-12-28 01:59:59.000001", cast(cast("0000-12-28 01:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_100_strict "${const_sql_1_100}"
    testFoldConst("${const_sql_1_100}")
    def const_sql_1_101 = """select "0000-12-28 01:59:59.999999", cast(cast("0000-12-28 01:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_101_strict "${const_sql_1_101}"
    testFoldConst("${const_sql_1_101}")
    def const_sql_1_102 = """select "0000-12-28 23:00:00.000000", cast(cast("0000-12-28 23:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_102_strict "${const_sql_1_102}"
    testFoldConst("${const_sql_1_102}")
    def const_sql_1_103 = """select "0000-12-28 23:00:00.000001", cast(cast("0000-12-28 23:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_103_strict "${const_sql_1_103}"
    testFoldConst("${const_sql_1_103}")
    def const_sql_1_104 = """select "0000-12-28 23:00:00.999999", cast(cast("0000-12-28 23:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_104_strict "${const_sql_1_104}"
    testFoldConst("${const_sql_1_104}")
    def const_sql_1_105 = """select "0000-12-28 23:00:01.000000", cast(cast("0000-12-28 23:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_105_strict "${const_sql_1_105}"
    testFoldConst("${const_sql_1_105}")
    def const_sql_1_106 = """select "0000-12-28 23:00:01.000001", cast(cast("0000-12-28 23:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_106_strict "${const_sql_1_106}"
    testFoldConst("${const_sql_1_106}")
    def const_sql_1_107 = """select "0000-12-28 23:00:01.999999", cast(cast("0000-12-28 23:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_107_strict "${const_sql_1_107}"
    testFoldConst("${const_sql_1_107}")
    def const_sql_1_108 = """select "0000-12-28 23:00:59.000000", cast(cast("0000-12-28 23:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_108_strict "${const_sql_1_108}"
    testFoldConst("${const_sql_1_108}")
    def const_sql_1_109 = """select "0000-12-28 23:00:59.000001", cast(cast("0000-12-28 23:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_109_strict "${const_sql_1_109}"
    testFoldConst("${const_sql_1_109}")
    def const_sql_1_110 = """select "0000-12-28 23:00:59.999999", cast(cast("0000-12-28 23:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_110_strict "${const_sql_1_110}"
    testFoldConst("${const_sql_1_110}")
    def const_sql_1_111 = """select "0000-12-28 23:01:00.000000", cast(cast("0000-12-28 23:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_111_strict "${const_sql_1_111}"
    testFoldConst("${const_sql_1_111}")
    def const_sql_1_112 = """select "0000-12-28 23:01:00.000001", cast(cast("0000-12-28 23:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_112_strict "${const_sql_1_112}"
    testFoldConst("${const_sql_1_112}")
    def const_sql_1_113 = """select "0000-12-28 23:01:00.999999", cast(cast("0000-12-28 23:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_113_strict "${const_sql_1_113}"
    testFoldConst("${const_sql_1_113}")
    def const_sql_1_114 = """select "0000-12-28 23:01:01.000000", cast(cast("0000-12-28 23:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_114_strict "${const_sql_1_114}"
    testFoldConst("${const_sql_1_114}")
    def const_sql_1_115 = """select "0000-12-28 23:01:01.000001", cast(cast("0000-12-28 23:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_115_strict "${const_sql_1_115}"
    testFoldConst("${const_sql_1_115}")
    def const_sql_1_116 = """select "0000-12-28 23:01:01.999999", cast(cast("0000-12-28 23:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_116_strict "${const_sql_1_116}"
    testFoldConst("${const_sql_1_116}")
    def const_sql_1_117 = """select "0000-12-28 23:01:59.000000", cast(cast("0000-12-28 23:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_117_strict "${const_sql_1_117}"
    testFoldConst("${const_sql_1_117}")
    def const_sql_1_118 = """select "0000-12-28 23:01:59.000001", cast(cast("0000-12-28 23:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_118_strict "${const_sql_1_118}"
    testFoldConst("${const_sql_1_118}")
    def const_sql_1_119 = """select "0000-12-28 23:01:59.999999", cast(cast("0000-12-28 23:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_119_strict "${const_sql_1_119}"
    testFoldConst("${const_sql_1_119}")
    def const_sql_1_120 = """select "0000-12-28 23:59:00.000000", cast(cast("0000-12-28 23:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_120_strict "${const_sql_1_120}"
    testFoldConst("${const_sql_1_120}")
    def const_sql_1_121 = """select "0000-12-28 23:59:00.000001", cast(cast("0000-12-28 23:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_121_strict "${const_sql_1_121}"
    testFoldConst("${const_sql_1_121}")
    def const_sql_1_122 = """select "0000-12-28 23:59:00.999999", cast(cast("0000-12-28 23:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_122_strict "${const_sql_1_122}"
    testFoldConst("${const_sql_1_122}")
    def const_sql_1_123 = """select "0000-12-28 23:59:01.000000", cast(cast("0000-12-28 23:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_123_strict "${const_sql_1_123}"
    testFoldConst("${const_sql_1_123}")
    def const_sql_1_124 = """select "0000-12-28 23:59:01.000001", cast(cast("0000-12-28 23:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_124_strict "${const_sql_1_124}"
    testFoldConst("${const_sql_1_124}")
    def const_sql_1_125 = """select "0000-12-28 23:59:01.999999", cast(cast("0000-12-28 23:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_125_strict "${const_sql_1_125}"
    testFoldConst("${const_sql_1_125}")
    def const_sql_1_126 = """select "0000-12-28 23:59:59.000000", cast(cast("0000-12-28 23:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_126_strict "${const_sql_1_126}"
    testFoldConst("${const_sql_1_126}")
    def const_sql_1_127 = """select "0000-12-28 23:59:59.000001", cast(cast("0000-12-28 23:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_127_strict "${const_sql_1_127}"
    testFoldConst("${const_sql_1_127}")
    def const_sql_1_128 = """select "0000-12-28 23:59:59.999999", cast(cast("0000-12-28 23:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_128_strict "${const_sql_1_128}"
    testFoldConst("${const_sql_1_128}")
    def const_sql_1_129 = """select "0001-01-01 00:00:00.000000", cast(cast("0001-01-01 00:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_129_strict "${const_sql_1_129}"
    testFoldConst("${const_sql_1_129}")
    def const_sql_1_130 = """select "0001-01-01 00:00:00.000001", cast(cast("0001-01-01 00:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_130_strict "${const_sql_1_130}"
    testFoldConst("${const_sql_1_130}")
    def const_sql_1_131 = """select "0001-01-01 00:00:00.999999", cast(cast("0001-01-01 00:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_131_strict "${const_sql_1_131}"
    testFoldConst("${const_sql_1_131}")
    def const_sql_1_132 = """select "0001-01-01 00:00:01.000000", cast(cast("0001-01-01 00:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_132_strict "${const_sql_1_132}"
    testFoldConst("${const_sql_1_132}")
    def const_sql_1_133 = """select "0001-01-01 00:00:01.000001", cast(cast("0001-01-01 00:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_133_strict "${const_sql_1_133}"
    testFoldConst("${const_sql_1_133}")
    def const_sql_1_134 = """select "0001-01-01 00:00:01.999999", cast(cast("0001-01-01 00:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_134_strict "${const_sql_1_134}"
    testFoldConst("${const_sql_1_134}")
    def const_sql_1_135 = """select "0001-01-01 00:00:59.000000", cast(cast("0001-01-01 00:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_135_strict "${const_sql_1_135}"
    testFoldConst("${const_sql_1_135}")
    def const_sql_1_136 = """select "0001-01-01 00:00:59.000001", cast(cast("0001-01-01 00:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_136_strict "${const_sql_1_136}"
    testFoldConst("${const_sql_1_136}")
    def const_sql_1_137 = """select "0001-01-01 00:00:59.999999", cast(cast("0001-01-01 00:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_137_strict "${const_sql_1_137}"
    testFoldConst("${const_sql_1_137}")
    def const_sql_1_138 = """select "0001-01-01 00:01:00.000000", cast(cast("0001-01-01 00:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_138_strict "${const_sql_1_138}"
    testFoldConst("${const_sql_1_138}")
    def const_sql_1_139 = """select "0001-01-01 00:01:00.000001", cast(cast("0001-01-01 00:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_139_strict "${const_sql_1_139}"
    testFoldConst("${const_sql_1_139}")
    def const_sql_1_140 = """select "0001-01-01 00:01:00.999999", cast(cast("0001-01-01 00:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_140_strict "${const_sql_1_140}"
    testFoldConst("${const_sql_1_140}")
    def const_sql_1_141 = """select "0001-01-01 00:01:01.000000", cast(cast("0001-01-01 00:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_141_strict "${const_sql_1_141}"
    testFoldConst("${const_sql_1_141}")
    def const_sql_1_142 = """select "0001-01-01 00:01:01.000001", cast(cast("0001-01-01 00:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_142_strict "${const_sql_1_142}"
    testFoldConst("${const_sql_1_142}")
    def const_sql_1_143 = """select "0001-01-01 00:01:01.999999", cast(cast("0001-01-01 00:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_143_strict "${const_sql_1_143}"
    testFoldConst("${const_sql_1_143}")
    def const_sql_1_144 = """select "0001-01-01 00:01:59.000000", cast(cast("0001-01-01 00:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_144_strict "${const_sql_1_144}"
    testFoldConst("${const_sql_1_144}")
    def const_sql_1_145 = """select "0001-01-01 00:01:59.000001", cast(cast("0001-01-01 00:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_145_strict "${const_sql_1_145}"
    testFoldConst("${const_sql_1_145}")
    def const_sql_1_146 = """select "0001-01-01 00:01:59.999999", cast(cast("0001-01-01 00:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_146_strict "${const_sql_1_146}"
    testFoldConst("${const_sql_1_146}")
    def const_sql_1_147 = """select "0001-01-01 00:59:00.000000", cast(cast("0001-01-01 00:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_147_strict "${const_sql_1_147}"
    testFoldConst("${const_sql_1_147}")
    def const_sql_1_148 = """select "0001-01-01 00:59:00.000001", cast(cast("0001-01-01 00:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_148_strict "${const_sql_1_148}"
    testFoldConst("${const_sql_1_148}")
    def const_sql_1_149 = """select "0001-01-01 00:59:00.999999", cast(cast("0001-01-01 00:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_149_strict "${const_sql_1_149}"
    testFoldConst("${const_sql_1_149}")
    def const_sql_1_150 = """select "0001-01-01 00:59:01.000000", cast(cast("0001-01-01 00:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_150_strict "${const_sql_1_150}"
    testFoldConst("${const_sql_1_150}")
    def const_sql_1_151 = """select "0001-01-01 00:59:01.000001", cast(cast("0001-01-01 00:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_151_strict "${const_sql_1_151}"
    testFoldConst("${const_sql_1_151}")
    def const_sql_1_152 = """select "0001-01-01 00:59:01.999999", cast(cast("0001-01-01 00:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_152_strict "${const_sql_1_152}"
    testFoldConst("${const_sql_1_152}")
    def const_sql_1_153 = """select "0001-01-01 00:59:59.000000", cast(cast("0001-01-01 00:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_153_strict "${const_sql_1_153}"
    testFoldConst("${const_sql_1_153}")
    def const_sql_1_154 = """select "0001-01-01 00:59:59.000001", cast(cast("0001-01-01 00:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_154_strict "${const_sql_1_154}"
    testFoldConst("${const_sql_1_154}")
    def const_sql_1_155 = """select "0001-01-01 00:59:59.999999", cast(cast("0001-01-01 00:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_155_strict "${const_sql_1_155}"
    testFoldConst("${const_sql_1_155}")
    def const_sql_1_156 = """select "0001-01-01 01:00:00.000000", cast(cast("0001-01-01 01:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_156_strict "${const_sql_1_156}"
    testFoldConst("${const_sql_1_156}")
    def const_sql_1_157 = """select "0001-01-01 01:00:00.000001", cast(cast("0001-01-01 01:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_157_strict "${const_sql_1_157}"
    testFoldConst("${const_sql_1_157}")
    def const_sql_1_158 = """select "0001-01-01 01:00:00.999999", cast(cast("0001-01-01 01:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_158_strict "${const_sql_1_158}"
    testFoldConst("${const_sql_1_158}")
    def const_sql_1_159 = """select "0001-01-01 01:00:01.000000", cast(cast("0001-01-01 01:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_159_strict "${const_sql_1_159}"
    testFoldConst("${const_sql_1_159}")
    def const_sql_1_160 = """select "0001-01-01 01:00:01.000001", cast(cast("0001-01-01 01:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_160_strict "${const_sql_1_160}"
    testFoldConst("${const_sql_1_160}")
    def const_sql_1_161 = """select "0001-01-01 01:00:01.999999", cast(cast("0001-01-01 01:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_161_strict "${const_sql_1_161}"
    testFoldConst("${const_sql_1_161}")
    def const_sql_1_162 = """select "0001-01-01 01:00:59.000000", cast(cast("0001-01-01 01:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_162_strict "${const_sql_1_162}"
    testFoldConst("${const_sql_1_162}")
    def const_sql_1_163 = """select "0001-01-01 01:00:59.000001", cast(cast("0001-01-01 01:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_163_strict "${const_sql_1_163}"
    testFoldConst("${const_sql_1_163}")
    def const_sql_1_164 = """select "0001-01-01 01:00:59.999999", cast(cast("0001-01-01 01:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_164_strict "${const_sql_1_164}"
    testFoldConst("${const_sql_1_164}")
    def const_sql_1_165 = """select "0001-01-01 01:01:00.000000", cast(cast("0001-01-01 01:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_165_strict "${const_sql_1_165}"
    testFoldConst("${const_sql_1_165}")
    def const_sql_1_166 = """select "0001-01-01 01:01:00.000001", cast(cast("0001-01-01 01:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_166_strict "${const_sql_1_166}"
    testFoldConst("${const_sql_1_166}")
    def const_sql_1_167 = """select "0001-01-01 01:01:00.999999", cast(cast("0001-01-01 01:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_167_strict "${const_sql_1_167}"
    testFoldConst("${const_sql_1_167}")
    def const_sql_1_168 = """select "0001-01-01 01:01:01.000000", cast(cast("0001-01-01 01:01:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_168_strict "${const_sql_1_168}"
    testFoldConst("${const_sql_1_168}")
    def const_sql_1_169 = """select "0001-01-01 01:01:01.000001", cast(cast("0001-01-01 01:01:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_169_strict "${const_sql_1_169}"
    testFoldConst("${const_sql_1_169}")
    def const_sql_1_170 = """select "0001-01-01 01:01:01.999999", cast(cast("0001-01-01 01:01:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_170_strict "${const_sql_1_170}"
    testFoldConst("${const_sql_1_170}")
    def const_sql_1_171 = """select "0001-01-01 01:01:59.000000", cast(cast("0001-01-01 01:01:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_171_strict "${const_sql_1_171}"
    testFoldConst("${const_sql_1_171}")
    def const_sql_1_172 = """select "0001-01-01 01:01:59.000001", cast(cast("0001-01-01 01:01:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_172_strict "${const_sql_1_172}"
    testFoldConst("${const_sql_1_172}")
    def const_sql_1_173 = """select "0001-01-01 01:01:59.999999", cast(cast("0001-01-01 01:01:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_173_strict "${const_sql_1_173}"
    testFoldConst("${const_sql_1_173}")
    def const_sql_1_174 = """select "0001-01-01 01:59:00.000000", cast(cast("0001-01-01 01:59:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_174_strict "${const_sql_1_174}"
    testFoldConst("${const_sql_1_174}")
    def const_sql_1_175 = """select "0001-01-01 01:59:00.000001", cast(cast("0001-01-01 01:59:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_175_strict "${const_sql_1_175}"
    testFoldConst("${const_sql_1_175}")
    def const_sql_1_176 = """select "0001-01-01 01:59:00.999999", cast(cast("0001-01-01 01:59:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_176_strict "${const_sql_1_176}"
    testFoldConst("${const_sql_1_176}")
    def const_sql_1_177 = """select "0001-01-01 01:59:01.000000", cast(cast("0001-01-01 01:59:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_177_strict "${const_sql_1_177}"
    testFoldConst("${const_sql_1_177}")
    def const_sql_1_178 = """select "0001-01-01 01:59:01.000001", cast(cast("0001-01-01 01:59:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_178_strict "${const_sql_1_178}"
    testFoldConst("${const_sql_1_178}")
    def const_sql_1_179 = """select "0001-01-01 01:59:01.999999", cast(cast("0001-01-01 01:59:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_179_strict "${const_sql_1_179}"
    testFoldConst("${const_sql_1_179}")
    def const_sql_1_180 = """select "0001-01-01 01:59:59.000000", cast(cast("0001-01-01 01:59:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_180_strict "${const_sql_1_180}"
    testFoldConst("${const_sql_1_180}")
    def const_sql_1_181 = """select "0001-01-01 01:59:59.000001", cast(cast("0001-01-01 01:59:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_181_strict "${const_sql_1_181}"
    testFoldConst("${const_sql_1_181}")
    def const_sql_1_182 = """select "0001-01-01 01:59:59.999999", cast(cast("0001-01-01 01:59:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_182_strict "${const_sql_1_182}"
    testFoldConst("${const_sql_1_182}")
    def const_sql_1_183 = """select "0001-01-01 23:00:00.000000", cast(cast("0001-01-01 23:00:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_183_strict "${const_sql_1_183}"
    testFoldConst("${const_sql_1_183}")
    def const_sql_1_184 = """select "0001-01-01 23:00:00.000001", cast(cast("0001-01-01 23:00:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_184_strict "${const_sql_1_184}"
    testFoldConst("${const_sql_1_184}")
    def const_sql_1_185 = """select "0001-01-01 23:00:00.999999", cast(cast("0001-01-01 23:00:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_185_strict "${const_sql_1_185}"
    testFoldConst("${const_sql_1_185}")
    def const_sql_1_186 = """select "0001-01-01 23:00:01.000000", cast(cast("0001-01-01 23:00:01.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_186_strict "${const_sql_1_186}"
    testFoldConst("${const_sql_1_186}")
    def const_sql_1_187 = """select "0001-01-01 23:00:01.000001", cast(cast("0001-01-01 23:00:01.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_187_strict "${const_sql_1_187}"
    testFoldConst("${const_sql_1_187}")
    def const_sql_1_188 = """select "0001-01-01 23:00:01.999999", cast(cast("0001-01-01 23:00:01.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_188_strict "${const_sql_1_188}"
    testFoldConst("${const_sql_1_188}")
    def const_sql_1_189 = """select "0001-01-01 23:00:59.000000", cast(cast("0001-01-01 23:00:59.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_189_strict "${const_sql_1_189}"
    testFoldConst("${const_sql_1_189}")
    def const_sql_1_190 = """select "0001-01-01 23:00:59.000001", cast(cast("0001-01-01 23:00:59.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_190_strict "${const_sql_1_190}"
    testFoldConst("${const_sql_1_190}")
    def const_sql_1_191 = """select "0001-01-01 23:00:59.999999", cast(cast("0001-01-01 23:00:59.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_191_strict "${const_sql_1_191}"
    testFoldConst("${const_sql_1_191}")
    def const_sql_1_192 = """select "0001-01-01 23:01:00.000000", cast(cast("0001-01-01 23:01:00.000000" as datetimev2(6)) as largeint);"""
    qt_sql_1_192_strict "${const_sql_1_192}"
    testFoldConst("${const_sql_1_192}")
    def const_sql_1_193 = """select "0001-01-01 23:01:00.000001", cast(cast("0001-01-01 23:01:00.000001" as datetimev2(6)) as largeint);"""
    qt_sql_1_193_strict "${const_sql_1_193}"
    testFoldConst("${const_sql_1_193}")
    def const_sql_1_194 = """select "0001-01-01 23:01:00.999999", cast(cast("0001-01-01 23:01:00.999999" as datetimev2(6)) as largeint);"""
    qt_sql_1_194_strict "${const_sql_1_194}"
    testFoldConst("${const_sql_1_194}")

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
    qt_sql_1_128_non_strict "${const_sql_1_128}"
    testFoldConst("${const_sql_1_128}")
    qt_sql_1_129_non_strict "${const_sql_1_129}"
    testFoldConst("${const_sql_1_129}")
    qt_sql_1_130_non_strict "${const_sql_1_130}"
    testFoldConst("${const_sql_1_130}")
    qt_sql_1_131_non_strict "${const_sql_1_131}"
    testFoldConst("${const_sql_1_131}")
    qt_sql_1_132_non_strict "${const_sql_1_132}"
    testFoldConst("${const_sql_1_132}")
    qt_sql_1_133_non_strict "${const_sql_1_133}"
    testFoldConst("${const_sql_1_133}")
    qt_sql_1_134_non_strict "${const_sql_1_134}"
    testFoldConst("${const_sql_1_134}")
    qt_sql_1_135_non_strict "${const_sql_1_135}"
    testFoldConst("${const_sql_1_135}")
    qt_sql_1_136_non_strict "${const_sql_1_136}"
    testFoldConst("${const_sql_1_136}")
    qt_sql_1_137_non_strict "${const_sql_1_137}"
    testFoldConst("${const_sql_1_137}")
    qt_sql_1_138_non_strict "${const_sql_1_138}"
    testFoldConst("${const_sql_1_138}")
    qt_sql_1_139_non_strict "${const_sql_1_139}"
    testFoldConst("${const_sql_1_139}")
    qt_sql_1_140_non_strict "${const_sql_1_140}"
    testFoldConst("${const_sql_1_140}")
    qt_sql_1_141_non_strict "${const_sql_1_141}"
    testFoldConst("${const_sql_1_141}")
    qt_sql_1_142_non_strict "${const_sql_1_142}"
    testFoldConst("${const_sql_1_142}")
    qt_sql_1_143_non_strict "${const_sql_1_143}"
    testFoldConst("${const_sql_1_143}")
    qt_sql_1_144_non_strict "${const_sql_1_144}"
    testFoldConst("${const_sql_1_144}")
    qt_sql_1_145_non_strict "${const_sql_1_145}"
    testFoldConst("${const_sql_1_145}")
    qt_sql_1_146_non_strict "${const_sql_1_146}"
    testFoldConst("${const_sql_1_146}")
    qt_sql_1_147_non_strict "${const_sql_1_147}"
    testFoldConst("${const_sql_1_147}")
    qt_sql_1_148_non_strict "${const_sql_1_148}"
    testFoldConst("${const_sql_1_148}")
    qt_sql_1_149_non_strict "${const_sql_1_149}"
    testFoldConst("${const_sql_1_149}")
    qt_sql_1_150_non_strict "${const_sql_1_150}"
    testFoldConst("${const_sql_1_150}")
    qt_sql_1_151_non_strict "${const_sql_1_151}"
    testFoldConst("${const_sql_1_151}")
    qt_sql_1_152_non_strict "${const_sql_1_152}"
    testFoldConst("${const_sql_1_152}")
    qt_sql_1_153_non_strict "${const_sql_1_153}"
    testFoldConst("${const_sql_1_153}")
    qt_sql_1_154_non_strict "${const_sql_1_154}"
    testFoldConst("${const_sql_1_154}")
    qt_sql_1_155_non_strict "${const_sql_1_155}"
    testFoldConst("${const_sql_1_155}")
    qt_sql_1_156_non_strict "${const_sql_1_156}"
    testFoldConst("${const_sql_1_156}")
    qt_sql_1_157_non_strict "${const_sql_1_157}"
    testFoldConst("${const_sql_1_157}")
    qt_sql_1_158_non_strict "${const_sql_1_158}"
    testFoldConst("${const_sql_1_158}")
    qt_sql_1_159_non_strict "${const_sql_1_159}"
    testFoldConst("${const_sql_1_159}")
    qt_sql_1_160_non_strict "${const_sql_1_160}"
    testFoldConst("${const_sql_1_160}")
    qt_sql_1_161_non_strict "${const_sql_1_161}"
    testFoldConst("${const_sql_1_161}")
    qt_sql_1_162_non_strict "${const_sql_1_162}"
    testFoldConst("${const_sql_1_162}")
    qt_sql_1_163_non_strict "${const_sql_1_163}"
    testFoldConst("${const_sql_1_163}")
    qt_sql_1_164_non_strict "${const_sql_1_164}"
    testFoldConst("${const_sql_1_164}")
    qt_sql_1_165_non_strict "${const_sql_1_165}"
    testFoldConst("${const_sql_1_165}")
    qt_sql_1_166_non_strict "${const_sql_1_166}"
    testFoldConst("${const_sql_1_166}")
    qt_sql_1_167_non_strict "${const_sql_1_167}"
    testFoldConst("${const_sql_1_167}")
    qt_sql_1_168_non_strict "${const_sql_1_168}"
    testFoldConst("${const_sql_1_168}")
    qt_sql_1_169_non_strict "${const_sql_1_169}"
    testFoldConst("${const_sql_1_169}")
    qt_sql_1_170_non_strict "${const_sql_1_170}"
    testFoldConst("${const_sql_1_170}")
    qt_sql_1_171_non_strict "${const_sql_1_171}"
    testFoldConst("${const_sql_1_171}")
    qt_sql_1_172_non_strict "${const_sql_1_172}"
    testFoldConst("${const_sql_1_172}")
    qt_sql_1_173_non_strict "${const_sql_1_173}"
    testFoldConst("${const_sql_1_173}")
    qt_sql_1_174_non_strict "${const_sql_1_174}"
    testFoldConst("${const_sql_1_174}")
    qt_sql_1_175_non_strict "${const_sql_1_175}"
    testFoldConst("${const_sql_1_175}")
    qt_sql_1_176_non_strict "${const_sql_1_176}"
    testFoldConst("${const_sql_1_176}")
    qt_sql_1_177_non_strict "${const_sql_1_177}"
    testFoldConst("${const_sql_1_177}")
    qt_sql_1_178_non_strict "${const_sql_1_178}"
    testFoldConst("${const_sql_1_178}")
    qt_sql_1_179_non_strict "${const_sql_1_179}"
    testFoldConst("${const_sql_1_179}")
    qt_sql_1_180_non_strict "${const_sql_1_180}"
    testFoldConst("${const_sql_1_180}")
    qt_sql_1_181_non_strict "${const_sql_1_181}"
    testFoldConst("${const_sql_1_181}")
    qt_sql_1_182_non_strict "${const_sql_1_182}"
    testFoldConst("${const_sql_1_182}")
    qt_sql_1_183_non_strict "${const_sql_1_183}"
    testFoldConst("${const_sql_1_183}")
    qt_sql_1_184_non_strict "${const_sql_1_184}"
    testFoldConst("${const_sql_1_184}")
    qt_sql_1_185_non_strict "${const_sql_1_185}"
    testFoldConst("${const_sql_1_185}")
    qt_sql_1_186_non_strict "${const_sql_1_186}"
    testFoldConst("${const_sql_1_186}")
    qt_sql_1_187_non_strict "${const_sql_1_187}"
    testFoldConst("${const_sql_1_187}")
    qt_sql_1_188_non_strict "${const_sql_1_188}"
    testFoldConst("${const_sql_1_188}")
    qt_sql_1_189_non_strict "${const_sql_1_189}"
    testFoldConst("${const_sql_1_189}")
    qt_sql_1_190_non_strict "${const_sql_1_190}"
    testFoldConst("${const_sql_1_190}")
    qt_sql_1_191_non_strict "${const_sql_1_191}"
    testFoldConst("${const_sql_1_191}")
    qt_sql_1_192_non_strict "${const_sql_1_192}"
    testFoldConst("${const_sql_1_192}")
    qt_sql_1_193_non_strict "${const_sql_1_193}"
    testFoldConst("${const_sql_1_193}")
    qt_sql_1_194_non_strict "${const_sql_1_194}"
    testFoldConst("${const_sql_1_194}")
}