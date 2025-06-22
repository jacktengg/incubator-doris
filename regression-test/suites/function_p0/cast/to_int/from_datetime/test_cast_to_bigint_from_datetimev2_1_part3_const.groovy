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


suite("test_cast_to_bigint_from_datetimev2_1_part3_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_3_0 = """select "0001-12-28 00:59:00.0", cast(cast("0001-12-28 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_0_strict "${const_sql_3_0}"
    testFoldConst("${const_sql_3_0}")
    def const_sql_3_1 = """select "0001-12-28 00:59:00.0", cast(cast("0001-12-28 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_1_strict "${const_sql_3_1}"
    testFoldConst("${const_sql_3_1}")
    def const_sql_3_2 = """select "0001-12-28 00:59:00.9", cast(cast("0001-12-28 00:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_2_strict "${const_sql_3_2}"
    testFoldConst("${const_sql_3_2}")
    def const_sql_3_3 = """select "0001-12-28 00:59:01.0", cast(cast("0001-12-28 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_3_strict "${const_sql_3_3}"
    testFoldConst("${const_sql_3_3}")
    def const_sql_3_4 = """select "0001-12-28 00:59:01.0", cast(cast("0001-12-28 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_4_strict "${const_sql_3_4}"
    testFoldConst("${const_sql_3_4}")
    def const_sql_3_5 = """select "0001-12-28 00:59:01.9", cast(cast("0001-12-28 00:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_5_strict "${const_sql_3_5}"
    testFoldConst("${const_sql_3_5}")
    def const_sql_3_6 = """select "0001-12-28 00:59:59.0", cast(cast("0001-12-28 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_6_strict "${const_sql_3_6}"
    testFoldConst("${const_sql_3_6}")
    def const_sql_3_7 = """select "0001-12-28 00:59:59.0", cast(cast("0001-12-28 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_7_strict "${const_sql_3_7}"
    testFoldConst("${const_sql_3_7}")
    def const_sql_3_8 = """select "0001-12-28 00:59:59.9", cast(cast("0001-12-28 00:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_8_strict "${const_sql_3_8}"
    testFoldConst("${const_sql_3_8}")
    def const_sql_3_9 = """select "0001-12-28 01:00:00.0", cast(cast("0001-12-28 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_9_strict "${const_sql_3_9}"
    testFoldConst("${const_sql_3_9}")
    def const_sql_3_10 = """select "0001-12-28 01:00:00.0", cast(cast("0001-12-28 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_10_strict "${const_sql_3_10}"
    testFoldConst("${const_sql_3_10}")
    def const_sql_3_11 = """select "0001-12-28 01:00:00.9", cast(cast("0001-12-28 01:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_11_strict "${const_sql_3_11}"
    testFoldConst("${const_sql_3_11}")
    def const_sql_3_12 = """select "0001-12-28 01:00:01.0", cast(cast("0001-12-28 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_12_strict "${const_sql_3_12}"
    testFoldConst("${const_sql_3_12}")
    def const_sql_3_13 = """select "0001-12-28 01:00:01.0", cast(cast("0001-12-28 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_13_strict "${const_sql_3_13}"
    testFoldConst("${const_sql_3_13}")
    def const_sql_3_14 = """select "0001-12-28 01:00:01.9", cast(cast("0001-12-28 01:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_14_strict "${const_sql_3_14}"
    testFoldConst("${const_sql_3_14}")
    def const_sql_3_15 = """select "0001-12-28 01:00:59.0", cast(cast("0001-12-28 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_15_strict "${const_sql_3_15}"
    testFoldConst("${const_sql_3_15}")
    def const_sql_3_16 = """select "0001-12-28 01:00:59.0", cast(cast("0001-12-28 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_16_strict "${const_sql_3_16}"
    testFoldConst("${const_sql_3_16}")
    def const_sql_3_17 = """select "0001-12-28 01:00:59.9", cast(cast("0001-12-28 01:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_17_strict "${const_sql_3_17}"
    testFoldConst("${const_sql_3_17}")
    def const_sql_3_18 = """select "0001-12-28 01:01:00.0", cast(cast("0001-12-28 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_18_strict "${const_sql_3_18}"
    testFoldConst("${const_sql_3_18}")
    def const_sql_3_19 = """select "0001-12-28 01:01:00.0", cast(cast("0001-12-28 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_19_strict "${const_sql_3_19}"
    testFoldConst("${const_sql_3_19}")
    def const_sql_3_20 = """select "0001-12-28 01:01:00.9", cast(cast("0001-12-28 01:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_20_strict "${const_sql_3_20}"
    testFoldConst("${const_sql_3_20}")
    def const_sql_3_21 = """select "0001-12-28 01:01:01.0", cast(cast("0001-12-28 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_21_strict "${const_sql_3_21}"
    testFoldConst("${const_sql_3_21}")
    def const_sql_3_22 = """select "0001-12-28 01:01:01.0", cast(cast("0001-12-28 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_22_strict "${const_sql_3_22}"
    testFoldConst("${const_sql_3_22}")
    def const_sql_3_23 = """select "0001-12-28 01:01:01.9", cast(cast("0001-12-28 01:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_23_strict "${const_sql_3_23}"
    testFoldConst("${const_sql_3_23}")
    def const_sql_3_24 = """select "0001-12-28 01:01:59.0", cast(cast("0001-12-28 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_24_strict "${const_sql_3_24}"
    testFoldConst("${const_sql_3_24}")
    def const_sql_3_25 = """select "0001-12-28 01:01:59.0", cast(cast("0001-12-28 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_25_strict "${const_sql_3_25}"
    testFoldConst("${const_sql_3_25}")
    def const_sql_3_26 = """select "0001-12-28 01:01:59.9", cast(cast("0001-12-28 01:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_26_strict "${const_sql_3_26}"
    testFoldConst("${const_sql_3_26}")
    def const_sql_3_27 = """select "0001-12-28 01:59:00.0", cast(cast("0001-12-28 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_27_strict "${const_sql_3_27}"
    testFoldConst("${const_sql_3_27}")
    def const_sql_3_28 = """select "0001-12-28 01:59:00.0", cast(cast("0001-12-28 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_28_strict "${const_sql_3_28}"
    testFoldConst("${const_sql_3_28}")
    def const_sql_3_29 = """select "0001-12-28 01:59:00.9", cast(cast("0001-12-28 01:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_29_strict "${const_sql_3_29}"
    testFoldConst("${const_sql_3_29}")
    def const_sql_3_30 = """select "0001-12-28 01:59:01.0", cast(cast("0001-12-28 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_30_strict "${const_sql_3_30}"
    testFoldConst("${const_sql_3_30}")
    def const_sql_3_31 = """select "0001-12-28 01:59:01.0", cast(cast("0001-12-28 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_31_strict "${const_sql_3_31}"
    testFoldConst("${const_sql_3_31}")
    def const_sql_3_32 = """select "0001-12-28 01:59:01.9", cast(cast("0001-12-28 01:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_32_strict "${const_sql_3_32}"
    testFoldConst("${const_sql_3_32}")
    def const_sql_3_33 = """select "0001-12-28 01:59:59.0", cast(cast("0001-12-28 01:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_33_strict "${const_sql_3_33}"
    testFoldConst("${const_sql_3_33}")
    def const_sql_3_34 = """select "0001-12-28 01:59:59.0", cast(cast("0001-12-28 01:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_34_strict "${const_sql_3_34}"
    testFoldConst("${const_sql_3_34}")
    def const_sql_3_35 = """select "0001-12-28 01:59:59.9", cast(cast("0001-12-28 01:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_35_strict "${const_sql_3_35}"
    testFoldConst("${const_sql_3_35}")
    def const_sql_3_36 = """select "0001-12-28 23:00:00.0", cast(cast("0001-12-28 23:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_36_strict "${const_sql_3_36}"
    testFoldConst("${const_sql_3_36}")
    def const_sql_3_37 = """select "0001-12-28 23:00:00.0", cast(cast("0001-12-28 23:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_37_strict "${const_sql_3_37}"
    testFoldConst("${const_sql_3_37}")
    def const_sql_3_38 = """select "0001-12-28 23:00:00.9", cast(cast("0001-12-28 23:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_38_strict "${const_sql_3_38}"
    testFoldConst("${const_sql_3_38}")
    def const_sql_3_39 = """select "0001-12-28 23:00:01.0", cast(cast("0001-12-28 23:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_39_strict "${const_sql_3_39}"
    testFoldConst("${const_sql_3_39}")
    def const_sql_3_40 = """select "0001-12-28 23:00:01.0", cast(cast("0001-12-28 23:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_40_strict "${const_sql_3_40}"
    testFoldConst("${const_sql_3_40}")
    def const_sql_3_41 = """select "0001-12-28 23:00:01.9", cast(cast("0001-12-28 23:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_41_strict "${const_sql_3_41}"
    testFoldConst("${const_sql_3_41}")
    def const_sql_3_42 = """select "0001-12-28 23:00:59.0", cast(cast("0001-12-28 23:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_42_strict "${const_sql_3_42}"
    testFoldConst("${const_sql_3_42}")
    def const_sql_3_43 = """select "0001-12-28 23:00:59.0", cast(cast("0001-12-28 23:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_43_strict "${const_sql_3_43}"
    testFoldConst("${const_sql_3_43}")
    def const_sql_3_44 = """select "0001-12-28 23:00:59.9", cast(cast("0001-12-28 23:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_44_strict "${const_sql_3_44}"
    testFoldConst("${const_sql_3_44}")
    def const_sql_3_45 = """select "0001-12-28 23:01:00.0", cast(cast("0001-12-28 23:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_45_strict "${const_sql_3_45}"
    testFoldConst("${const_sql_3_45}")
    def const_sql_3_46 = """select "0001-12-28 23:01:00.0", cast(cast("0001-12-28 23:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_46_strict "${const_sql_3_46}"
    testFoldConst("${const_sql_3_46}")
    def const_sql_3_47 = """select "0001-12-28 23:01:00.9", cast(cast("0001-12-28 23:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_47_strict "${const_sql_3_47}"
    testFoldConst("${const_sql_3_47}")
    def const_sql_3_48 = """select "0001-12-28 23:01:01.0", cast(cast("0001-12-28 23:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_48_strict "${const_sql_3_48}"
    testFoldConst("${const_sql_3_48}")
    def const_sql_3_49 = """select "0001-12-28 23:01:01.0", cast(cast("0001-12-28 23:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_49_strict "${const_sql_3_49}"
    testFoldConst("${const_sql_3_49}")
    def const_sql_3_50 = """select "0001-12-28 23:01:01.9", cast(cast("0001-12-28 23:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_50_strict "${const_sql_3_50}"
    testFoldConst("${const_sql_3_50}")
    def const_sql_3_51 = """select "0001-12-28 23:01:59.0", cast(cast("0001-12-28 23:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_51_strict "${const_sql_3_51}"
    testFoldConst("${const_sql_3_51}")
    def const_sql_3_52 = """select "0001-12-28 23:01:59.0", cast(cast("0001-12-28 23:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_52_strict "${const_sql_3_52}"
    testFoldConst("${const_sql_3_52}")
    def const_sql_3_53 = """select "0001-12-28 23:01:59.9", cast(cast("0001-12-28 23:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_53_strict "${const_sql_3_53}"
    testFoldConst("${const_sql_3_53}")
    def const_sql_3_54 = """select "0001-12-28 23:59:00.0", cast(cast("0001-12-28 23:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_54_strict "${const_sql_3_54}"
    testFoldConst("${const_sql_3_54}")
    def const_sql_3_55 = """select "0001-12-28 23:59:00.0", cast(cast("0001-12-28 23:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_55_strict "${const_sql_3_55}"
    testFoldConst("${const_sql_3_55}")
    def const_sql_3_56 = """select "0001-12-28 23:59:00.9", cast(cast("0001-12-28 23:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_56_strict "${const_sql_3_56}"
    testFoldConst("${const_sql_3_56}")
    def const_sql_3_57 = """select "0001-12-28 23:59:01.0", cast(cast("0001-12-28 23:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_57_strict "${const_sql_3_57}"
    testFoldConst("${const_sql_3_57}")
    def const_sql_3_58 = """select "0001-12-28 23:59:01.0", cast(cast("0001-12-28 23:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_58_strict "${const_sql_3_58}"
    testFoldConst("${const_sql_3_58}")
    def const_sql_3_59 = """select "0001-12-28 23:59:01.9", cast(cast("0001-12-28 23:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_59_strict "${const_sql_3_59}"
    testFoldConst("${const_sql_3_59}")
    def const_sql_3_60 = """select "0001-12-28 23:59:59.0", cast(cast("0001-12-28 23:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_60_strict "${const_sql_3_60}"
    testFoldConst("${const_sql_3_60}")
    def const_sql_3_61 = """select "0001-12-28 23:59:59.0", cast(cast("0001-12-28 23:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_61_strict "${const_sql_3_61}"
    testFoldConst("${const_sql_3_61}")
    def const_sql_3_62 = """select "0001-12-28 23:59:59.9", cast(cast("0001-12-28 23:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_62_strict "${const_sql_3_62}"
    testFoldConst("${const_sql_3_62}")
    def const_sql_3_63 = """select "0010-01-01 00:00:00.0", cast(cast("0010-01-01 00:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_63_strict "${const_sql_3_63}"
    testFoldConst("${const_sql_3_63}")
    def const_sql_3_64 = """select "0010-01-01 00:00:00.0", cast(cast("0010-01-01 00:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_64_strict "${const_sql_3_64}"
    testFoldConst("${const_sql_3_64}")
    def const_sql_3_65 = """select "0010-01-01 00:00:00.9", cast(cast("0010-01-01 00:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_65_strict "${const_sql_3_65}"
    testFoldConst("${const_sql_3_65}")
    def const_sql_3_66 = """select "0010-01-01 00:00:01.0", cast(cast("0010-01-01 00:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_66_strict "${const_sql_3_66}"
    testFoldConst("${const_sql_3_66}")
    def const_sql_3_67 = """select "0010-01-01 00:00:01.0", cast(cast("0010-01-01 00:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_67_strict "${const_sql_3_67}"
    testFoldConst("${const_sql_3_67}")
    def const_sql_3_68 = """select "0010-01-01 00:00:01.9", cast(cast("0010-01-01 00:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_68_strict "${const_sql_3_68}"
    testFoldConst("${const_sql_3_68}")
    def const_sql_3_69 = """select "0010-01-01 00:00:59.0", cast(cast("0010-01-01 00:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_69_strict "${const_sql_3_69}"
    testFoldConst("${const_sql_3_69}")
    def const_sql_3_70 = """select "0010-01-01 00:00:59.0", cast(cast("0010-01-01 00:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_70_strict "${const_sql_3_70}"
    testFoldConst("${const_sql_3_70}")
    def const_sql_3_71 = """select "0010-01-01 00:00:59.9", cast(cast("0010-01-01 00:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_71_strict "${const_sql_3_71}"
    testFoldConst("${const_sql_3_71}")
    def const_sql_3_72 = """select "0010-01-01 00:01:00.0", cast(cast("0010-01-01 00:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_72_strict "${const_sql_3_72}"
    testFoldConst("${const_sql_3_72}")
    def const_sql_3_73 = """select "0010-01-01 00:01:00.0", cast(cast("0010-01-01 00:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_73_strict "${const_sql_3_73}"
    testFoldConst("${const_sql_3_73}")
    def const_sql_3_74 = """select "0010-01-01 00:01:00.9", cast(cast("0010-01-01 00:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_74_strict "${const_sql_3_74}"
    testFoldConst("${const_sql_3_74}")
    def const_sql_3_75 = """select "0010-01-01 00:01:01.0", cast(cast("0010-01-01 00:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_75_strict "${const_sql_3_75}"
    testFoldConst("${const_sql_3_75}")
    def const_sql_3_76 = """select "0010-01-01 00:01:01.0", cast(cast("0010-01-01 00:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_76_strict "${const_sql_3_76}"
    testFoldConst("${const_sql_3_76}")
    def const_sql_3_77 = """select "0010-01-01 00:01:01.9", cast(cast("0010-01-01 00:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_77_strict "${const_sql_3_77}"
    testFoldConst("${const_sql_3_77}")
    def const_sql_3_78 = """select "0010-01-01 00:01:59.0", cast(cast("0010-01-01 00:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_78_strict "${const_sql_3_78}"
    testFoldConst("${const_sql_3_78}")
    def const_sql_3_79 = """select "0010-01-01 00:01:59.0", cast(cast("0010-01-01 00:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_79_strict "${const_sql_3_79}"
    testFoldConst("${const_sql_3_79}")
    def const_sql_3_80 = """select "0010-01-01 00:01:59.9", cast(cast("0010-01-01 00:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_80_strict "${const_sql_3_80}"
    testFoldConst("${const_sql_3_80}")
    def const_sql_3_81 = """select "0010-01-01 00:59:00.0", cast(cast("0010-01-01 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_81_strict "${const_sql_3_81}"
    testFoldConst("${const_sql_3_81}")
    def const_sql_3_82 = """select "0010-01-01 00:59:00.0", cast(cast("0010-01-01 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_82_strict "${const_sql_3_82}"
    testFoldConst("${const_sql_3_82}")
    def const_sql_3_83 = """select "0010-01-01 00:59:00.9", cast(cast("0010-01-01 00:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_83_strict "${const_sql_3_83}"
    testFoldConst("${const_sql_3_83}")
    def const_sql_3_84 = """select "0010-01-01 00:59:01.0", cast(cast("0010-01-01 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_84_strict "${const_sql_3_84}"
    testFoldConst("${const_sql_3_84}")
    def const_sql_3_85 = """select "0010-01-01 00:59:01.0", cast(cast("0010-01-01 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_85_strict "${const_sql_3_85}"
    testFoldConst("${const_sql_3_85}")
    def const_sql_3_86 = """select "0010-01-01 00:59:01.9", cast(cast("0010-01-01 00:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_86_strict "${const_sql_3_86}"
    testFoldConst("${const_sql_3_86}")
    def const_sql_3_87 = """select "0010-01-01 00:59:59.0", cast(cast("0010-01-01 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_87_strict "${const_sql_3_87}"
    testFoldConst("${const_sql_3_87}")
    def const_sql_3_88 = """select "0010-01-01 00:59:59.0", cast(cast("0010-01-01 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_88_strict "${const_sql_3_88}"
    testFoldConst("${const_sql_3_88}")
    def const_sql_3_89 = """select "0010-01-01 00:59:59.9", cast(cast("0010-01-01 00:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_89_strict "${const_sql_3_89}"
    testFoldConst("${const_sql_3_89}")
    def const_sql_3_90 = """select "0010-01-01 01:00:00.0", cast(cast("0010-01-01 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_90_strict "${const_sql_3_90}"
    testFoldConst("${const_sql_3_90}")
    def const_sql_3_91 = """select "0010-01-01 01:00:00.0", cast(cast("0010-01-01 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_91_strict "${const_sql_3_91}"
    testFoldConst("${const_sql_3_91}")
    def const_sql_3_92 = """select "0010-01-01 01:00:00.9", cast(cast("0010-01-01 01:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_92_strict "${const_sql_3_92}"
    testFoldConst("${const_sql_3_92}")
    def const_sql_3_93 = """select "0010-01-01 01:00:01.0", cast(cast("0010-01-01 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_93_strict "${const_sql_3_93}"
    testFoldConst("${const_sql_3_93}")
    def const_sql_3_94 = """select "0010-01-01 01:00:01.0", cast(cast("0010-01-01 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_94_strict "${const_sql_3_94}"
    testFoldConst("${const_sql_3_94}")
    def const_sql_3_95 = """select "0010-01-01 01:00:01.9", cast(cast("0010-01-01 01:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_95_strict "${const_sql_3_95}"
    testFoldConst("${const_sql_3_95}")
    def const_sql_3_96 = """select "0010-01-01 01:00:59.0", cast(cast("0010-01-01 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_96_strict "${const_sql_3_96}"
    testFoldConst("${const_sql_3_96}")
    def const_sql_3_97 = """select "0010-01-01 01:00:59.0", cast(cast("0010-01-01 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_97_strict "${const_sql_3_97}"
    testFoldConst("${const_sql_3_97}")
    def const_sql_3_98 = """select "0010-01-01 01:00:59.9", cast(cast("0010-01-01 01:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_98_strict "${const_sql_3_98}"
    testFoldConst("${const_sql_3_98}")
    def const_sql_3_99 = """select "0010-01-01 01:01:00.0", cast(cast("0010-01-01 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_99_strict "${const_sql_3_99}"
    testFoldConst("${const_sql_3_99}")
    def const_sql_3_100 = """select "0010-01-01 01:01:00.0", cast(cast("0010-01-01 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_100_strict "${const_sql_3_100}"
    testFoldConst("${const_sql_3_100}")
    def const_sql_3_101 = """select "0010-01-01 01:01:00.9", cast(cast("0010-01-01 01:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_101_strict "${const_sql_3_101}"
    testFoldConst("${const_sql_3_101}")
    def const_sql_3_102 = """select "0010-01-01 01:01:01.0", cast(cast("0010-01-01 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_102_strict "${const_sql_3_102}"
    testFoldConst("${const_sql_3_102}")
    def const_sql_3_103 = """select "0010-01-01 01:01:01.0", cast(cast("0010-01-01 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_103_strict "${const_sql_3_103}"
    testFoldConst("${const_sql_3_103}")
    def const_sql_3_104 = """select "0010-01-01 01:01:01.9", cast(cast("0010-01-01 01:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_104_strict "${const_sql_3_104}"
    testFoldConst("${const_sql_3_104}")
    def const_sql_3_105 = """select "0010-01-01 01:01:59.0", cast(cast("0010-01-01 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_105_strict "${const_sql_3_105}"
    testFoldConst("${const_sql_3_105}")
    def const_sql_3_106 = """select "0010-01-01 01:01:59.0", cast(cast("0010-01-01 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_106_strict "${const_sql_3_106}"
    testFoldConst("${const_sql_3_106}")
    def const_sql_3_107 = """select "0010-01-01 01:01:59.9", cast(cast("0010-01-01 01:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_107_strict "${const_sql_3_107}"
    testFoldConst("${const_sql_3_107}")
    def const_sql_3_108 = """select "0010-01-01 01:59:00.0", cast(cast("0010-01-01 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_108_strict "${const_sql_3_108}"
    testFoldConst("${const_sql_3_108}")
    def const_sql_3_109 = """select "0010-01-01 01:59:00.0", cast(cast("0010-01-01 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_109_strict "${const_sql_3_109}"
    testFoldConst("${const_sql_3_109}")
    def const_sql_3_110 = """select "0010-01-01 01:59:00.9", cast(cast("0010-01-01 01:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_110_strict "${const_sql_3_110}"
    testFoldConst("${const_sql_3_110}")
    def const_sql_3_111 = """select "0010-01-01 01:59:01.0", cast(cast("0010-01-01 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_111_strict "${const_sql_3_111}"
    testFoldConst("${const_sql_3_111}")
    def const_sql_3_112 = """select "0010-01-01 01:59:01.0", cast(cast("0010-01-01 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_112_strict "${const_sql_3_112}"
    testFoldConst("${const_sql_3_112}")
    def const_sql_3_113 = """select "0010-01-01 01:59:01.9", cast(cast("0010-01-01 01:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_113_strict "${const_sql_3_113}"
    testFoldConst("${const_sql_3_113}")
    def const_sql_3_114 = """select "0010-01-01 01:59:59.0", cast(cast("0010-01-01 01:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_114_strict "${const_sql_3_114}"
    testFoldConst("${const_sql_3_114}")
    def const_sql_3_115 = """select "0010-01-01 01:59:59.0", cast(cast("0010-01-01 01:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_115_strict "${const_sql_3_115}"
    testFoldConst("${const_sql_3_115}")
    def const_sql_3_116 = """select "0010-01-01 01:59:59.9", cast(cast("0010-01-01 01:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_116_strict "${const_sql_3_116}"
    testFoldConst("${const_sql_3_116}")
    def const_sql_3_117 = """select "0010-01-01 23:00:00.0", cast(cast("0010-01-01 23:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_117_strict "${const_sql_3_117}"
    testFoldConst("${const_sql_3_117}")
    def const_sql_3_118 = """select "0010-01-01 23:00:00.0", cast(cast("0010-01-01 23:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_118_strict "${const_sql_3_118}"
    testFoldConst("${const_sql_3_118}")
    def const_sql_3_119 = """select "0010-01-01 23:00:00.9", cast(cast("0010-01-01 23:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_119_strict "${const_sql_3_119}"
    testFoldConst("${const_sql_3_119}")
    def const_sql_3_120 = """select "0010-01-01 23:00:01.0", cast(cast("0010-01-01 23:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_120_strict "${const_sql_3_120}"
    testFoldConst("${const_sql_3_120}")
    def const_sql_3_121 = """select "0010-01-01 23:00:01.0", cast(cast("0010-01-01 23:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_121_strict "${const_sql_3_121}"
    testFoldConst("${const_sql_3_121}")
    def const_sql_3_122 = """select "0010-01-01 23:00:01.9", cast(cast("0010-01-01 23:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_122_strict "${const_sql_3_122}"
    testFoldConst("${const_sql_3_122}")
    def const_sql_3_123 = """select "0010-01-01 23:00:59.0", cast(cast("0010-01-01 23:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_123_strict "${const_sql_3_123}"
    testFoldConst("${const_sql_3_123}")
    def const_sql_3_124 = """select "0010-01-01 23:00:59.0", cast(cast("0010-01-01 23:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_124_strict "${const_sql_3_124}"
    testFoldConst("${const_sql_3_124}")
    def const_sql_3_125 = """select "0010-01-01 23:00:59.9", cast(cast("0010-01-01 23:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_125_strict "${const_sql_3_125}"
    testFoldConst("${const_sql_3_125}")
    def const_sql_3_126 = """select "0010-01-01 23:01:00.0", cast(cast("0010-01-01 23:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_126_strict "${const_sql_3_126}"
    testFoldConst("${const_sql_3_126}")
    def const_sql_3_127 = """select "0010-01-01 23:01:00.0", cast(cast("0010-01-01 23:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_127_strict "${const_sql_3_127}"
    testFoldConst("${const_sql_3_127}")
    def const_sql_3_128 = """select "0010-01-01 23:01:00.9", cast(cast("0010-01-01 23:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_128_strict "${const_sql_3_128}"
    testFoldConst("${const_sql_3_128}")
    def const_sql_3_129 = """select "0010-01-01 23:01:01.0", cast(cast("0010-01-01 23:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_129_strict "${const_sql_3_129}"
    testFoldConst("${const_sql_3_129}")
    def const_sql_3_130 = """select "0010-01-01 23:01:01.0", cast(cast("0010-01-01 23:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_130_strict "${const_sql_3_130}"
    testFoldConst("${const_sql_3_130}")
    def const_sql_3_131 = """select "0010-01-01 23:01:01.9", cast(cast("0010-01-01 23:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_131_strict "${const_sql_3_131}"
    testFoldConst("${const_sql_3_131}")
    def const_sql_3_132 = """select "0010-01-01 23:01:59.0", cast(cast("0010-01-01 23:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_132_strict "${const_sql_3_132}"
    testFoldConst("${const_sql_3_132}")
    def const_sql_3_133 = """select "0010-01-01 23:01:59.0", cast(cast("0010-01-01 23:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_133_strict "${const_sql_3_133}"
    testFoldConst("${const_sql_3_133}")
    def const_sql_3_134 = """select "0010-01-01 23:01:59.9", cast(cast("0010-01-01 23:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_134_strict "${const_sql_3_134}"
    testFoldConst("${const_sql_3_134}")
    def const_sql_3_135 = """select "0010-01-01 23:59:00.0", cast(cast("0010-01-01 23:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_135_strict "${const_sql_3_135}"
    testFoldConst("${const_sql_3_135}")
    def const_sql_3_136 = """select "0010-01-01 23:59:00.0", cast(cast("0010-01-01 23:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_136_strict "${const_sql_3_136}"
    testFoldConst("${const_sql_3_136}")
    def const_sql_3_137 = """select "0010-01-01 23:59:00.9", cast(cast("0010-01-01 23:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_137_strict "${const_sql_3_137}"
    testFoldConst("${const_sql_3_137}")
    def const_sql_3_138 = """select "0010-01-01 23:59:01.0", cast(cast("0010-01-01 23:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_138_strict "${const_sql_3_138}"
    testFoldConst("${const_sql_3_138}")
    def const_sql_3_139 = """select "0010-01-01 23:59:01.0", cast(cast("0010-01-01 23:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_139_strict "${const_sql_3_139}"
    testFoldConst("${const_sql_3_139}")
    def const_sql_3_140 = """select "0010-01-01 23:59:01.9", cast(cast("0010-01-01 23:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_140_strict "${const_sql_3_140}"
    testFoldConst("${const_sql_3_140}")
    def const_sql_3_141 = """select "0010-01-01 23:59:59.0", cast(cast("0010-01-01 23:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_141_strict "${const_sql_3_141}"
    testFoldConst("${const_sql_3_141}")
    def const_sql_3_142 = """select "0010-01-01 23:59:59.0", cast(cast("0010-01-01 23:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_142_strict "${const_sql_3_142}"
    testFoldConst("${const_sql_3_142}")
    def const_sql_3_143 = """select "0010-01-01 23:59:59.9", cast(cast("0010-01-01 23:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_143_strict "${const_sql_3_143}"
    testFoldConst("${const_sql_3_143}")
    def const_sql_3_144 = """select "0010-01-28 00:00:00.0", cast(cast("0010-01-28 00:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_144_strict "${const_sql_3_144}"
    testFoldConst("${const_sql_3_144}")
    def const_sql_3_145 = """select "0010-01-28 00:00:00.0", cast(cast("0010-01-28 00:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_145_strict "${const_sql_3_145}"
    testFoldConst("${const_sql_3_145}")
    def const_sql_3_146 = """select "0010-01-28 00:00:00.9", cast(cast("0010-01-28 00:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_146_strict "${const_sql_3_146}"
    testFoldConst("${const_sql_3_146}")
    def const_sql_3_147 = """select "0010-01-28 00:00:01.0", cast(cast("0010-01-28 00:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_147_strict "${const_sql_3_147}"
    testFoldConst("${const_sql_3_147}")
    def const_sql_3_148 = """select "0010-01-28 00:00:01.0", cast(cast("0010-01-28 00:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_148_strict "${const_sql_3_148}"
    testFoldConst("${const_sql_3_148}")
    def const_sql_3_149 = """select "0010-01-28 00:00:01.9", cast(cast("0010-01-28 00:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_149_strict "${const_sql_3_149}"
    testFoldConst("${const_sql_3_149}")
    def const_sql_3_150 = """select "0010-01-28 00:00:59.0", cast(cast("0010-01-28 00:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_150_strict "${const_sql_3_150}"
    testFoldConst("${const_sql_3_150}")
    def const_sql_3_151 = """select "0010-01-28 00:00:59.0", cast(cast("0010-01-28 00:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_151_strict "${const_sql_3_151}"
    testFoldConst("${const_sql_3_151}")
    def const_sql_3_152 = """select "0010-01-28 00:00:59.9", cast(cast("0010-01-28 00:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_152_strict "${const_sql_3_152}"
    testFoldConst("${const_sql_3_152}")
    def const_sql_3_153 = """select "0010-01-28 00:01:00.0", cast(cast("0010-01-28 00:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_153_strict "${const_sql_3_153}"
    testFoldConst("${const_sql_3_153}")
    def const_sql_3_154 = """select "0010-01-28 00:01:00.0", cast(cast("0010-01-28 00:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_154_strict "${const_sql_3_154}"
    testFoldConst("${const_sql_3_154}")
    def const_sql_3_155 = """select "0010-01-28 00:01:00.9", cast(cast("0010-01-28 00:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_155_strict "${const_sql_3_155}"
    testFoldConst("${const_sql_3_155}")
    def const_sql_3_156 = """select "0010-01-28 00:01:01.0", cast(cast("0010-01-28 00:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_156_strict "${const_sql_3_156}"
    testFoldConst("${const_sql_3_156}")
    def const_sql_3_157 = """select "0010-01-28 00:01:01.0", cast(cast("0010-01-28 00:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_157_strict "${const_sql_3_157}"
    testFoldConst("${const_sql_3_157}")
    def const_sql_3_158 = """select "0010-01-28 00:01:01.9", cast(cast("0010-01-28 00:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_158_strict "${const_sql_3_158}"
    testFoldConst("${const_sql_3_158}")
    def const_sql_3_159 = """select "0010-01-28 00:01:59.0", cast(cast("0010-01-28 00:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_159_strict "${const_sql_3_159}"
    testFoldConst("${const_sql_3_159}")
    def const_sql_3_160 = """select "0010-01-28 00:01:59.0", cast(cast("0010-01-28 00:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_160_strict "${const_sql_3_160}"
    testFoldConst("${const_sql_3_160}")
    def const_sql_3_161 = """select "0010-01-28 00:01:59.9", cast(cast("0010-01-28 00:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_161_strict "${const_sql_3_161}"
    testFoldConst("${const_sql_3_161}")
    def const_sql_3_162 = """select "0010-01-28 00:59:00.0", cast(cast("0010-01-28 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_162_strict "${const_sql_3_162}"
    testFoldConst("${const_sql_3_162}")
    def const_sql_3_163 = """select "0010-01-28 00:59:00.0", cast(cast("0010-01-28 00:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_163_strict "${const_sql_3_163}"
    testFoldConst("${const_sql_3_163}")
    def const_sql_3_164 = """select "0010-01-28 00:59:00.9", cast(cast("0010-01-28 00:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_164_strict "${const_sql_3_164}"
    testFoldConst("${const_sql_3_164}")
    def const_sql_3_165 = """select "0010-01-28 00:59:01.0", cast(cast("0010-01-28 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_165_strict "${const_sql_3_165}"
    testFoldConst("${const_sql_3_165}")
    def const_sql_3_166 = """select "0010-01-28 00:59:01.0", cast(cast("0010-01-28 00:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_166_strict "${const_sql_3_166}"
    testFoldConst("${const_sql_3_166}")
    def const_sql_3_167 = """select "0010-01-28 00:59:01.9", cast(cast("0010-01-28 00:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_167_strict "${const_sql_3_167}"
    testFoldConst("${const_sql_3_167}")
    def const_sql_3_168 = """select "0010-01-28 00:59:59.0", cast(cast("0010-01-28 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_168_strict "${const_sql_3_168}"
    testFoldConst("${const_sql_3_168}")
    def const_sql_3_169 = """select "0010-01-28 00:59:59.0", cast(cast("0010-01-28 00:59:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_169_strict "${const_sql_3_169}"
    testFoldConst("${const_sql_3_169}")
    def const_sql_3_170 = """select "0010-01-28 00:59:59.9", cast(cast("0010-01-28 00:59:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_170_strict "${const_sql_3_170}"
    testFoldConst("${const_sql_3_170}")
    def const_sql_3_171 = """select "0010-01-28 01:00:00.0", cast(cast("0010-01-28 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_171_strict "${const_sql_3_171}"
    testFoldConst("${const_sql_3_171}")
    def const_sql_3_172 = """select "0010-01-28 01:00:00.0", cast(cast("0010-01-28 01:00:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_172_strict "${const_sql_3_172}"
    testFoldConst("${const_sql_3_172}")
    def const_sql_3_173 = """select "0010-01-28 01:00:00.9", cast(cast("0010-01-28 01:00:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_173_strict "${const_sql_3_173}"
    testFoldConst("${const_sql_3_173}")
    def const_sql_3_174 = """select "0010-01-28 01:00:01.0", cast(cast("0010-01-28 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_174_strict "${const_sql_3_174}"
    testFoldConst("${const_sql_3_174}")
    def const_sql_3_175 = """select "0010-01-28 01:00:01.0", cast(cast("0010-01-28 01:00:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_175_strict "${const_sql_3_175}"
    testFoldConst("${const_sql_3_175}")
    def const_sql_3_176 = """select "0010-01-28 01:00:01.9", cast(cast("0010-01-28 01:00:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_176_strict "${const_sql_3_176}"
    testFoldConst("${const_sql_3_176}")
    def const_sql_3_177 = """select "0010-01-28 01:00:59.0", cast(cast("0010-01-28 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_177_strict "${const_sql_3_177}"
    testFoldConst("${const_sql_3_177}")
    def const_sql_3_178 = """select "0010-01-28 01:00:59.0", cast(cast("0010-01-28 01:00:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_178_strict "${const_sql_3_178}"
    testFoldConst("${const_sql_3_178}")
    def const_sql_3_179 = """select "0010-01-28 01:00:59.9", cast(cast("0010-01-28 01:00:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_179_strict "${const_sql_3_179}"
    testFoldConst("${const_sql_3_179}")
    def const_sql_3_180 = """select "0010-01-28 01:01:00.0", cast(cast("0010-01-28 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_180_strict "${const_sql_3_180}"
    testFoldConst("${const_sql_3_180}")
    def const_sql_3_181 = """select "0010-01-28 01:01:00.0", cast(cast("0010-01-28 01:01:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_181_strict "${const_sql_3_181}"
    testFoldConst("${const_sql_3_181}")
    def const_sql_3_182 = """select "0010-01-28 01:01:00.9", cast(cast("0010-01-28 01:01:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_182_strict "${const_sql_3_182}"
    testFoldConst("${const_sql_3_182}")
    def const_sql_3_183 = """select "0010-01-28 01:01:01.0", cast(cast("0010-01-28 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_183_strict "${const_sql_3_183}"
    testFoldConst("${const_sql_3_183}")
    def const_sql_3_184 = """select "0010-01-28 01:01:01.0", cast(cast("0010-01-28 01:01:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_184_strict "${const_sql_3_184}"
    testFoldConst("${const_sql_3_184}")
    def const_sql_3_185 = """select "0010-01-28 01:01:01.9", cast(cast("0010-01-28 01:01:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_185_strict "${const_sql_3_185}"
    testFoldConst("${const_sql_3_185}")
    def const_sql_3_186 = """select "0010-01-28 01:01:59.0", cast(cast("0010-01-28 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_186_strict "${const_sql_3_186}"
    testFoldConst("${const_sql_3_186}")
    def const_sql_3_187 = """select "0010-01-28 01:01:59.0", cast(cast("0010-01-28 01:01:59.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_187_strict "${const_sql_3_187}"
    testFoldConst("${const_sql_3_187}")
    def const_sql_3_188 = """select "0010-01-28 01:01:59.9", cast(cast("0010-01-28 01:01:59.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_188_strict "${const_sql_3_188}"
    testFoldConst("${const_sql_3_188}")
    def const_sql_3_189 = """select "0010-01-28 01:59:00.0", cast(cast("0010-01-28 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_189_strict "${const_sql_3_189}"
    testFoldConst("${const_sql_3_189}")
    def const_sql_3_190 = """select "0010-01-28 01:59:00.0", cast(cast("0010-01-28 01:59:00.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_190_strict "${const_sql_3_190}"
    testFoldConst("${const_sql_3_190}")
    def const_sql_3_191 = """select "0010-01-28 01:59:00.9", cast(cast("0010-01-28 01:59:00.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_191_strict "${const_sql_3_191}"
    testFoldConst("${const_sql_3_191}")
    def const_sql_3_192 = """select "0010-01-28 01:59:01.0", cast(cast("0010-01-28 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_192_strict "${const_sql_3_192}"
    testFoldConst("${const_sql_3_192}")
    def const_sql_3_193 = """select "0010-01-28 01:59:01.0", cast(cast("0010-01-28 01:59:01.0" as datetimev2(1)) as bigint);"""
    qt_sql_3_193_strict "${const_sql_3_193}"
    testFoldConst("${const_sql_3_193}")
    def const_sql_3_194 = """select "0010-01-28 01:59:01.9", cast(cast("0010-01-28 01:59:01.9" as datetimev2(1)) as bigint);"""
    qt_sql_3_194_strict "${const_sql_3_194}"
    testFoldConst("${const_sql_3_194}")

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
    qt_sql_3_64_non_strict "${const_sql_3_64}"
    testFoldConst("${const_sql_3_64}")
    qt_sql_3_65_non_strict "${const_sql_3_65}"
    testFoldConst("${const_sql_3_65}")
    qt_sql_3_66_non_strict "${const_sql_3_66}"
    testFoldConst("${const_sql_3_66}")
    qt_sql_3_67_non_strict "${const_sql_3_67}"
    testFoldConst("${const_sql_3_67}")
    qt_sql_3_68_non_strict "${const_sql_3_68}"
    testFoldConst("${const_sql_3_68}")
    qt_sql_3_69_non_strict "${const_sql_3_69}"
    testFoldConst("${const_sql_3_69}")
    qt_sql_3_70_non_strict "${const_sql_3_70}"
    testFoldConst("${const_sql_3_70}")
    qt_sql_3_71_non_strict "${const_sql_3_71}"
    testFoldConst("${const_sql_3_71}")
    qt_sql_3_72_non_strict "${const_sql_3_72}"
    testFoldConst("${const_sql_3_72}")
    qt_sql_3_73_non_strict "${const_sql_3_73}"
    testFoldConst("${const_sql_3_73}")
    qt_sql_3_74_non_strict "${const_sql_3_74}"
    testFoldConst("${const_sql_3_74}")
    qt_sql_3_75_non_strict "${const_sql_3_75}"
    testFoldConst("${const_sql_3_75}")
    qt_sql_3_76_non_strict "${const_sql_3_76}"
    testFoldConst("${const_sql_3_76}")
    qt_sql_3_77_non_strict "${const_sql_3_77}"
    testFoldConst("${const_sql_3_77}")
    qt_sql_3_78_non_strict "${const_sql_3_78}"
    testFoldConst("${const_sql_3_78}")
    qt_sql_3_79_non_strict "${const_sql_3_79}"
    testFoldConst("${const_sql_3_79}")
    qt_sql_3_80_non_strict "${const_sql_3_80}"
    testFoldConst("${const_sql_3_80}")
    qt_sql_3_81_non_strict "${const_sql_3_81}"
    testFoldConst("${const_sql_3_81}")
    qt_sql_3_82_non_strict "${const_sql_3_82}"
    testFoldConst("${const_sql_3_82}")
    qt_sql_3_83_non_strict "${const_sql_3_83}"
    testFoldConst("${const_sql_3_83}")
    qt_sql_3_84_non_strict "${const_sql_3_84}"
    testFoldConst("${const_sql_3_84}")
    qt_sql_3_85_non_strict "${const_sql_3_85}"
    testFoldConst("${const_sql_3_85}")
    qt_sql_3_86_non_strict "${const_sql_3_86}"
    testFoldConst("${const_sql_3_86}")
    qt_sql_3_87_non_strict "${const_sql_3_87}"
    testFoldConst("${const_sql_3_87}")
    qt_sql_3_88_non_strict "${const_sql_3_88}"
    testFoldConst("${const_sql_3_88}")
    qt_sql_3_89_non_strict "${const_sql_3_89}"
    testFoldConst("${const_sql_3_89}")
    qt_sql_3_90_non_strict "${const_sql_3_90}"
    testFoldConst("${const_sql_3_90}")
    qt_sql_3_91_non_strict "${const_sql_3_91}"
    testFoldConst("${const_sql_3_91}")
    qt_sql_3_92_non_strict "${const_sql_3_92}"
    testFoldConst("${const_sql_3_92}")
    qt_sql_3_93_non_strict "${const_sql_3_93}"
    testFoldConst("${const_sql_3_93}")
    qt_sql_3_94_non_strict "${const_sql_3_94}"
    testFoldConst("${const_sql_3_94}")
    qt_sql_3_95_non_strict "${const_sql_3_95}"
    testFoldConst("${const_sql_3_95}")
    qt_sql_3_96_non_strict "${const_sql_3_96}"
    testFoldConst("${const_sql_3_96}")
    qt_sql_3_97_non_strict "${const_sql_3_97}"
    testFoldConst("${const_sql_3_97}")
    qt_sql_3_98_non_strict "${const_sql_3_98}"
    testFoldConst("${const_sql_3_98}")
    qt_sql_3_99_non_strict "${const_sql_3_99}"
    testFoldConst("${const_sql_3_99}")
    qt_sql_3_100_non_strict "${const_sql_3_100}"
    testFoldConst("${const_sql_3_100}")
    qt_sql_3_101_non_strict "${const_sql_3_101}"
    testFoldConst("${const_sql_3_101}")
    qt_sql_3_102_non_strict "${const_sql_3_102}"
    testFoldConst("${const_sql_3_102}")
    qt_sql_3_103_non_strict "${const_sql_3_103}"
    testFoldConst("${const_sql_3_103}")
    qt_sql_3_104_non_strict "${const_sql_3_104}"
    testFoldConst("${const_sql_3_104}")
    qt_sql_3_105_non_strict "${const_sql_3_105}"
    testFoldConst("${const_sql_3_105}")
    qt_sql_3_106_non_strict "${const_sql_3_106}"
    testFoldConst("${const_sql_3_106}")
    qt_sql_3_107_non_strict "${const_sql_3_107}"
    testFoldConst("${const_sql_3_107}")
    qt_sql_3_108_non_strict "${const_sql_3_108}"
    testFoldConst("${const_sql_3_108}")
    qt_sql_3_109_non_strict "${const_sql_3_109}"
    testFoldConst("${const_sql_3_109}")
    qt_sql_3_110_non_strict "${const_sql_3_110}"
    testFoldConst("${const_sql_3_110}")
    qt_sql_3_111_non_strict "${const_sql_3_111}"
    testFoldConst("${const_sql_3_111}")
    qt_sql_3_112_non_strict "${const_sql_3_112}"
    testFoldConst("${const_sql_3_112}")
    qt_sql_3_113_non_strict "${const_sql_3_113}"
    testFoldConst("${const_sql_3_113}")
    qt_sql_3_114_non_strict "${const_sql_3_114}"
    testFoldConst("${const_sql_3_114}")
    qt_sql_3_115_non_strict "${const_sql_3_115}"
    testFoldConst("${const_sql_3_115}")
    qt_sql_3_116_non_strict "${const_sql_3_116}"
    testFoldConst("${const_sql_3_116}")
    qt_sql_3_117_non_strict "${const_sql_3_117}"
    testFoldConst("${const_sql_3_117}")
    qt_sql_3_118_non_strict "${const_sql_3_118}"
    testFoldConst("${const_sql_3_118}")
    qt_sql_3_119_non_strict "${const_sql_3_119}"
    testFoldConst("${const_sql_3_119}")
    qt_sql_3_120_non_strict "${const_sql_3_120}"
    testFoldConst("${const_sql_3_120}")
    qt_sql_3_121_non_strict "${const_sql_3_121}"
    testFoldConst("${const_sql_3_121}")
    qt_sql_3_122_non_strict "${const_sql_3_122}"
    testFoldConst("${const_sql_3_122}")
    qt_sql_3_123_non_strict "${const_sql_3_123}"
    testFoldConst("${const_sql_3_123}")
    qt_sql_3_124_non_strict "${const_sql_3_124}"
    testFoldConst("${const_sql_3_124}")
    qt_sql_3_125_non_strict "${const_sql_3_125}"
    testFoldConst("${const_sql_3_125}")
    qt_sql_3_126_non_strict "${const_sql_3_126}"
    testFoldConst("${const_sql_3_126}")
    qt_sql_3_127_non_strict "${const_sql_3_127}"
    testFoldConst("${const_sql_3_127}")
    qt_sql_3_128_non_strict "${const_sql_3_128}"
    testFoldConst("${const_sql_3_128}")
    qt_sql_3_129_non_strict "${const_sql_3_129}"
    testFoldConst("${const_sql_3_129}")
    qt_sql_3_130_non_strict "${const_sql_3_130}"
    testFoldConst("${const_sql_3_130}")
    qt_sql_3_131_non_strict "${const_sql_3_131}"
    testFoldConst("${const_sql_3_131}")
    qt_sql_3_132_non_strict "${const_sql_3_132}"
    testFoldConst("${const_sql_3_132}")
    qt_sql_3_133_non_strict "${const_sql_3_133}"
    testFoldConst("${const_sql_3_133}")
    qt_sql_3_134_non_strict "${const_sql_3_134}"
    testFoldConst("${const_sql_3_134}")
    qt_sql_3_135_non_strict "${const_sql_3_135}"
    testFoldConst("${const_sql_3_135}")
    qt_sql_3_136_non_strict "${const_sql_3_136}"
    testFoldConst("${const_sql_3_136}")
    qt_sql_3_137_non_strict "${const_sql_3_137}"
    testFoldConst("${const_sql_3_137}")
    qt_sql_3_138_non_strict "${const_sql_3_138}"
    testFoldConst("${const_sql_3_138}")
    qt_sql_3_139_non_strict "${const_sql_3_139}"
    testFoldConst("${const_sql_3_139}")
    qt_sql_3_140_non_strict "${const_sql_3_140}"
    testFoldConst("${const_sql_3_140}")
    qt_sql_3_141_non_strict "${const_sql_3_141}"
    testFoldConst("${const_sql_3_141}")
    qt_sql_3_142_non_strict "${const_sql_3_142}"
    testFoldConst("${const_sql_3_142}")
    qt_sql_3_143_non_strict "${const_sql_3_143}"
    testFoldConst("${const_sql_3_143}")
    qt_sql_3_144_non_strict "${const_sql_3_144}"
    testFoldConst("${const_sql_3_144}")
    qt_sql_3_145_non_strict "${const_sql_3_145}"
    testFoldConst("${const_sql_3_145}")
    qt_sql_3_146_non_strict "${const_sql_3_146}"
    testFoldConst("${const_sql_3_146}")
    qt_sql_3_147_non_strict "${const_sql_3_147}"
    testFoldConst("${const_sql_3_147}")
    qt_sql_3_148_non_strict "${const_sql_3_148}"
    testFoldConst("${const_sql_3_148}")
    qt_sql_3_149_non_strict "${const_sql_3_149}"
    testFoldConst("${const_sql_3_149}")
    qt_sql_3_150_non_strict "${const_sql_3_150}"
    testFoldConst("${const_sql_3_150}")
    qt_sql_3_151_non_strict "${const_sql_3_151}"
    testFoldConst("${const_sql_3_151}")
    qt_sql_3_152_non_strict "${const_sql_3_152}"
    testFoldConst("${const_sql_3_152}")
    qt_sql_3_153_non_strict "${const_sql_3_153}"
    testFoldConst("${const_sql_3_153}")
    qt_sql_3_154_non_strict "${const_sql_3_154}"
    testFoldConst("${const_sql_3_154}")
    qt_sql_3_155_non_strict "${const_sql_3_155}"
    testFoldConst("${const_sql_3_155}")
    qt_sql_3_156_non_strict "${const_sql_3_156}"
    testFoldConst("${const_sql_3_156}")
    qt_sql_3_157_non_strict "${const_sql_3_157}"
    testFoldConst("${const_sql_3_157}")
    qt_sql_3_158_non_strict "${const_sql_3_158}"
    testFoldConst("${const_sql_3_158}")
    qt_sql_3_159_non_strict "${const_sql_3_159}"
    testFoldConst("${const_sql_3_159}")
    qt_sql_3_160_non_strict "${const_sql_3_160}"
    testFoldConst("${const_sql_3_160}")
    qt_sql_3_161_non_strict "${const_sql_3_161}"
    testFoldConst("${const_sql_3_161}")
    qt_sql_3_162_non_strict "${const_sql_3_162}"
    testFoldConst("${const_sql_3_162}")
    qt_sql_3_163_non_strict "${const_sql_3_163}"
    testFoldConst("${const_sql_3_163}")
    qt_sql_3_164_non_strict "${const_sql_3_164}"
    testFoldConst("${const_sql_3_164}")
    qt_sql_3_165_non_strict "${const_sql_3_165}"
    testFoldConst("${const_sql_3_165}")
    qt_sql_3_166_non_strict "${const_sql_3_166}"
    testFoldConst("${const_sql_3_166}")
    qt_sql_3_167_non_strict "${const_sql_3_167}"
    testFoldConst("${const_sql_3_167}")
    qt_sql_3_168_non_strict "${const_sql_3_168}"
    testFoldConst("${const_sql_3_168}")
    qt_sql_3_169_non_strict "${const_sql_3_169}"
    testFoldConst("${const_sql_3_169}")
    qt_sql_3_170_non_strict "${const_sql_3_170}"
    testFoldConst("${const_sql_3_170}")
    qt_sql_3_171_non_strict "${const_sql_3_171}"
    testFoldConst("${const_sql_3_171}")
    qt_sql_3_172_non_strict "${const_sql_3_172}"
    testFoldConst("${const_sql_3_172}")
    qt_sql_3_173_non_strict "${const_sql_3_173}"
    testFoldConst("${const_sql_3_173}")
    qt_sql_3_174_non_strict "${const_sql_3_174}"
    testFoldConst("${const_sql_3_174}")
    qt_sql_3_175_non_strict "${const_sql_3_175}"
    testFoldConst("${const_sql_3_175}")
    qt_sql_3_176_non_strict "${const_sql_3_176}"
    testFoldConst("${const_sql_3_176}")
    qt_sql_3_177_non_strict "${const_sql_3_177}"
    testFoldConst("${const_sql_3_177}")
    qt_sql_3_178_non_strict "${const_sql_3_178}"
    testFoldConst("${const_sql_3_178}")
    qt_sql_3_179_non_strict "${const_sql_3_179}"
    testFoldConst("${const_sql_3_179}")
    qt_sql_3_180_non_strict "${const_sql_3_180}"
    testFoldConst("${const_sql_3_180}")
    qt_sql_3_181_non_strict "${const_sql_3_181}"
    testFoldConst("${const_sql_3_181}")
    qt_sql_3_182_non_strict "${const_sql_3_182}"
    testFoldConst("${const_sql_3_182}")
    qt_sql_3_183_non_strict "${const_sql_3_183}"
    testFoldConst("${const_sql_3_183}")
    qt_sql_3_184_non_strict "${const_sql_3_184}"
    testFoldConst("${const_sql_3_184}")
    qt_sql_3_185_non_strict "${const_sql_3_185}"
    testFoldConst("${const_sql_3_185}")
    qt_sql_3_186_non_strict "${const_sql_3_186}"
    testFoldConst("${const_sql_3_186}")
    qt_sql_3_187_non_strict "${const_sql_3_187}"
    testFoldConst("${const_sql_3_187}")
    qt_sql_3_188_non_strict "${const_sql_3_188}"
    testFoldConst("${const_sql_3_188}")
    qt_sql_3_189_non_strict "${const_sql_3_189}"
    testFoldConst("${const_sql_3_189}")
    qt_sql_3_190_non_strict "${const_sql_3_190}"
    testFoldConst("${const_sql_3_190}")
    qt_sql_3_191_non_strict "${const_sql_3_191}"
    testFoldConst("${const_sql_3_191}")
    qt_sql_3_192_non_strict "${const_sql_3_192}"
    testFoldConst("${const_sql_3_192}")
    qt_sql_3_193_non_strict "${const_sql_3_193}"
    testFoldConst("${const_sql_3_193}")
    qt_sql_3_194_non_strict "${const_sql_3_194}"
    testFoldConst("${const_sql_3_194}")
}