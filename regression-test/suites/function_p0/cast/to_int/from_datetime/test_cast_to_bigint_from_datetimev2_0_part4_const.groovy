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


suite("test_cast_to_bigint_from_datetimev2_0_part4_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_4_0 = """select "0010-01-28 01:59:59", cast(cast("0010-01-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_0_strict "${const_sql_4_0}"
    testFoldConst("${const_sql_4_0}")
    def const_sql_4_1 = """select "0010-01-28 01:59:59", cast(cast("0010-01-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_1_strict "${const_sql_4_1}"
    testFoldConst("${const_sql_4_1}")
    def const_sql_4_2 = """select "0010-01-28 01:59:59", cast(cast("0010-01-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_2_strict "${const_sql_4_2}"
    testFoldConst("${const_sql_4_2}")
    def const_sql_4_3 = """select "0010-01-28 23:00:00", cast(cast("0010-01-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_3_strict "${const_sql_4_3}"
    testFoldConst("${const_sql_4_3}")
    def const_sql_4_4 = """select "0010-01-28 23:00:00", cast(cast("0010-01-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_4_strict "${const_sql_4_4}"
    testFoldConst("${const_sql_4_4}")
    def const_sql_4_5 = """select "0010-01-28 23:00:00", cast(cast("0010-01-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_5_strict "${const_sql_4_5}"
    testFoldConst("${const_sql_4_5}")
    def const_sql_4_6 = """select "0010-01-28 23:00:01", cast(cast("0010-01-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_6_strict "${const_sql_4_6}"
    testFoldConst("${const_sql_4_6}")
    def const_sql_4_7 = """select "0010-01-28 23:00:01", cast(cast("0010-01-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_7_strict "${const_sql_4_7}"
    testFoldConst("${const_sql_4_7}")
    def const_sql_4_8 = """select "0010-01-28 23:00:01", cast(cast("0010-01-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_8_strict "${const_sql_4_8}"
    testFoldConst("${const_sql_4_8}")
    def const_sql_4_9 = """select "0010-01-28 23:00:59", cast(cast("0010-01-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_9_strict "${const_sql_4_9}"
    testFoldConst("${const_sql_4_9}")
    def const_sql_4_10 = """select "0010-01-28 23:00:59", cast(cast("0010-01-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_10_strict "${const_sql_4_10}"
    testFoldConst("${const_sql_4_10}")
    def const_sql_4_11 = """select "0010-01-28 23:00:59", cast(cast("0010-01-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_11_strict "${const_sql_4_11}"
    testFoldConst("${const_sql_4_11}")
    def const_sql_4_12 = """select "0010-01-28 23:01:00", cast(cast("0010-01-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_12_strict "${const_sql_4_12}"
    testFoldConst("${const_sql_4_12}")
    def const_sql_4_13 = """select "0010-01-28 23:01:00", cast(cast("0010-01-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_13_strict "${const_sql_4_13}"
    testFoldConst("${const_sql_4_13}")
    def const_sql_4_14 = """select "0010-01-28 23:01:00", cast(cast("0010-01-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_14_strict "${const_sql_4_14}"
    testFoldConst("${const_sql_4_14}")
    def const_sql_4_15 = """select "0010-01-28 23:01:01", cast(cast("0010-01-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_15_strict "${const_sql_4_15}"
    testFoldConst("${const_sql_4_15}")
    def const_sql_4_16 = """select "0010-01-28 23:01:01", cast(cast("0010-01-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_16_strict "${const_sql_4_16}"
    testFoldConst("${const_sql_4_16}")
    def const_sql_4_17 = """select "0010-01-28 23:01:01", cast(cast("0010-01-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_17_strict "${const_sql_4_17}"
    testFoldConst("${const_sql_4_17}")
    def const_sql_4_18 = """select "0010-01-28 23:01:59", cast(cast("0010-01-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_18_strict "${const_sql_4_18}"
    testFoldConst("${const_sql_4_18}")
    def const_sql_4_19 = """select "0010-01-28 23:01:59", cast(cast("0010-01-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_19_strict "${const_sql_4_19}"
    testFoldConst("${const_sql_4_19}")
    def const_sql_4_20 = """select "0010-01-28 23:01:59", cast(cast("0010-01-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_20_strict "${const_sql_4_20}"
    testFoldConst("${const_sql_4_20}")
    def const_sql_4_21 = """select "0010-01-28 23:59:00", cast(cast("0010-01-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_21_strict "${const_sql_4_21}"
    testFoldConst("${const_sql_4_21}")
    def const_sql_4_22 = """select "0010-01-28 23:59:00", cast(cast("0010-01-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_22_strict "${const_sql_4_22}"
    testFoldConst("${const_sql_4_22}")
    def const_sql_4_23 = """select "0010-01-28 23:59:00", cast(cast("0010-01-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_23_strict "${const_sql_4_23}"
    testFoldConst("${const_sql_4_23}")
    def const_sql_4_24 = """select "0010-01-28 23:59:01", cast(cast("0010-01-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_24_strict "${const_sql_4_24}"
    testFoldConst("${const_sql_4_24}")
    def const_sql_4_25 = """select "0010-01-28 23:59:01", cast(cast("0010-01-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_25_strict "${const_sql_4_25}"
    testFoldConst("${const_sql_4_25}")
    def const_sql_4_26 = """select "0010-01-28 23:59:01", cast(cast("0010-01-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_26_strict "${const_sql_4_26}"
    testFoldConst("${const_sql_4_26}")
    def const_sql_4_27 = """select "0010-01-28 23:59:59", cast(cast("0010-01-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_27_strict "${const_sql_4_27}"
    testFoldConst("${const_sql_4_27}")
    def const_sql_4_28 = """select "0010-01-28 23:59:59", cast(cast("0010-01-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_28_strict "${const_sql_4_28}"
    testFoldConst("${const_sql_4_28}")
    def const_sql_4_29 = """select "0010-01-28 23:59:59", cast(cast("0010-01-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_29_strict "${const_sql_4_29}"
    testFoldConst("${const_sql_4_29}")
    def const_sql_4_30 = """select "0010-12-01 00:00:00", cast(cast("0010-12-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_30_strict "${const_sql_4_30}"
    testFoldConst("${const_sql_4_30}")
    def const_sql_4_31 = """select "0010-12-01 00:00:00", cast(cast("0010-12-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_31_strict "${const_sql_4_31}"
    testFoldConst("${const_sql_4_31}")
    def const_sql_4_32 = """select "0010-12-01 00:00:00", cast(cast("0010-12-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_32_strict "${const_sql_4_32}"
    testFoldConst("${const_sql_4_32}")
    def const_sql_4_33 = """select "0010-12-01 00:00:01", cast(cast("0010-12-01 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_33_strict "${const_sql_4_33}"
    testFoldConst("${const_sql_4_33}")
    def const_sql_4_34 = """select "0010-12-01 00:00:01", cast(cast("0010-12-01 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_34_strict "${const_sql_4_34}"
    testFoldConst("${const_sql_4_34}")
    def const_sql_4_35 = """select "0010-12-01 00:00:01", cast(cast("0010-12-01 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_35_strict "${const_sql_4_35}"
    testFoldConst("${const_sql_4_35}")
    def const_sql_4_36 = """select "0010-12-01 00:00:59", cast(cast("0010-12-01 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_36_strict "${const_sql_4_36}"
    testFoldConst("${const_sql_4_36}")
    def const_sql_4_37 = """select "0010-12-01 00:00:59", cast(cast("0010-12-01 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_37_strict "${const_sql_4_37}"
    testFoldConst("${const_sql_4_37}")
    def const_sql_4_38 = """select "0010-12-01 00:00:59", cast(cast("0010-12-01 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_38_strict "${const_sql_4_38}"
    testFoldConst("${const_sql_4_38}")
    def const_sql_4_39 = """select "0010-12-01 00:01:00", cast(cast("0010-12-01 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_39_strict "${const_sql_4_39}"
    testFoldConst("${const_sql_4_39}")
    def const_sql_4_40 = """select "0010-12-01 00:01:00", cast(cast("0010-12-01 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_40_strict "${const_sql_4_40}"
    testFoldConst("${const_sql_4_40}")
    def const_sql_4_41 = """select "0010-12-01 00:01:00", cast(cast("0010-12-01 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_41_strict "${const_sql_4_41}"
    testFoldConst("${const_sql_4_41}")
    def const_sql_4_42 = """select "0010-12-01 00:01:01", cast(cast("0010-12-01 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_42_strict "${const_sql_4_42}"
    testFoldConst("${const_sql_4_42}")
    def const_sql_4_43 = """select "0010-12-01 00:01:01", cast(cast("0010-12-01 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_43_strict "${const_sql_4_43}"
    testFoldConst("${const_sql_4_43}")
    def const_sql_4_44 = """select "0010-12-01 00:01:01", cast(cast("0010-12-01 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_44_strict "${const_sql_4_44}"
    testFoldConst("${const_sql_4_44}")
    def const_sql_4_45 = """select "0010-12-01 00:01:59", cast(cast("0010-12-01 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_45_strict "${const_sql_4_45}"
    testFoldConst("${const_sql_4_45}")
    def const_sql_4_46 = """select "0010-12-01 00:01:59", cast(cast("0010-12-01 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_46_strict "${const_sql_4_46}"
    testFoldConst("${const_sql_4_46}")
    def const_sql_4_47 = """select "0010-12-01 00:01:59", cast(cast("0010-12-01 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_47_strict "${const_sql_4_47}"
    testFoldConst("${const_sql_4_47}")
    def const_sql_4_48 = """select "0010-12-01 00:59:00", cast(cast("0010-12-01 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_48_strict "${const_sql_4_48}"
    testFoldConst("${const_sql_4_48}")
    def const_sql_4_49 = """select "0010-12-01 00:59:00", cast(cast("0010-12-01 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_49_strict "${const_sql_4_49}"
    testFoldConst("${const_sql_4_49}")
    def const_sql_4_50 = """select "0010-12-01 00:59:00", cast(cast("0010-12-01 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_50_strict "${const_sql_4_50}"
    testFoldConst("${const_sql_4_50}")
    def const_sql_4_51 = """select "0010-12-01 00:59:01", cast(cast("0010-12-01 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_51_strict "${const_sql_4_51}"
    testFoldConst("${const_sql_4_51}")
    def const_sql_4_52 = """select "0010-12-01 00:59:01", cast(cast("0010-12-01 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_52_strict "${const_sql_4_52}"
    testFoldConst("${const_sql_4_52}")
    def const_sql_4_53 = """select "0010-12-01 00:59:01", cast(cast("0010-12-01 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_53_strict "${const_sql_4_53}"
    testFoldConst("${const_sql_4_53}")
    def const_sql_4_54 = """select "0010-12-01 00:59:59", cast(cast("0010-12-01 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_54_strict "${const_sql_4_54}"
    testFoldConst("${const_sql_4_54}")
    def const_sql_4_55 = """select "0010-12-01 00:59:59", cast(cast("0010-12-01 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_55_strict "${const_sql_4_55}"
    testFoldConst("${const_sql_4_55}")
    def const_sql_4_56 = """select "0010-12-01 00:59:59", cast(cast("0010-12-01 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_56_strict "${const_sql_4_56}"
    testFoldConst("${const_sql_4_56}")
    def const_sql_4_57 = """select "0010-12-01 01:00:00", cast(cast("0010-12-01 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_57_strict "${const_sql_4_57}"
    testFoldConst("${const_sql_4_57}")
    def const_sql_4_58 = """select "0010-12-01 01:00:00", cast(cast("0010-12-01 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_58_strict "${const_sql_4_58}"
    testFoldConst("${const_sql_4_58}")
    def const_sql_4_59 = """select "0010-12-01 01:00:00", cast(cast("0010-12-01 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_59_strict "${const_sql_4_59}"
    testFoldConst("${const_sql_4_59}")
    def const_sql_4_60 = """select "0010-12-01 01:00:01", cast(cast("0010-12-01 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_60_strict "${const_sql_4_60}"
    testFoldConst("${const_sql_4_60}")
    def const_sql_4_61 = """select "0010-12-01 01:00:01", cast(cast("0010-12-01 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_61_strict "${const_sql_4_61}"
    testFoldConst("${const_sql_4_61}")
    def const_sql_4_62 = """select "0010-12-01 01:00:01", cast(cast("0010-12-01 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_62_strict "${const_sql_4_62}"
    testFoldConst("${const_sql_4_62}")
    def const_sql_4_63 = """select "0010-12-01 01:00:59", cast(cast("0010-12-01 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_63_strict "${const_sql_4_63}"
    testFoldConst("${const_sql_4_63}")
    def const_sql_4_64 = """select "0010-12-01 01:00:59", cast(cast("0010-12-01 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_64_strict "${const_sql_4_64}"
    testFoldConst("${const_sql_4_64}")
    def const_sql_4_65 = """select "0010-12-01 01:00:59", cast(cast("0010-12-01 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_65_strict "${const_sql_4_65}"
    testFoldConst("${const_sql_4_65}")
    def const_sql_4_66 = """select "0010-12-01 01:01:00", cast(cast("0010-12-01 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_66_strict "${const_sql_4_66}"
    testFoldConst("${const_sql_4_66}")
    def const_sql_4_67 = """select "0010-12-01 01:01:00", cast(cast("0010-12-01 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_67_strict "${const_sql_4_67}"
    testFoldConst("${const_sql_4_67}")
    def const_sql_4_68 = """select "0010-12-01 01:01:00", cast(cast("0010-12-01 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_68_strict "${const_sql_4_68}"
    testFoldConst("${const_sql_4_68}")
    def const_sql_4_69 = """select "0010-12-01 01:01:01", cast(cast("0010-12-01 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_69_strict "${const_sql_4_69}"
    testFoldConst("${const_sql_4_69}")
    def const_sql_4_70 = """select "0010-12-01 01:01:01", cast(cast("0010-12-01 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_70_strict "${const_sql_4_70}"
    testFoldConst("${const_sql_4_70}")
    def const_sql_4_71 = """select "0010-12-01 01:01:01", cast(cast("0010-12-01 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_71_strict "${const_sql_4_71}"
    testFoldConst("${const_sql_4_71}")
    def const_sql_4_72 = """select "0010-12-01 01:01:59", cast(cast("0010-12-01 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_72_strict "${const_sql_4_72}"
    testFoldConst("${const_sql_4_72}")
    def const_sql_4_73 = """select "0010-12-01 01:01:59", cast(cast("0010-12-01 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_73_strict "${const_sql_4_73}"
    testFoldConst("${const_sql_4_73}")
    def const_sql_4_74 = """select "0010-12-01 01:01:59", cast(cast("0010-12-01 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_74_strict "${const_sql_4_74}"
    testFoldConst("${const_sql_4_74}")
    def const_sql_4_75 = """select "0010-12-01 01:59:00", cast(cast("0010-12-01 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_75_strict "${const_sql_4_75}"
    testFoldConst("${const_sql_4_75}")
    def const_sql_4_76 = """select "0010-12-01 01:59:00", cast(cast("0010-12-01 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_76_strict "${const_sql_4_76}"
    testFoldConst("${const_sql_4_76}")
    def const_sql_4_77 = """select "0010-12-01 01:59:00", cast(cast("0010-12-01 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_77_strict "${const_sql_4_77}"
    testFoldConst("${const_sql_4_77}")
    def const_sql_4_78 = """select "0010-12-01 01:59:01", cast(cast("0010-12-01 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_78_strict "${const_sql_4_78}"
    testFoldConst("${const_sql_4_78}")
    def const_sql_4_79 = """select "0010-12-01 01:59:01", cast(cast("0010-12-01 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_79_strict "${const_sql_4_79}"
    testFoldConst("${const_sql_4_79}")
    def const_sql_4_80 = """select "0010-12-01 01:59:01", cast(cast("0010-12-01 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_80_strict "${const_sql_4_80}"
    testFoldConst("${const_sql_4_80}")
    def const_sql_4_81 = """select "0010-12-01 01:59:59", cast(cast("0010-12-01 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_81_strict "${const_sql_4_81}"
    testFoldConst("${const_sql_4_81}")
    def const_sql_4_82 = """select "0010-12-01 01:59:59", cast(cast("0010-12-01 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_82_strict "${const_sql_4_82}"
    testFoldConst("${const_sql_4_82}")
    def const_sql_4_83 = """select "0010-12-01 01:59:59", cast(cast("0010-12-01 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_83_strict "${const_sql_4_83}"
    testFoldConst("${const_sql_4_83}")
    def const_sql_4_84 = """select "0010-12-01 23:00:00", cast(cast("0010-12-01 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_84_strict "${const_sql_4_84}"
    testFoldConst("${const_sql_4_84}")
    def const_sql_4_85 = """select "0010-12-01 23:00:00", cast(cast("0010-12-01 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_85_strict "${const_sql_4_85}"
    testFoldConst("${const_sql_4_85}")
    def const_sql_4_86 = """select "0010-12-01 23:00:00", cast(cast("0010-12-01 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_86_strict "${const_sql_4_86}"
    testFoldConst("${const_sql_4_86}")
    def const_sql_4_87 = """select "0010-12-01 23:00:01", cast(cast("0010-12-01 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_87_strict "${const_sql_4_87}"
    testFoldConst("${const_sql_4_87}")
    def const_sql_4_88 = """select "0010-12-01 23:00:01", cast(cast("0010-12-01 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_88_strict "${const_sql_4_88}"
    testFoldConst("${const_sql_4_88}")
    def const_sql_4_89 = """select "0010-12-01 23:00:01", cast(cast("0010-12-01 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_89_strict "${const_sql_4_89}"
    testFoldConst("${const_sql_4_89}")
    def const_sql_4_90 = """select "0010-12-01 23:00:59", cast(cast("0010-12-01 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_90_strict "${const_sql_4_90}"
    testFoldConst("${const_sql_4_90}")
    def const_sql_4_91 = """select "0010-12-01 23:00:59", cast(cast("0010-12-01 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_91_strict "${const_sql_4_91}"
    testFoldConst("${const_sql_4_91}")
    def const_sql_4_92 = """select "0010-12-01 23:00:59", cast(cast("0010-12-01 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_92_strict "${const_sql_4_92}"
    testFoldConst("${const_sql_4_92}")
    def const_sql_4_93 = """select "0010-12-01 23:01:00", cast(cast("0010-12-01 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_93_strict "${const_sql_4_93}"
    testFoldConst("${const_sql_4_93}")
    def const_sql_4_94 = """select "0010-12-01 23:01:00", cast(cast("0010-12-01 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_94_strict "${const_sql_4_94}"
    testFoldConst("${const_sql_4_94}")
    def const_sql_4_95 = """select "0010-12-01 23:01:00", cast(cast("0010-12-01 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_95_strict "${const_sql_4_95}"
    testFoldConst("${const_sql_4_95}")
    def const_sql_4_96 = """select "0010-12-01 23:01:01", cast(cast("0010-12-01 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_96_strict "${const_sql_4_96}"
    testFoldConst("${const_sql_4_96}")
    def const_sql_4_97 = """select "0010-12-01 23:01:01", cast(cast("0010-12-01 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_97_strict "${const_sql_4_97}"
    testFoldConst("${const_sql_4_97}")
    def const_sql_4_98 = """select "0010-12-01 23:01:01", cast(cast("0010-12-01 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_98_strict "${const_sql_4_98}"
    testFoldConst("${const_sql_4_98}")
    def const_sql_4_99 = """select "0010-12-01 23:01:59", cast(cast("0010-12-01 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_99_strict "${const_sql_4_99}"
    testFoldConst("${const_sql_4_99}")
    def const_sql_4_100 = """select "0010-12-01 23:01:59", cast(cast("0010-12-01 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_100_strict "${const_sql_4_100}"
    testFoldConst("${const_sql_4_100}")
    def const_sql_4_101 = """select "0010-12-01 23:01:59", cast(cast("0010-12-01 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_101_strict "${const_sql_4_101}"
    testFoldConst("${const_sql_4_101}")
    def const_sql_4_102 = """select "0010-12-01 23:59:00", cast(cast("0010-12-01 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_102_strict "${const_sql_4_102}"
    testFoldConst("${const_sql_4_102}")
    def const_sql_4_103 = """select "0010-12-01 23:59:00", cast(cast("0010-12-01 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_103_strict "${const_sql_4_103}"
    testFoldConst("${const_sql_4_103}")
    def const_sql_4_104 = """select "0010-12-01 23:59:00", cast(cast("0010-12-01 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_104_strict "${const_sql_4_104}"
    testFoldConst("${const_sql_4_104}")
    def const_sql_4_105 = """select "0010-12-01 23:59:01", cast(cast("0010-12-01 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_105_strict "${const_sql_4_105}"
    testFoldConst("${const_sql_4_105}")
    def const_sql_4_106 = """select "0010-12-01 23:59:01", cast(cast("0010-12-01 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_106_strict "${const_sql_4_106}"
    testFoldConst("${const_sql_4_106}")
    def const_sql_4_107 = """select "0010-12-01 23:59:01", cast(cast("0010-12-01 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_107_strict "${const_sql_4_107}"
    testFoldConst("${const_sql_4_107}")
    def const_sql_4_108 = """select "0010-12-01 23:59:59", cast(cast("0010-12-01 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_108_strict "${const_sql_4_108}"
    testFoldConst("${const_sql_4_108}")
    def const_sql_4_109 = """select "0010-12-01 23:59:59", cast(cast("0010-12-01 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_109_strict "${const_sql_4_109}"
    testFoldConst("${const_sql_4_109}")
    def const_sql_4_110 = """select "0010-12-01 23:59:59", cast(cast("0010-12-01 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_110_strict "${const_sql_4_110}"
    testFoldConst("${const_sql_4_110}")
    def const_sql_4_111 = """select "0010-12-28 00:00:00", cast(cast("0010-12-28 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_111_strict "${const_sql_4_111}"
    testFoldConst("${const_sql_4_111}")
    def const_sql_4_112 = """select "0010-12-28 00:00:00", cast(cast("0010-12-28 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_112_strict "${const_sql_4_112}"
    testFoldConst("${const_sql_4_112}")
    def const_sql_4_113 = """select "0010-12-28 00:00:00", cast(cast("0010-12-28 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_113_strict "${const_sql_4_113}"
    testFoldConst("${const_sql_4_113}")
    def const_sql_4_114 = """select "0010-12-28 00:00:01", cast(cast("0010-12-28 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_114_strict "${const_sql_4_114}"
    testFoldConst("${const_sql_4_114}")
    def const_sql_4_115 = """select "0010-12-28 00:00:01", cast(cast("0010-12-28 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_115_strict "${const_sql_4_115}"
    testFoldConst("${const_sql_4_115}")
    def const_sql_4_116 = """select "0010-12-28 00:00:01", cast(cast("0010-12-28 00:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_116_strict "${const_sql_4_116}"
    testFoldConst("${const_sql_4_116}")
    def const_sql_4_117 = """select "0010-12-28 00:00:59", cast(cast("0010-12-28 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_117_strict "${const_sql_4_117}"
    testFoldConst("${const_sql_4_117}")
    def const_sql_4_118 = """select "0010-12-28 00:00:59", cast(cast("0010-12-28 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_118_strict "${const_sql_4_118}"
    testFoldConst("${const_sql_4_118}")
    def const_sql_4_119 = """select "0010-12-28 00:00:59", cast(cast("0010-12-28 00:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_119_strict "${const_sql_4_119}"
    testFoldConst("${const_sql_4_119}")
    def const_sql_4_120 = """select "0010-12-28 00:01:00", cast(cast("0010-12-28 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_120_strict "${const_sql_4_120}"
    testFoldConst("${const_sql_4_120}")
    def const_sql_4_121 = """select "0010-12-28 00:01:00", cast(cast("0010-12-28 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_121_strict "${const_sql_4_121}"
    testFoldConst("${const_sql_4_121}")
    def const_sql_4_122 = """select "0010-12-28 00:01:00", cast(cast("0010-12-28 00:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_122_strict "${const_sql_4_122}"
    testFoldConst("${const_sql_4_122}")
    def const_sql_4_123 = """select "0010-12-28 00:01:01", cast(cast("0010-12-28 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_123_strict "${const_sql_4_123}"
    testFoldConst("${const_sql_4_123}")
    def const_sql_4_124 = """select "0010-12-28 00:01:01", cast(cast("0010-12-28 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_124_strict "${const_sql_4_124}"
    testFoldConst("${const_sql_4_124}")
    def const_sql_4_125 = """select "0010-12-28 00:01:01", cast(cast("0010-12-28 00:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_125_strict "${const_sql_4_125}"
    testFoldConst("${const_sql_4_125}")
    def const_sql_4_126 = """select "0010-12-28 00:01:59", cast(cast("0010-12-28 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_126_strict "${const_sql_4_126}"
    testFoldConst("${const_sql_4_126}")
    def const_sql_4_127 = """select "0010-12-28 00:01:59", cast(cast("0010-12-28 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_127_strict "${const_sql_4_127}"
    testFoldConst("${const_sql_4_127}")
    def const_sql_4_128 = """select "0010-12-28 00:01:59", cast(cast("0010-12-28 00:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_128_strict "${const_sql_4_128}"
    testFoldConst("${const_sql_4_128}")
    def const_sql_4_129 = """select "0010-12-28 00:59:00", cast(cast("0010-12-28 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_129_strict "${const_sql_4_129}"
    testFoldConst("${const_sql_4_129}")
    def const_sql_4_130 = """select "0010-12-28 00:59:00", cast(cast("0010-12-28 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_130_strict "${const_sql_4_130}"
    testFoldConst("${const_sql_4_130}")
    def const_sql_4_131 = """select "0010-12-28 00:59:00", cast(cast("0010-12-28 00:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_131_strict "${const_sql_4_131}"
    testFoldConst("${const_sql_4_131}")
    def const_sql_4_132 = """select "0010-12-28 00:59:01", cast(cast("0010-12-28 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_132_strict "${const_sql_4_132}"
    testFoldConst("${const_sql_4_132}")
    def const_sql_4_133 = """select "0010-12-28 00:59:01", cast(cast("0010-12-28 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_133_strict "${const_sql_4_133}"
    testFoldConst("${const_sql_4_133}")
    def const_sql_4_134 = """select "0010-12-28 00:59:01", cast(cast("0010-12-28 00:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_134_strict "${const_sql_4_134}"
    testFoldConst("${const_sql_4_134}")
    def const_sql_4_135 = """select "0010-12-28 00:59:59", cast(cast("0010-12-28 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_135_strict "${const_sql_4_135}"
    testFoldConst("${const_sql_4_135}")
    def const_sql_4_136 = """select "0010-12-28 00:59:59", cast(cast("0010-12-28 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_136_strict "${const_sql_4_136}"
    testFoldConst("${const_sql_4_136}")
    def const_sql_4_137 = """select "0010-12-28 00:59:59", cast(cast("0010-12-28 00:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_137_strict "${const_sql_4_137}"
    testFoldConst("${const_sql_4_137}")
    def const_sql_4_138 = """select "0010-12-28 01:00:00", cast(cast("0010-12-28 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_138_strict "${const_sql_4_138}"
    testFoldConst("${const_sql_4_138}")
    def const_sql_4_139 = """select "0010-12-28 01:00:00", cast(cast("0010-12-28 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_139_strict "${const_sql_4_139}"
    testFoldConst("${const_sql_4_139}")
    def const_sql_4_140 = """select "0010-12-28 01:00:00", cast(cast("0010-12-28 01:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_140_strict "${const_sql_4_140}"
    testFoldConst("${const_sql_4_140}")
    def const_sql_4_141 = """select "0010-12-28 01:00:01", cast(cast("0010-12-28 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_141_strict "${const_sql_4_141}"
    testFoldConst("${const_sql_4_141}")
    def const_sql_4_142 = """select "0010-12-28 01:00:01", cast(cast("0010-12-28 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_142_strict "${const_sql_4_142}"
    testFoldConst("${const_sql_4_142}")
    def const_sql_4_143 = """select "0010-12-28 01:00:01", cast(cast("0010-12-28 01:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_143_strict "${const_sql_4_143}"
    testFoldConst("${const_sql_4_143}")
    def const_sql_4_144 = """select "0010-12-28 01:00:59", cast(cast("0010-12-28 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_144_strict "${const_sql_4_144}"
    testFoldConst("${const_sql_4_144}")
    def const_sql_4_145 = """select "0010-12-28 01:00:59", cast(cast("0010-12-28 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_145_strict "${const_sql_4_145}"
    testFoldConst("${const_sql_4_145}")
    def const_sql_4_146 = """select "0010-12-28 01:00:59", cast(cast("0010-12-28 01:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_146_strict "${const_sql_4_146}"
    testFoldConst("${const_sql_4_146}")
    def const_sql_4_147 = """select "0010-12-28 01:01:00", cast(cast("0010-12-28 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_147_strict "${const_sql_4_147}"
    testFoldConst("${const_sql_4_147}")
    def const_sql_4_148 = """select "0010-12-28 01:01:00", cast(cast("0010-12-28 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_148_strict "${const_sql_4_148}"
    testFoldConst("${const_sql_4_148}")
    def const_sql_4_149 = """select "0010-12-28 01:01:00", cast(cast("0010-12-28 01:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_149_strict "${const_sql_4_149}"
    testFoldConst("${const_sql_4_149}")
    def const_sql_4_150 = """select "0010-12-28 01:01:01", cast(cast("0010-12-28 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_150_strict "${const_sql_4_150}"
    testFoldConst("${const_sql_4_150}")
    def const_sql_4_151 = """select "0010-12-28 01:01:01", cast(cast("0010-12-28 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_151_strict "${const_sql_4_151}"
    testFoldConst("${const_sql_4_151}")
    def const_sql_4_152 = """select "0010-12-28 01:01:01", cast(cast("0010-12-28 01:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_152_strict "${const_sql_4_152}"
    testFoldConst("${const_sql_4_152}")
    def const_sql_4_153 = """select "0010-12-28 01:01:59", cast(cast("0010-12-28 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_153_strict "${const_sql_4_153}"
    testFoldConst("${const_sql_4_153}")
    def const_sql_4_154 = """select "0010-12-28 01:01:59", cast(cast("0010-12-28 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_154_strict "${const_sql_4_154}"
    testFoldConst("${const_sql_4_154}")
    def const_sql_4_155 = """select "0010-12-28 01:01:59", cast(cast("0010-12-28 01:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_155_strict "${const_sql_4_155}"
    testFoldConst("${const_sql_4_155}")
    def const_sql_4_156 = """select "0010-12-28 01:59:00", cast(cast("0010-12-28 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_156_strict "${const_sql_4_156}"
    testFoldConst("${const_sql_4_156}")
    def const_sql_4_157 = """select "0010-12-28 01:59:00", cast(cast("0010-12-28 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_157_strict "${const_sql_4_157}"
    testFoldConst("${const_sql_4_157}")
    def const_sql_4_158 = """select "0010-12-28 01:59:00", cast(cast("0010-12-28 01:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_158_strict "${const_sql_4_158}"
    testFoldConst("${const_sql_4_158}")
    def const_sql_4_159 = """select "0010-12-28 01:59:01", cast(cast("0010-12-28 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_159_strict "${const_sql_4_159}"
    testFoldConst("${const_sql_4_159}")
    def const_sql_4_160 = """select "0010-12-28 01:59:01", cast(cast("0010-12-28 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_160_strict "${const_sql_4_160}"
    testFoldConst("${const_sql_4_160}")
    def const_sql_4_161 = """select "0010-12-28 01:59:01", cast(cast("0010-12-28 01:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_161_strict "${const_sql_4_161}"
    testFoldConst("${const_sql_4_161}")
    def const_sql_4_162 = """select "0010-12-28 01:59:59", cast(cast("0010-12-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_162_strict "${const_sql_4_162}"
    testFoldConst("${const_sql_4_162}")
    def const_sql_4_163 = """select "0010-12-28 01:59:59", cast(cast("0010-12-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_163_strict "${const_sql_4_163}"
    testFoldConst("${const_sql_4_163}")
    def const_sql_4_164 = """select "0010-12-28 01:59:59", cast(cast("0010-12-28 01:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_164_strict "${const_sql_4_164}"
    testFoldConst("${const_sql_4_164}")
    def const_sql_4_165 = """select "0010-12-28 23:00:00", cast(cast("0010-12-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_165_strict "${const_sql_4_165}"
    testFoldConst("${const_sql_4_165}")
    def const_sql_4_166 = """select "0010-12-28 23:00:00", cast(cast("0010-12-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_166_strict "${const_sql_4_166}"
    testFoldConst("${const_sql_4_166}")
    def const_sql_4_167 = """select "0010-12-28 23:00:00", cast(cast("0010-12-28 23:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_167_strict "${const_sql_4_167}"
    testFoldConst("${const_sql_4_167}")
    def const_sql_4_168 = """select "0010-12-28 23:00:01", cast(cast("0010-12-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_168_strict "${const_sql_4_168}"
    testFoldConst("${const_sql_4_168}")
    def const_sql_4_169 = """select "0010-12-28 23:00:01", cast(cast("0010-12-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_169_strict "${const_sql_4_169}"
    testFoldConst("${const_sql_4_169}")
    def const_sql_4_170 = """select "0010-12-28 23:00:01", cast(cast("0010-12-28 23:00:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_170_strict "${const_sql_4_170}"
    testFoldConst("${const_sql_4_170}")
    def const_sql_4_171 = """select "0010-12-28 23:00:59", cast(cast("0010-12-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_171_strict "${const_sql_4_171}"
    testFoldConst("${const_sql_4_171}")
    def const_sql_4_172 = """select "0010-12-28 23:00:59", cast(cast("0010-12-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_172_strict "${const_sql_4_172}"
    testFoldConst("${const_sql_4_172}")
    def const_sql_4_173 = """select "0010-12-28 23:00:59", cast(cast("0010-12-28 23:00:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_173_strict "${const_sql_4_173}"
    testFoldConst("${const_sql_4_173}")
    def const_sql_4_174 = """select "0010-12-28 23:01:00", cast(cast("0010-12-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_174_strict "${const_sql_4_174}"
    testFoldConst("${const_sql_4_174}")
    def const_sql_4_175 = """select "0010-12-28 23:01:00", cast(cast("0010-12-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_175_strict "${const_sql_4_175}"
    testFoldConst("${const_sql_4_175}")
    def const_sql_4_176 = """select "0010-12-28 23:01:00", cast(cast("0010-12-28 23:01:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_176_strict "${const_sql_4_176}"
    testFoldConst("${const_sql_4_176}")
    def const_sql_4_177 = """select "0010-12-28 23:01:01", cast(cast("0010-12-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_177_strict "${const_sql_4_177}"
    testFoldConst("${const_sql_4_177}")
    def const_sql_4_178 = """select "0010-12-28 23:01:01", cast(cast("0010-12-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_178_strict "${const_sql_4_178}"
    testFoldConst("${const_sql_4_178}")
    def const_sql_4_179 = """select "0010-12-28 23:01:01", cast(cast("0010-12-28 23:01:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_179_strict "${const_sql_4_179}"
    testFoldConst("${const_sql_4_179}")
    def const_sql_4_180 = """select "0010-12-28 23:01:59", cast(cast("0010-12-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_180_strict "${const_sql_4_180}"
    testFoldConst("${const_sql_4_180}")
    def const_sql_4_181 = """select "0010-12-28 23:01:59", cast(cast("0010-12-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_181_strict "${const_sql_4_181}"
    testFoldConst("${const_sql_4_181}")
    def const_sql_4_182 = """select "0010-12-28 23:01:59", cast(cast("0010-12-28 23:01:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_182_strict "${const_sql_4_182}"
    testFoldConst("${const_sql_4_182}")
    def const_sql_4_183 = """select "0010-12-28 23:59:00", cast(cast("0010-12-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_183_strict "${const_sql_4_183}"
    testFoldConst("${const_sql_4_183}")
    def const_sql_4_184 = """select "0010-12-28 23:59:00", cast(cast("0010-12-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_184_strict "${const_sql_4_184}"
    testFoldConst("${const_sql_4_184}")
    def const_sql_4_185 = """select "0010-12-28 23:59:00", cast(cast("0010-12-28 23:59:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_185_strict "${const_sql_4_185}"
    testFoldConst("${const_sql_4_185}")
    def const_sql_4_186 = """select "0010-12-28 23:59:01", cast(cast("0010-12-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_186_strict "${const_sql_4_186}"
    testFoldConst("${const_sql_4_186}")
    def const_sql_4_187 = """select "0010-12-28 23:59:01", cast(cast("0010-12-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_187_strict "${const_sql_4_187}"
    testFoldConst("${const_sql_4_187}")
    def const_sql_4_188 = """select "0010-12-28 23:59:01", cast(cast("0010-12-28 23:59:01" as datetimev2(0)) as bigint);"""
    qt_sql_4_188_strict "${const_sql_4_188}"
    testFoldConst("${const_sql_4_188}")
    def const_sql_4_189 = """select "0010-12-28 23:59:59", cast(cast("0010-12-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_189_strict "${const_sql_4_189}"
    testFoldConst("${const_sql_4_189}")
    def const_sql_4_190 = """select "0010-12-28 23:59:59", cast(cast("0010-12-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_190_strict "${const_sql_4_190}"
    testFoldConst("${const_sql_4_190}")
    def const_sql_4_191 = """select "0010-12-28 23:59:59", cast(cast("0010-12-28 23:59:59" as datetimev2(0)) as bigint);"""
    qt_sql_4_191_strict "${const_sql_4_191}"
    testFoldConst("${const_sql_4_191}")
    def const_sql_4_192 = """select "0100-01-01 00:00:00", cast(cast("0100-01-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_192_strict "${const_sql_4_192}"
    testFoldConst("${const_sql_4_192}")
    def const_sql_4_193 = """select "0100-01-01 00:00:00", cast(cast("0100-01-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_193_strict "${const_sql_4_193}"
    testFoldConst("${const_sql_4_193}")
    def const_sql_4_194 = """select "0100-01-01 00:00:00", cast(cast("0100-01-01 00:00:00" as datetimev2(0)) as bigint);"""
    qt_sql_4_194_strict "${const_sql_4_194}"
    testFoldConst("${const_sql_4_194}")

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
    qt_sql_4_128_non_strict "${const_sql_4_128}"
    testFoldConst("${const_sql_4_128}")
    qt_sql_4_129_non_strict "${const_sql_4_129}"
    testFoldConst("${const_sql_4_129}")
    qt_sql_4_130_non_strict "${const_sql_4_130}"
    testFoldConst("${const_sql_4_130}")
    qt_sql_4_131_non_strict "${const_sql_4_131}"
    testFoldConst("${const_sql_4_131}")
    qt_sql_4_132_non_strict "${const_sql_4_132}"
    testFoldConst("${const_sql_4_132}")
    qt_sql_4_133_non_strict "${const_sql_4_133}"
    testFoldConst("${const_sql_4_133}")
    qt_sql_4_134_non_strict "${const_sql_4_134}"
    testFoldConst("${const_sql_4_134}")
    qt_sql_4_135_non_strict "${const_sql_4_135}"
    testFoldConst("${const_sql_4_135}")
    qt_sql_4_136_non_strict "${const_sql_4_136}"
    testFoldConst("${const_sql_4_136}")
    qt_sql_4_137_non_strict "${const_sql_4_137}"
    testFoldConst("${const_sql_4_137}")
    qt_sql_4_138_non_strict "${const_sql_4_138}"
    testFoldConst("${const_sql_4_138}")
    qt_sql_4_139_non_strict "${const_sql_4_139}"
    testFoldConst("${const_sql_4_139}")
    qt_sql_4_140_non_strict "${const_sql_4_140}"
    testFoldConst("${const_sql_4_140}")
    qt_sql_4_141_non_strict "${const_sql_4_141}"
    testFoldConst("${const_sql_4_141}")
    qt_sql_4_142_non_strict "${const_sql_4_142}"
    testFoldConst("${const_sql_4_142}")
    qt_sql_4_143_non_strict "${const_sql_4_143}"
    testFoldConst("${const_sql_4_143}")
    qt_sql_4_144_non_strict "${const_sql_4_144}"
    testFoldConst("${const_sql_4_144}")
    qt_sql_4_145_non_strict "${const_sql_4_145}"
    testFoldConst("${const_sql_4_145}")
    qt_sql_4_146_non_strict "${const_sql_4_146}"
    testFoldConst("${const_sql_4_146}")
    qt_sql_4_147_non_strict "${const_sql_4_147}"
    testFoldConst("${const_sql_4_147}")
    qt_sql_4_148_non_strict "${const_sql_4_148}"
    testFoldConst("${const_sql_4_148}")
    qt_sql_4_149_non_strict "${const_sql_4_149}"
    testFoldConst("${const_sql_4_149}")
    qt_sql_4_150_non_strict "${const_sql_4_150}"
    testFoldConst("${const_sql_4_150}")
    qt_sql_4_151_non_strict "${const_sql_4_151}"
    testFoldConst("${const_sql_4_151}")
    qt_sql_4_152_non_strict "${const_sql_4_152}"
    testFoldConst("${const_sql_4_152}")
    qt_sql_4_153_non_strict "${const_sql_4_153}"
    testFoldConst("${const_sql_4_153}")
    qt_sql_4_154_non_strict "${const_sql_4_154}"
    testFoldConst("${const_sql_4_154}")
    qt_sql_4_155_non_strict "${const_sql_4_155}"
    testFoldConst("${const_sql_4_155}")
    qt_sql_4_156_non_strict "${const_sql_4_156}"
    testFoldConst("${const_sql_4_156}")
    qt_sql_4_157_non_strict "${const_sql_4_157}"
    testFoldConst("${const_sql_4_157}")
    qt_sql_4_158_non_strict "${const_sql_4_158}"
    testFoldConst("${const_sql_4_158}")
    qt_sql_4_159_non_strict "${const_sql_4_159}"
    testFoldConst("${const_sql_4_159}")
    qt_sql_4_160_non_strict "${const_sql_4_160}"
    testFoldConst("${const_sql_4_160}")
    qt_sql_4_161_non_strict "${const_sql_4_161}"
    testFoldConst("${const_sql_4_161}")
    qt_sql_4_162_non_strict "${const_sql_4_162}"
    testFoldConst("${const_sql_4_162}")
    qt_sql_4_163_non_strict "${const_sql_4_163}"
    testFoldConst("${const_sql_4_163}")
    qt_sql_4_164_non_strict "${const_sql_4_164}"
    testFoldConst("${const_sql_4_164}")
    qt_sql_4_165_non_strict "${const_sql_4_165}"
    testFoldConst("${const_sql_4_165}")
    qt_sql_4_166_non_strict "${const_sql_4_166}"
    testFoldConst("${const_sql_4_166}")
    qt_sql_4_167_non_strict "${const_sql_4_167}"
    testFoldConst("${const_sql_4_167}")
    qt_sql_4_168_non_strict "${const_sql_4_168}"
    testFoldConst("${const_sql_4_168}")
    qt_sql_4_169_non_strict "${const_sql_4_169}"
    testFoldConst("${const_sql_4_169}")
    qt_sql_4_170_non_strict "${const_sql_4_170}"
    testFoldConst("${const_sql_4_170}")
    qt_sql_4_171_non_strict "${const_sql_4_171}"
    testFoldConst("${const_sql_4_171}")
    qt_sql_4_172_non_strict "${const_sql_4_172}"
    testFoldConst("${const_sql_4_172}")
    qt_sql_4_173_non_strict "${const_sql_4_173}"
    testFoldConst("${const_sql_4_173}")
    qt_sql_4_174_non_strict "${const_sql_4_174}"
    testFoldConst("${const_sql_4_174}")
    qt_sql_4_175_non_strict "${const_sql_4_175}"
    testFoldConst("${const_sql_4_175}")
    qt_sql_4_176_non_strict "${const_sql_4_176}"
    testFoldConst("${const_sql_4_176}")
    qt_sql_4_177_non_strict "${const_sql_4_177}"
    testFoldConst("${const_sql_4_177}")
    qt_sql_4_178_non_strict "${const_sql_4_178}"
    testFoldConst("${const_sql_4_178}")
    qt_sql_4_179_non_strict "${const_sql_4_179}"
    testFoldConst("${const_sql_4_179}")
    qt_sql_4_180_non_strict "${const_sql_4_180}"
    testFoldConst("${const_sql_4_180}")
    qt_sql_4_181_non_strict "${const_sql_4_181}"
    testFoldConst("${const_sql_4_181}")
    qt_sql_4_182_non_strict "${const_sql_4_182}"
    testFoldConst("${const_sql_4_182}")
    qt_sql_4_183_non_strict "${const_sql_4_183}"
    testFoldConst("${const_sql_4_183}")
    qt_sql_4_184_non_strict "${const_sql_4_184}"
    testFoldConst("${const_sql_4_184}")
    qt_sql_4_185_non_strict "${const_sql_4_185}"
    testFoldConst("${const_sql_4_185}")
    qt_sql_4_186_non_strict "${const_sql_4_186}"
    testFoldConst("${const_sql_4_186}")
    qt_sql_4_187_non_strict "${const_sql_4_187}"
    testFoldConst("${const_sql_4_187}")
    qt_sql_4_188_non_strict "${const_sql_4_188}"
    testFoldConst("${const_sql_4_188}")
    qt_sql_4_189_non_strict "${const_sql_4_189}"
    testFoldConst("${const_sql_4_189}")
    qt_sql_4_190_non_strict "${const_sql_4_190}"
    testFoldConst("${const_sql_4_190}")
    qt_sql_4_191_non_strict "${const_sql_4_191}"
    testFoldConst("${const_sql_4_191}")
    qt_sql_4_192_non_strict "${const_sql_4_192}"
    testFoldConst("${const_sql_4_192}")
    qt_sql_4_193_non_strict "${const_sql_4_193}"
    testFoldConst("${const_sql_4_193}")
    qt_sql_4_194_non_strict "${const_sql_4_194}"
    testFoldConst("${const_sql_4_194}")
}