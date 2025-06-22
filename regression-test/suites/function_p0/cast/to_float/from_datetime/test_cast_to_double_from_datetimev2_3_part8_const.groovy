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


suite("test_cast_to_double_from_datetimev2_3_part8_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_8_0 = """select "2025-12-28 00:59:01.000", cast(cast("2025-12-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_0};"""
        exception ""
    }
    def const_sql_8_1 = """select "2025-12-28 00:59:01.000", cast(cast("2025-12-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_1};"""
        exception ""
    }
    def const_sql_8_2 = """select "2025-12-28 00:59:01.999", cast(cast("2025-12-28 00:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_2};"""
        exception ""
    }
    def const_sql_8_3 = """select "2025-12-28 00:59:59.000", cast(cast("2025-12-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_3};"""
        exception ""
    }
    def const_sql_8_4 = """select "2025-12-28 00:59:59.000", cast(cast("2025-12-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_4};"""
        exception ""
    }
    def const_sql_8_5 = """select "2025-12-28 00:59:59.999", cast(cast("2025-12-28 00:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_5};"""
        exception ""
    }
    def const_sql_8_6 = """select "2025-12-28 01:00:00.000", cast(cast("2025-12-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_6};"""
        exception ""
    }
    def const_sql_8_7 = """select "2025-12-28 01:00:00.000", cast(cast("2025-12-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_7};"""
        exception ""
    }
    def const_sql_8_8 = """select "2025-12-28 01:00:00.999", cast(cast("2025-12-28 01:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_8};"""
        exception ""
    }
    def const_sql_8_9 = """select "2025-12-28 01:00:01.000", cast(cast("2025-12-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_9};"""
        exception ""
    }
    def const_sql_8_10 = """select "2025-12-28 01:00:01.000", cast(cast("2025-12-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_10};"""
        exception ""
    }
    def const_sql_8_11 = """select "2025-12-28 01:00:01.999", cast(cast("2025-12-28 01:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_11};"""
        exception ""
    }
    def const_sql_8_12 = """select "2025-12-28 01:00:59.000", cast(cast("2025-12-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_12};"""
        exception ""
    }
    def const_sql_8_13 = """select "2025-12-28 01:00:59.000", cast(cast("2025-12-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_13};"""
        exception ""
    }
    def const_sql_8_14 = """select "2025-12-28 01:00:59.999", cast(cast("2025-12-28 01:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_14};"""
        exception ""
    }
    def const_sql_8_15 = """select "2025-12-28 01:01:00.000", cast(cast("2025-12-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_15};"""
        exception ""
    }
    def const_sql_8_16 = """select "2025-12-28 01:01:00.000", cast(cast("2025-12-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_16};"""
        exception ""
    }
    def const_sql_8_17 = """select "2025-12-28 01:01:00.999", cast(cast("2025-12-28 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_17};"""
        exception ""
    }
    def const_sql_8_18 = """select "2025-12-28 01:01:01.000", cast(cast("2025-12-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_18};"""
        exception ""
    }
    def const_sql_8_19 = """select "2025-12-28 01:01:01.000", cast(cast("2025-12-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_19};"""
        exception ""
    }
    def const_sql_8_20 = """select "2025-12-28 01:01:01.999", cast(cast("2025-12-28 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_20};"""
        exception ""
    }
    def const_sql_8_21 = """select "2025-12-28 01:01:59.000", cast(cast("2025-12-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_21};"""
        exception ""
    }
    def const_sql_8_22 = """select "2025-12-28 01:01:59.000", cast(cast("2025-12-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_22};"""
        exception ""
    }
    def const_sql_8_23 = """select "2025-12-28 01:01:59.999", cast(cast("2025-12-28 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_23};"""
        exception ""
    }
    def const_sql_8_24 = """select "2025-12-28 01:59:00.000", cast(cast("2025-12-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_24};"""
        exception ""
    }
    def const_sql_8_25 = """select "2025-12-28 01:59:00.000", cast(cast("2025-12-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_25};"""
        exception ""
    }
    def const_sql_8_26 = """select "2025-12-28 01:59:00.999", cast(cast("2025-12-28 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_26};"""
        exception ""
    }
    def const_sql_8_27 = """select "2025-12-28 01:59:01.000", cast(cast("2025-12-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_27};"""
        exception ""
    }
    def const_sql_8_28 = """select "2025-12-28 01:59:01.000", cast(cast("2025-12-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_28};"""
        exception ""
    }
    def const_sql_8_29 = """select "2025-12-28 01:59:01.999", cast(cast("2025-12-28 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_29};"""
        exception ""
    }
    def const_sql_8_30 = """select "2025-12-28 01:59:59.000", cast(cast("2025-12-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_30};"""
        exception ""
    }
    def const_sql_8_31 = """select "2025-12-28 01:59:59.000", cast(cast("2025-12-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_31};"""
        exception ""
    }
    def const_sql_8_32 = """select "2025-12-28 01:59:59.999", cast(cast("2025-12-28 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_32};"""
        exception ""
    }
    def const_sql_8_33 = """select "2025-12-28 23:00:00.000", cast(cast("2025-12-28 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_33};"""
        exception ""
    }
    def const_sql_8_34 = """select "2025-12-28 23:00:00.000", cast(cast("2025-12-28 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_34};"""
        exception ""
    }
    def const_sql_8_35 = """select "2025-12-28 23:00:00.999", cast(cast("2025-12-28 23:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_35};"""
        exception ""
    }
    def const_sql_8_36 = """select "2025-12-28 23:00:01.000", cast(cast("2025-12-28 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_36};"""
        exception ""
    }
    def const_sql_8_37 = """select "2025-12-28 23:00:01.000", cast(cast("2025-12-28 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_37};"""
        exception ""
    }
    def const_sql_8_38 = """select "2025-12-28 23:00:01.999", cast(cast("2025-12-28 23:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_38};"""
        exception ""
    }
    def const_sql_8_39 = """select "2025-12-28 23:00:59.000", cast(cast("2025-12-28 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_39};"""
        exception ""
    }
    def const_sql_8_40 = """select "2025-12-28 23:00:59.000", cast(cast("2025-12-28 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_40};"""
        exception ""
    }
    def const_sql_8_41 = """select "2025-12-28 23:00:59.999", cast(cast("2025-12-28 23:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_41};"""
        exception ""
    }
    def const_sql_8_42 = """select "2025-12-28 23:01:00.000", cast(cast("2025-12-28 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_42};"""
        exception ""
    }
    def const_sql_8_43 = """select "2025-12-28 23:01:00.000", cast(cast("2025-12-28 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_43};"""
        exception ""
    }
    def const_sql_8_44 = """select "2025-12-28 23:01:00.999", cast(cast("2025-12-28 23:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_44};"""
        exception ""
    }
    def const_sql_8_45 = """select "2025-12-28 23:01:01.000", cast(cast("2025-12-28 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_45};"""
        exception ""
    }
    def const_sql_8_46 = """select "2025-12-28 23:01:01.000", cast(cast("2025-12-28 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_46};"""
        exception ""
    }
    def const_sql_8_47 = """select "2025-12-28 23:01:01.999", cast(cast("2025-12-28 23:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_47};"""
        exception ""
    }
    def const_sql_8_48 = """select "2025-12-28 23:01:59.000", cast(cast("2025-12-28 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_48};"""
        exception ""
    }
    def const_sql_8_49 = """select "2025-12-28 23:01:59.000", cast(cast("2025-12-28 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_49};"""
        exception ""
    }
    def const_sql_8_50 = """select "2025-12-28 23:01:59.999", cast(cast("2025-12-28 23:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_50};"""
        exception ""
    }
    def const_sql_8_51 = """select "2025-12-28 23:59:00.000", cast(cast("2025-12-28 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_51};"""
        exception ""
    }
    def const_sql_8_52 = """select "2025-12-28 23:59:00.000", cast(cast("2025-12-28 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_52};"""
        exception ""
    }
    def const_sql_8_53 = """select "2025-12-28 23:59:00.999", cast(cast("2025-12-28 23:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_53};"""
        exception ""
    }
    def const_sql_8_54 = """select "2025-12-28 23:59:01.000", cast(cast("2025-12-28 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_54};"""
        exception ""
    }
    def const_sql_8_55 = """select "2025-12-28 23:59:01.000", cast(cast("2025-12-28 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_55};"""
        exception ""
    }
    def const_sql_8_56 = """select "2025-12-28 23:59:01.999", cast(cast("2025-12-28 23:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_56};"""
        exception ""
    }
    def const_sql_8_57 = """select "2025-12-28 23:59:59.000", cast(cast("2025-12-28 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_57};"""
        exception ""
    }
    def const_sql_8_58 = """select "2025-12-28 23:59:59.000", cast(cast("2025-12-28 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_58};"""
        exception ""
    }
    def const_sql_8_59 = """select "2025-12-28 23:59:59.999", cast(cast("2025-12-28 23:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_59};"""
        exception ""
    }
    def const_sql_8_60 = """select "9999-01-01 00:00:00.000", cast(cast("9999-01-01 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_60};"""
        exception ""
    }
    def const_sql_8_61 = """select "9999-01-01 00:00:00.000", cast(cast("9999-01-01 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_61};"""
        exception ""
    }
    def const_sql_8_62 = """select "9999-01-01 00:00:00.999", cast(cast("9999-01-01 00:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_62};"""
        exception ""
    }
    def const_sql_8_63 = """select "9999-01-01 00:00:01.000", cast(cast("9999-01-01 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_63};"""
        exception ""
    }
    def const_sql_8_64 = """select "9999-01-01 00:00:01.000", cast(cast("9999-01-01 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_64};"""
        exception ""
    }
    def const_sql_8_65 = """select "9999-01-01 00:00:01.999", cast(cast("9999-01-01 00:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_65};"""
        exception ""
    }
    def const_sql_8_66 = """select "9999-01-01 00:00:59.000", cast(cast("9999-01-01 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_66};"""
        exception ""
    }
    def const_sql_8_67 = """select "9999-01-01 00:00:59.000", cast(cast("9999-01-01 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_67};"""
        exception ""
    }
    def const_sql_8_68 = """select "9999-01-01 00:00:59.999", cast(cast("9999-01-01 00:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_68};"""
        exception ""
    }
    def const_sql_8_69 = """select "9999-01-01 00:01:00.000", cast(cast("9999-01-01 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_69};"""
        exception ""
    }
    def const_sql_8_70 = """select "9999-01-01 00:01:00.000", cast(cast("9999-01-01 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_70};"""
        exception ""
    }
    def const_sql_8_71 = """select "9999-01-01 00:01:00.999", cast(cast("9999-01-01 00:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_71};"""
        exception ""
    }
    def const_sql_8_72 = """select "9999-01-01 00:01:01.000", cast(cast("9999-01-01 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_72};"""
        exception ""
    }
    def const_sql_8_73 = """select "9999-01-01 00:01:01.000", cast(cast("9999-01-01 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_73};"""
        exception ""
    }
    def const_sql_8_74 = """select "9999-01-01 00:01:01.999", cast(cast("9999-01-01 00:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_74};"""
        exception ""
    }
    def const_sql_8_75 = """select "9999-01-01 00:01:59.000", cast(cast("9999-01-01 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_75};"""
        exception ""
    }
    def const_sql_8_76 = """select "9999-01-01 00:01:59.000", cast(cast("9999-01-01 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_76};"""
        exception ""
    }
    def const_sql_8_77 = """select "9999-01-01 00:01:59.999", cast(cast("9999-01-01 00:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_77};"""
        exception ""
    }
    def const_sql_8_78 = """select "9999-01-01 00:59:00.000", cast(cast("9999-01-01 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_78};"""
        exception ""
    }
    def const_sql_8_79 = """select "9999-01-01 00:59:00.000", cast(cast("9999-01-01 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_79};"""
        exception ""
    }
    def const_sql_8_80 = """select "9999-01-01 00:59:00.999", cast(cast("9999-01-01 00:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_80};"""
        exception ""
    }
    def const_sql_8_81 = """select "9999-01-01 00:59:01.000", cast(cast("9999-01-01 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_81};"""
        exception ""
    }
    def const_sql_8_82 = """select "9999-01-01 00:59:01.000", cast(cast("9999-01-01 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_82};"""
        exception ""
    }
    def const_sql_8_83 = """select "9999-01-01 00:59:01.999", cast(cast("9999-01-01 00:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_83};"""
        exception ""
    }
    def const_sql_8_84 = """select "9999-01-01 00:59:59.000", cast(cast("9999-01-01 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_84};"""
        exception ""
    }
    def const_sql_8_85 = """select "9999-01-01 00:59:59.000", cast(cast("9999-01-01 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_85};"""
        exception ""
    }
    def const_sql_8_86 = """select "9999-01-01 00:59:59.999", cast(cast("9999-01-01 00:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_86};"""
        exception ""
    }
    def const_sql_8_87 = """select "9999-01-01 01:00:00.000", cast(cast("9999-01-01 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_87};"""
        exception ""
    }
    def const_sql_8_88 = """select "9999-01-01 01:00:00.000", cast(cast("9999-01-01 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_88};"""
        exception ""
    }
    def const_sql_8_89 = """select "9999-01-01 01:00:00.999", cast(cast("9999-01-01 01:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_89};"""
        exception ""
    }
    def const_sql_8_90 = """select "9999-01-01 01:00:01.000", cast(cast("9999-01-01 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_90};"""
        exception ""
    }
    def const_sql_8_91 = """select "9999-01-01 01:00:01.000", cast(cast("9999-01-01 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_91};"""
        exception ""
    }
    def const_sql_8_92 = """select "9999-01-01 01:00:01.999", cast(cast("9999-01-01 01:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_92};"""
        exception ""
    }
    def const_sql_8_93 = """select "9999-01-01 01:00:59.000", cast(cast("9999-01-01 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_93};"""
        exception ""
    }
    def const_sql_8_94 = """select "9999-01-01 01:00:59.000", cast(cast("9999-01-01 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_94};"""
        exception ""
    }
    def const_sql_8_95 = """select "9999-01-01 01:00:59.999", cast(cast("9999-01-01 01:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_95};"""
        exception ""
    }
    def const_sql_8_96 = """select "9999-01-01 01:01:00.000", cast(cast("9999-01-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_96};"""
        exception ""
    }
    def const_sql_8_97 = """select "9999-01-01 01:01:00.000", cast(cast("9999-01-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_97};"""
        exception ""
    }
    def const_sql_8_98 = """select "9999-01-01 01:01:00.999", cast(cast("9999-01-01 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_98};"""
        exception ""
    }
    def const_sql_8_99 = """select "9999-01-01 01:01:01.000", cast(cast("9999-01-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_99};"""
        exception ""
    }
    def const_sql_8_100 = """select "9999-01-01 01:01:01.000", cast(cast("9999-01-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_100};"""
        exception ""
    }
    def const_sql_8_101 = """select "9999-01-01 01:01:01.999", cast(cast("9999-01-01 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_101};"""
        exception ""
    }
    def const_sql_8_102 = """select "9999-01-01 01:01:59.000", cast(cast("9999-01-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_102};"""
        exception ""
    }
    def const_sql_8_103 = """select "9999-01-01 01:01:59.000", cast(cast("9999-01-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_103};"""
        exception ""
    }
    def const_sql_8_104 = """select "9999-01-01 01:01:59.999", cast(cast("9999-01-01 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_104};"""
        exception ""
    }
    def const_sql_8_105 = """select "9999-01-01 01:59:00.000", cast(cast("9999-01-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_105};"""
        exception ""
    }
    def const_sql_8_106 = """select "9999-01-01 01:59:00.000", cast(cast("9999-01-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_106};"""
        exception ""
    }
    def const_sql_8_107 = """select "9999-01-01 01:59:00.999", cast(cast("9999-01-01 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_107};"""
        exception ""
    }
    def const_sql_8_108 = """select "9999-01-01 01:59:01.000", cast(cast("9999-01-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_108};"""
        exception ""
    }
    def const_sql_8_109 = """select "9999-01-01 01:59:01.000", cast(cast("9999-01-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_109};"""
        exception ""
    }
    def const_sql_8_110 = """select "9999-01-01 01:59:01.999", cast(cast("9999-01-01 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_110};"""
        exception ""
    }
    def const_sql_8_111 = """select "9999-01-01 01:59:59.000", cast(cast("9999-01-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_111};"""
        exception ""
    }
    def const_sql_8_112 = """select "9999-01-01 01:59:59.000", cast(cast("9999-01-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_112};"""
        exception ""
    }
    def const_sql_8_113 = """select "9999-01-01 01:59:59.999", cast(cast("9999-01-01 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_113};"""
        exception ""
    }
    def const_sql_8_114 = """select "9999-01-01 23:00:00.000", cast(cast("9999-01-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_114};"""
        exception ""
    }
    def const_sql_8_115 = """select "9999-01-01 23:00:00.000", cast(cast("9999-01-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_115};"""
        exception ""
    }
    def const_sql_8_116 = """select "9999-01-01 23:00:00.999", cast(cast("9999-01-01 23:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_116};"""
        exception ""
    }
    def const_sql_8_117 = """select "9999-01-01 23:00:01.000", cast(cast("9999-01-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_117};"""
        exception ""
    }
    def const_sql_8_118 = """select "9999-01-01 23:00:01.000", cast(cast("9999-01-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_118};"""
        exception ""
    }
    def const_sql_8_119 = """select "9999-01-01 23:00:01.999", cast(cast("9999-01-01 23:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_119};"""
        exception ""
    }
    def const_sql_8_120 = """select "9999-01-01 23:00:59.000", cast(cast("9999-01-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_120};"""
        exception ""
    }
    def const_sql_8_121 = """select "9999-01-01 23:00:59.000", cast(cast("9999-01-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_121};"""
        exception ""
    }
    def const_sql_8_122 = """select "9999-01-01 23:00:59.999", cast(cast("9999-01-01 23:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_122};"""
        exception ""
    }
    def const_sql_8_123 = """select "9999-01-01 23:01:00.000", cast(cast("9999-01-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_123};"""
        exception ""
    }
    def const_sql_8_124 = """select "9999-01-01 23:01:00.000", cast(cast("9999-01-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_124};"""
        exception ""
    }
    def const_sql_8_125 = """select "9999-01-01 23:01:00.999", cast(cast("9999-01-01 23:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_125};"""
        exception ""
    }
    def const_sql_8_126 = """select "9999-01-01 23:01:01.000", cast(cast("9999-01-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_126};"""
        exception ""
    }
    def const_sql_8_127 = """select "9999-01-01 23:01:01.000", cast(cast("9999-01-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_127};"""
        exception ""
    }
    def const_sql_8_128 = """select "9999-01-01 23:01:01.999", cast(cast("9999-01-01 23:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_128};"""
        exception ""
    }
    def const_sql_8_129 = """select "9999-01-01 23:01:59.000", cast(cast("9999-01-01 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_129};"""
        exception ""
    }
    def const_sql_8_130 = """select "9999-01-01 23:01:59.000", cast(cast("9999-01-01 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_130};"""
        exception ""
    }
    def const_sql_8_131 = """select "9999-01-01 23:01:59.999", cast(cast("9999-01-01 23:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_131};"""
        exception ""
    }
    def const_sql_8_132 = """select "9999-01-01 23:59:00.000", cast(cast("9999-01-01 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_132};"""
        exception ""
    }
    def const_sql_8_133 = """select "9999-01-01 23:59:00.000", cast(cast("9999-01-01 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_133};"""
        exception ""
    }
    def const_sql_8_134 = """select "9999-01-01 23:59:00.999", cast(cast("9999-01-01 23:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_134};"""
        exception ""
    }
    def const_sql_8_135 = """select "9999-01-01 23:59:01.000", cast(cast("9999-01-01 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_135};"""
        exception ""
    }
    def const_sql_8_136 = """select "9999-01-01 23:59:01.000", cast(cast("9999-01-01 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_136};"""
        exception ""
    }
    def const_sql_8_137 = """select "9999-01-01 23:59:01.999", cast(cast("9999-01-01 23:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_137};"""
        exception ""
    }
    def const_sql_8_138 = """select "9999-01-01 23:59:59.000", cast(cast("9999-01-01 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_138};"""
        exception ""
    }
    def const_sql_8_139 = """select "9999-01-01 23:59:59.000", cast(cast("9999-01-01 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_139};"""
        exception ""
    }
    def const_sql_8_140 = """select "9999-01-01 23:59:59.999", cast(cast("9999-01-01 23:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_140};"""
        exception ""
    }
    def const_sql_8_141 = """select "9999-01-28 00:00:00.000", cast(cast("9999-01-28 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_141};"""
        exception ""
    }
    def const_sql_8_142 = """select "9999-01-28 00:00:00.000", cast(cast("9999-01-28 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_142};"""
        exception ""
    }
    def const_sql_8_143 = """select "9999-01-28 00:00:00.999", cast(cast("9999-01-28 00:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_143};"""
        exception ""
    }
    def const_sql_8_144 = """select "9999-01-28 00:00:01.000", cast(cast("9999-01-28 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_144};"""
        exception ""
    }
    def const_sql_8_145 = """select "9999-01-28 00:00:01.000", cast(cast("9999-01-28 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_145};"""
        exception ""
    }
    def const_sql_8_146 = """select "9999-01-28 00:00:01.999", cast(cast("9999-01-28 00:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_146};"""
        exception ""
    }
    def const_sql_8_147 = """select "9999-01-28 00:00:59.000", cast(cast("9999-01-28 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_147};"""
        exception ""
    }
    def const_sql_8_148 = """select "9999-01-28 00:00:59.000", cast(cast("9999-01-28 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_148};"""
        exception ""
    }
    def const_sql_8_149 = """select "9999-01-28 00:00:59.999", cast(cast("9999-01-28 00:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_149};"""
        exception ""
    }
    def const_sql_8_150 = """select "9999-01-28 00:01:00.000", cast(cast("9999-01-28 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_150};"""
        exception ""
    }
    def const_sql_8_151 = """select "9999-01-28 00:01:00.000", cast(cast("9999-01-28 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_151};"""
        exception ""
    }
    def const_sql_8_152 = """select "9999-01-28 00:01:00.999", cast(cast("9999-01-28 00:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_152};"""
        exception ""
    }
    def const_sql_8_153 = """select "9999-01-28 00:01:01.000", cast(cast("9999-01-28 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_153};"""
        exception ""
    }
    def const_sql_8_154 = """select "9999-01-28 00:01:01.000", cast(cast("9999-01-28 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_154};"""
        exception ""
    }
    def const_sql_8_155 = """select "9999-01-28 00:01:01.999", cast(cast("9999-01-28 00:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_155};"""
        exception ""
    }
    def const_sql_8_156 = """select "9999-01-28 00:01:59.000", cast(cast("9999-01-28 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_156};"""
        exception ""
    }
    def const_sql_8_157 = """select "9999-01-28 00:01:59.000", cast(cast("9999-01-28 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_157};"""
        exception ""
    }
    def const_sql_8_158 = """select "9999-01-28 00:01:59.999", cast(cast("9999-01-28 00:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_158};"""
        exception ""
    }
    def const_sql_8_159 = """select "9999-01-28 00:59:00.000", cast(cast("9999-01-28 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_159};"""
        exception ""
    }
    def const_sql_8_160 = """select "9999-01-28 00:59:00.000", cast(cast("9999-01-28 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_160};"""
        exception ""
    }
    def const_sql_8_161 = """select "9999-01-28 00:59:00.999", cast(cast("9999-01-28 00:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_161};"""
        exception ""
    }
    def const_sql_8_162 = """select "9999-01-28 00:59:01.000", cast(cast("9999-01-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_162};"""
        exception ""
    }
    def const_sql_8_163 = """select "9999-01-28 00:59:01.000", cast(cast("9999-01-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_163};"""
        exception ""
    }
    def const_sql_8_164 = """select "9999-01-28 00:59:01.999", cast(cast("9999-01-28 00:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_164};"""
        exception ""
    }
    def const_sql_8_165 = """select "9999-01-28 00:59:59.000", cast(cast("9999-01-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_165};"""
        exception ""
    }
    def const_sql_8_166 = """select "9999-01-28 00:59:59.000", cast(cast("9999-01-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_166};"""
        exception ""
    }
    def const_sql_8_167 = """select "9999-01-28 00:59:59.999", cast(cast("9999-01-28 00:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_167};"""
        exception ""
    }
    def const_sql_8_168 = """select "9999-01-28 01:00:00.000", cast(cast("9999-01-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_168};"""
        exception ""
    }
    def const_sql_8_169 = """select "9999-01-28 01:00:00.000", cast(cast("9999-01-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_169};"""
        exception ""
    }
    def const_sql_8_170 = """select "9999-01-28 01:00:00.999", cast(cast("9999-01-28 01:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_170};"""
        exception ""
    }
    def const_sql_8_171 = """select "9999-01-28 01:00:01.000", cast(cast("9999-01-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_171};"""
        exception ""
    }
    def const_sql_8_172 = """select "9999-01-28 01:00:01.000", cast(cast("9999-01-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_172};"""
        exception ""
    }
    def const_sql_8_173 = """select "9999-01-28 01:00:01.999", cast(cast("9999-01-28 01:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_173};"""
        exception ""
    }
    def const_sql_8_174 = """select "9999-01-28 01:00:59.000", cast(cast("9999-01-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_174};"""
        exception ""
    }
    def const_sql_8_175 = """select "9999-01-28 01:00:59.000", cast(cast("9999-01-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_175};"""
        exception ""
    }
    def const_sql_8_176 = """select "9999-01-28 01:00:59.999", cast(cast("9999-01-28 01:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_176};"""
        exception ""
    }
    def const_sql_8_177 = """select "9999-01-28 01:01:00.000", cast(cast("9999-01-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_177};"""
        exception ""
    }
    def const_sql_8_178 = """select "9999-01-28 01:01:00.000", cast(cast("9999-01-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_178};"""
        exception ""
    }
    def const_sql_8_179 = """select "9999-01-28 01:01:00.999", cast(cast("9999-01-28 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_179};"""
        exception ""
    }
    def const_sql_8_180 = """select "9999-01-28 01:01:01.000", cast(cast("9999-01-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_180};"""
        exception ""
    }
    def const_sql_8_181 = """select "9999-01-28 01:01:01.000", cast(cast("9999-01-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_181};"""
        exception ""
    }
    def const_sql_8_182 = """select "9999-01-28 01:01:01.999", cast(cast("9999-01-28 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_182};"""
        exception ""
    }
    def const_sql_8_183 = """select "9999-01-28 01:01:59.000", cast(cast("9999-01-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_183};"""
        exception ""
    }
    def const_sql_8_184 = """select "9999-01-28 01:01:59.000", cast(cast("9999-01-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_184};"""
        exception ""
    }
    def const_sql_8_185 = """select "9999-01-28 01:01:59.999", cast(cast("9999-01-28 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_185};"""
        exception ""
    }
    def const_sql_8_186 = """select "9999-01-28 01:59:00.000", cast(cast("9999-01-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_186};"""
        exception ""
    }
    def const_sql_8_187 = """select "9999-01-28 01:59:00.000", cast(cast("9999-01-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_187};"""
        exception ""
    }
    def const_sql_8_188 = """select "9999-01-28 01:59:00.999", cast(cast("9999-01-28 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_188};"""
        exception ""
    }
    def const_sql_8_189 = """select "9999-01-28 01:59:01.000", cast(cast("9999-01-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_189};"""
        exception ""
    }
    def const_sql_8_190 = """select "9999-01-28 01:59:01.000", cast(cast("9999-01-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_190};"""
        exception ""
    }
    def const_sql_8_191 = """select "9999-01-28 01:59:01.999", cast(cast("9999-01-28 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_191};"""
        exception ""
    }
    def const_sql_8_192 = """select "9999-01-28 01:59:59.000", cast(cast("9999-01-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_192};"""
        exception ""
    }
    def const_sql_8_193 = """select "9999-01-28 01:59:59.000", cast(cast("9999-01-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_193};"""
        exception ""
    }
    def const_sql_8_194 = """select "9999-01-28 01:59:59.999", cast(cast("9999-01-28 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_8_194};"""
        exception ""
    }

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
    qt_sql_8_16_non_strict "${const_sql_8_16}"
    testFoldConst("${const_sql_8_16}")
    qt_sql_8_17_non_strict "${const_sql_8_17}"
    testFoldConst("${const_sql_8_17}")
    qt_sql_8_18_non_strict "${const_sql_8_18}"
    testFoldConst("${const_sql_8_18}")
    qt_sql_8_19_non_strict "${const_sql_8_19}"
    testFoldConst("${const_sql_8_19}")
    qt_sql_8_20_non_strict "${const_sql_8_20}"
    testFoldConst("${const_sql_8_20}")
    qt_sql_8_21_non_strict "${const_sql_8_21}"
    testFoldConst("${const_sql_8_21}")
    qt_sql_8_22_non_strict "${const_sql_8_22}"
    testFoldConst("${const_sql_8_22}")
    qt_sql_8_23_non_strict "${const_sql_8_23}"
    testFoldConst("${const_sql_8_23}")
    qt_sql_8_24_non_strict "${const_sql_8_24}"
    testFoldConst("${const_sql_8_24}")
    qt_sql_8_25_non_strict "${const_sql_8_25}"
    testFoldConst("${const_sql_8_25}")
    qt_sql_8_26_non_strict "${const_sql_8_26}"
    testFoldConst("${const_sql_8_26}")
    qt_sql_8_27_non_strict "${const_sql_8_27}"
    testFoldConst("${const_sql_8_27}")
    qt_sql_8_28_non_strict "${const_sql_8_28}"
    testFoldConst("${const_sql_8_28}")
    qt_sql_8_29_non_strict "${const_sql_8_29}"
    testFoldConst("${const_sql_8_29}")
    qt_sql_8_30_non_strict "${const_sql_8_30}"
    testFoldConst("${const_sql_8_30}")
    qt_sql_8_31_non_strict "${const_sql_8_31}"
    testFoldConst("${const_sql_8_31}")
    qt_sql_8_32_non_strict "${const_sql_8_32}"
    testFoldConst("${const_sql_8_32}")
    qt_sql_8_33_non_strict "${const_sql_8_33}"
    testFoldConst("${const_sql_8_33}")
    qt_sql_8_34_non_strict "${const_sql_8_34}"
    testFoldConst("${const_sql_8_34}")
    qt_sql_8_35_non_strict "${const_sql_8_35}"
    testFoldConst("${const_sql_8_35}")
    qt_sql_8_36_non_strict "${const_sql_8_36}"
    testFoldConst("${const_sql_8_36}")
    qt_sql_8_37_non_strict "${const_sql_8_37}"
    testFoldConst("${const_sql_8_37}")
    qt_sql_8_38_non_strict "${const_sql_8_38}"
    testFoldConst("${const_sql_8_38}")
    qt_sql_8_39_non_strict "${const_sql_8_39}"
    testFoldConst("${const_sql_8_39}")
    qt_sql_8_40_non_strict "${const_sql_8_40}"
    testFoldConst("${const_sql_8_40}")
    qt_sql_8_41_non_strict "${const_sql_8_41}"
    testFoldConst("${const_sql_8_41}")
    qt_sql_8_42_non_strict "${const_sql_8_42}"
    testFoldConst("${const_sql_8_42}")
    qt_sql_8_43_non_strict "${const_sql_8_43}"
    testFoldConst("${const_sql_8_43}")
    qt_sql_8_44_non_strict "${const_sql_8_44}"
    testFoldConst("${const_sql_8_44}")
    qt_sql_8_45_non_strict "${const_sql_8_45}"
    testFoldConst("${const_sql_8_45}")
    qt_sql_8_46_non_strict "${const_sql_8_46}"
    testFoldConst("${const_sql_8_46}")
    qt_sql_8_47_non_strict "${const_sql_8_47}"
    testFoldConst("${const_sql_8_47}")
    qt_sql_8_48_non_strict "${const_sql_8_48}"
    testFoldConst("${const_sql_8_48}")
    qt_sql_8_49_non_strict "${const_sql_8_49}"
    testFoldConst("${const_sql_8_49}")
    qt_sql_8_50_non_strict "${const_sql_8_50}"
    testFoldConst("${const_sql_8_50}")
    qt_sql_8_51_non_strict "${const_sql_8_51}"
    testFoldConst("${const_sql_8_51}")
    qt_sql_8_52_non_strict "${const_sql_8_52}"
    testFoldConst("${const_sql_8_52}")
    qt_sql_8_53_non_strict "${const_sql_8_53}"
    testFoldConst("${const_sql_8_53}")
    qt_sql_8_54_non_strict "${const_sql_8_54}"
    testFoldConst("${const_sql_8_54}")
    qt_sql_8_55_non_strict "${const_sql_8_55}"
    testFoldConst("${const_sql_8_55}")
    qt_sql_8_56_non_strict "${const_sql_8_56}"
    testFoldConst("${const_sql_8_56}")
    qt_sql_8_57_non_strict "${const_sql_8_57}"
    testFoldConst("${const_sql_8_57}")
    qt_sql_8_58_non_strict "${const_sql_8_58}"
    testFoldConst("${const_sql_8_58}")
    qt_sql_8_59_non_strict "${const_sql_8_59}"
    testFoldConst("${const_sql_8_59}")
    qt_sql_8_60_non_strict "${const_sql_8_60}"
    testFoldConst("${const_sql_8_60}")
    qt_sql_8_61_non_strict "${const_sql_8_61}"
    testFoldConst("${const_sql_8_61}")
    qt_sql_8_62_non_strict "${const_sql_8_62}"
    testFoldConst("${const_sql_8_62}")
    qt_sql_8_63_non_strict "${const_sql_8_63}"
    testFoldConst("${const_sql_8_63}")
    qt_sql_8_64_non_strict "${const_sql_8_64}"
    testFoldConst("${const_sql_8_64}")
    qt_sql_8_65_non_strict "${const_sql_8_65}"
    testFoldConst("${const_sql_8_65}")
    qt_sql_8_66_non_strict "${const_sql_8_66}"
    testFoldConst("${const_sql_8_66}")
    qt_sql_8_67_non_strict "${const_sql_8_67}"
    testFoldConst("${const_sql_8_67}")
    qt_sql_8_68_non_strict "${const_sql_8_68}"
    testFoldConst("${const_sql_8_68}")
    qt_sql_8_69_non_strict "${const_sql_8_69}"
    testFoldConst("${const_sql_8_69}")
    qt_sql_8_70_non_strict "${const_sql_8_70}"
    testFoldConst("${const_sql_8_70}")
    qt_sql_8_71_non_strict "${const_sql_8_71}"
    testFoldConst("${const_sql_8_71}")
    qt_sql_8_72_non_strict "${const_sql_8_72}"
    testFoldConst("${const_sql_8_72}")
    qt_sql_8_73_non_strict "${const_sql_8_73}"
    testFoldConst("${const_sql_8_73}")
    qt_sql_8_74_non_strict "${const_sql_8_74}"
    testFoldConst("${const_sql_8_74}")
    qt_sql_8_75_non_strict "${const_sql_8_75}"
    testFoldConst("${const_sql_8_75}")
    qt_sql_8_76_non_strict "${const_sql_8_76}"
    testFoldConst("${const_sql_8_76}")
    qt_sql_8_77_non_strict "${const_sql_8_77}"
    testFoldConst("${const_sql_8_77}")
    qt_sql_8_78_non_strict "${const_sql_8_78}"
    testFoldConst("${const_sql_8_78}")
    qt_sql_8_79_non_strict "${const_sql_8_79}"
    testFoldConst("${const_sql_8_79}")
    qt_sql_8_80_non_strict "${const_sql_8_80}"
    testFoldConst("${const_sql_8_80}")
    qt_sql_8_81_non_strict "${const_sql_8_81}"
    testFoldConst("${const_sql_8_81}")
    qt_sql_8_82_non_strict "${const_sql_8_82}"
    testFoldConst("${const_sql_8_82}")
    qt_sql_8_83_non_strict "${const_sql_8_83}"
    testFoldConst("${const_sql_8_83}")
    qt_sql_8_84_non_strict "${const_sql_8_84}"
    testFoldConst("${const_sql_8_84}")
    qt_sql_8_85_non_strict "${const_sql_8_85}"
    testFoldConst("${const_sql_8_85}")
    qt_sql_8_86_non_strict "${const_sql_8_86}"
    testFoldConst("${const_sql_8_86}")
    qt_sql_8_87_non_strict "${const_sql_8_87}"
    testFoldConst("${const_sql_8_87}")
    qt_sql_8_88_non_strict "${const_sql_8_88}"
    testFoldConst("${const_sql_8_88}")
    qt_sql_8_89_non_strict "${const_sql_8_89}"
    testFoldConst("${const_sql_8_89}")
    qt_sql_8_90_non_strict "${const_sql_8_90}"
    testFoldConst("${const_sql_8_90}")
    qt_sql_8_91_non_strict "${const_sql_8_91}"
    testFoldConst("${const_sql_8_91}")
    qt_sql_8_92_non_strict "${const_sql_8_92}"
    testFoldConst("${const_sql_8_92}")
    qt_sql_8_93_non_strict "${const_sql_8_93}"
    testFoldConst("${const_sql_8_93}")
    qt_sql_8_94_non_strict "${const_sql_8_94}"
    testFoldConst("${const_sql_8_94}")
    qt_sql_8_95_non_strict "${const_sql_8_95}"
    testFoldConst("${const_sql_8_95}")
    qt_sql_8_96_non_strict "${const_sql_8_96}"
    testFoldConst("${const_sql_8_96}")
    qt_sql_8_97_non_strict "${const_sql_8_97}"
    testFoldConst("${const_sql_8_97}")
    qt_sql_8_98_non_strict "${const_sql_8_98}"
    testFoldConst("${const_sql_8_98}")
    qt_sql_8_99_non_strict "${const_sql_8_99}"
    testFoldConst("${const_sql_8_99}")
    qt_sql_8_100_non_strict "${const_sql_8_100}"
    testFoldConst("${const_sql_8_100}")
    qt_sql_8_101_non_strict "${const_sql_8_101}"
    testFoldConst("${const_sql_8_101}")
    qt_sql_8_102_non_strict "${const_sql_8_102}"
    testFoldConst("${const_sql_8_102}")
    qt_sql_8_103_non_strict "${const_sql_8_103}"
    testFoldConst("${const_sql_8_103}")
    qt_sql_8_104_non_strict "${const_sql_8_104}"
    testFoldConst("${const_sql_8_104}")
    qt_sql_8_105_non_strict "${const_sql_8_105}"
    testFoldConst("${const_sql_8_105}")
    qt_sql_8_106_non_strict "${const_sql_8_106}"
    testFoldConst("${const_sql_8_106}")
    qt_sql_8_107_non_strict "${const_sql_8_107}"
    testFoldConst("${const_sql_8_107}")
    qt_sql_8_108_non_strict "${const_sql_8_108}"
    testFoldConst("${const_sql_8_108}")
    qt_sql_8_109_non_strict "${const_sql_8_109}"
    testFoldConst("${const_sql_8_109}")
    qt_sql_8_110_non_strict "${const_sql_8_110}"
    testFoldConst("${const_sql_8_110}")
    qt_sql_8_111_non_strict "${const_sql_8_111}"
    testFoldConst("${const_sql_8_111}")
    qt_sql_8_112_non_strict "${const_sql_8_112}"
    testFoldConst("${const_sql_8_112}")
    qt_sql_8_113_non_strict "${const_sql_8_113}"
    testFoldConst("${const_sql_8_113}")
    qt_sql_8_114_non_strict "${const_sql_8_114}"
    testFoldConst("${const_sql_8_114}")
    qt_sql_8_115_non_strict "${const_sql_8_115}"
    testFoldConst("${const_sql_8_115}")
    qt_sql_8_116_non_strict "${const_sql_8_116}"
    testFoldConst("${const_sql_8_116}")
    qt_sql_8_117_non_strict "${const_sql_8_117}"
    testFoldConst("${const_sql_8_117}")
    qt_sql_8_118_non_strict "${const_sql_8_118}"
    testFoldConst("${const_sql_8_118}")
    qt_sql_8_119_non_strict "${const_sql_8_119}"
    testFoldConst("${const_sql_8_119}")
    qt_sql_8_120_non_strict "${const_sql_8_120}"
    testFoldConst("${const_sql_8_120}")
    qt_sql_8_121_non_strict "${const_sql_8_121}"
    testFoldConst("${const_sql_8_121}")
    qt_sql_8_122_non_strict "${const_sql_8_122}"
    testFoldConst("${const_sql_8_122}")
    qt_sql_8_123_non_strict "${const_sql_8_123}"
    testFoldConst("${const_sql_8_123}")
    qt_sql_8_124_non_strict "${const_sql_8_124}"
    testFoldConst("${const_sql_8_124}")
    qt_sql_8_125_non_strict "${const_sql_8_125}"
    testFoldConst("${const_sql_8_125}")
    qt_sql_8_126_non_strict "${const_sql_8_126}"
    testFoldConst("${const_sql_8_126}")
    qt_sql_8_127_non_strict "${const_sql_8_127}"
    testFoldConst("${const_sql_8_127}")
    qt_sql_8_128_non_strict "${const_sql_8_128}"
    testFoldConst("${const_sql_8_128}")
    qt_sql_8_129_non_strict "${const_sql_8_129}"
    testFoldConst("${const_sql_8_129}")
    qt_sql_8_130_non_strict "${const_sql_8_130}"
    testFoldConst("${const_sql_8_130}")
    qt_sql_8_131_non_strict "${const_sql_8_131}"
    testFoldConst("${const_sql_8_131}")
    qt_sql_8_132_non_strict "${const_sql_8_132}"
    testFoldConst("${const_sql_8_132}")
    qt_sql_8_133_non_strict "${const_sql_8_133}"
    testFoldConst("${const_sql_8_133}")
    qt_sql_8_134_non_strict "${const_sql_8_134}"
    testFoldConst("${const_sql_8_134}")
    qt_sql_8_135_non_strict "${const_sql_8_135}"
    testFoldConst("${const_sql_8_135}")
    qt_sql_8_136_non_strict "${const_sql_8_136}"
    testFoldConst("${const_sql_8_136}")
    qt_sql_8_137_non_strict "${const_sql_8_137}"
    testFoldConst("${const_sql_8_137}")
    qt_sql_8_138_non_strict "${const_sql_8_138}"
    testFoldConst("${const_sql_8_138}")
    qt_sql_8_139_non_strict "${const_sql_8_139}"
    testFoldConst("${const_sql_8_139}")
    qt_sql_8_140_non_strict "${const_sql_8_140}"
    testFoldConst("${const_sql_8_140}")
    qt_sql_8_141_non_strict "${const_sql_8_141}"
    testFoldConst("${const_sql_8_141}")
    qt_sql_8_142_non_strict "${const_sql_8_142}"
    testFoldConst("${const_sql_8_142}")
    qt_sql_8_143_non_strict "${const_sql_8_143}"
    testFoldConst("${const_sql_8_143}")
    qt_sql_8_144_non_strict "${const_sql_8_144}"
    testFoldConst("${const_sql_8_144}")
    qt_sql_8_145_non_strict "${const_sql_8_145}"
    testFoldConst("${const_sql_8_145}")
    qt_sql_8_146_non_strict "${const_sql_8_146}"
    testFoldConst("${const_sql_8_146}")
    qt_sql_8_147_non_strict "${const_sql_8_147}"
    testFoldConst("${const_sql_8_147}")
    qt_sql_8_148_non_strict "${const_sql_8_148}"
    testFoldConst("${const_sql_8_148}")
    qt_sql_8_149_non_strict "${const_sql_8_149}"
    testFoldConst("${const_sql_8_149}")
    qt_sql_8_150_non_strict "${const_sql_8_150}"
    testFoldConst("${const_sql_8_150}")
    qt_sql_8_151_non_strict "${const_sql_8_151}"
    testFoldConst("${const_sql_8_151}")
    qt_sql_8_152_non_strict "${const_sql_8_152}"
    testFoldConst("${const_sql_8_152}")
    qt_sql_8_153_non_strict "${const_sql_8_153}"
    testFoldConst("${const_sql_8_153}")
    qt_sql_8_154_non_strict "${const_sql_8_154}"
    testFoldConst("${const_sql_8_154}")
    qt_sql_8_155_non_strict "${const_sql_8_155}"
    testFoldConst("${const_sql_8_155}")
    qt_sql_8_156_non_strict "${const_sql_8_156}"
    testFoldConst("${const_sql_8_156}")
    qt_sql_8_157_non_strict "${const_sql_8_157}"
    testFoldConst("${const_sql_8_157}")
    qt_sql_8_158_non_strict "${const_sql_8_158}"
    testFoldConst("${const_sql_8_158}")
    qt_sql_8_159_non_strict "${const_sql_8_159}"
    testFoldConst("${const_sql_8_159}")
    qt_sql_8_160_non_strict "${const_sql_8_160}"
    testFoldConst("${const_sql_8_160}")
    qt_sql_8_161_non_strict "${const_sql_8_161}"
    testFoldConst("${const_sql_8_161}")
    qt_sql_8_162_non_strict "${const_sql_8_162}"
    testFoldConst("${const_sql_8_162}")
    qt_sql_8_163_non_strict "${const_sql_8_163}"
    testFoldConst("${const_sql_8_163}")
    qt_sql_8_164_non_strict "${const_sql_8_164}"
    testFoldConst("${const_sql_8_164}")
    qt_sql_8_165_non_strict "${const_sql_8_165}"
    testFoldConst("${const_sql_8_165}")
    qt_sql_8_166_non_strict "${const_sql_8_166}"
    testFoldConst("${const_sql_8_166}")
    qt_sql_8_167_non_strict "${const_sql_8_167}"
    testFoldConst("${const_sql_8_167}")
    qt_sql_8_168_non_strict "${const_sql_8_168}"
    testFoldConst("${const_sql_8_168}")
    qt_sql_8_169_non_strict "${const_sql_8_169}"
    testFoldConst("${const_sql_8_169}")
    qt_sql_8_170_non_strict "${const_sql_8_170}"
    testFoldConst("${const_sql_8_170}")
    qt_sql_8_171_non_strict "${const_sql_8_171}"
    testFoldConst("${const_sql_8_171}")
    qt_sql_8_172_non_strict "${const_sql_8_172}"
    testFoldConst("${const_sql_8_172}")
    qt_sql_8_173_non_strict "${const_sql_8_173}"
    testFoldConst("${const_sql_8_173}")
    qt_sql_8_174_non_strict "${const_sql_8_174}"
    testFoldConst("${const_sql_8_174}")
    qt_sql_8_175_non_strict "${const_sql_8_175}"
    testFoldConst("${const_sql_8_175}")
    qt_sql_8_176_non_strict "${const_sql_8_176}"
    testFoldConst("${const_sql_8_176}")
    qt_sql_8_177_non_strict "${const_sql_8_177}"
    testFoldConst("${const_sql_8_177}")
    qt_sql_8_178_non_strict "${const_sql_8_178}"
    testFoldConst("${const_sql_8_178}")
    qt_sql_8_179_non_strict "${const_sql_8_179}"
    testFoldConst("${const_sql_8_179}")
    qt_sql_8_180_non_strict "${const_sql_8_180}"
    testFoldConst("${const_sql_8_180}")
    qt_sql_8_181_non_strict "${const_sql_8_181}"
    testFoldConst("${const_sql_8_181}")
    qt_sql_8_182_non_strict "${const_sql_8_182}"
    testFoldConst("${const_sql_8_182}")
    qt_sql_8_183_non_strict "${const_sql_8_183}"
    testFoldConst("${const_sql_8_183}")
    qt_sql_8_184_non_strict "${const_sql_8_184}"
    testFoldConst("${const_sql_8_184}")
    qt_sql_8_185_non_strict "${const_sql_8_185}"
    testFoldConst("${const_sql_8_185}")
    qt_sql_8_186_non_strict "${const_sql_8_186}"
    testFoldConst("${const_sql_8_186}")
    qt_sql_8_187_non_strict "${const_sql_8_187}"
    testFoldConst("${const_sql_8_187}")
    qt_sql_8_188_non_strict "${const_sql_8_188}"
    testFoldConst("${const_sql_8_188}")
    qt_sql_8_189_non_strict "${const_sql_8_189}"
    testFoldConst("${const_sql_8_189}")
    qt_sql_8_190_non_strict "${const_sql_8_190}"
    testFoldConst("${const_sql_8_190}")
    qt_sql_8_191_non_strict "${const_sql_8_191}"
    testFoldConst("${const_sql_8_191}")
    qt_sql_8_192_non_strict "${const_sql_8_192}"
    testFoldConst("${const_sql_8_192}")
    qt_sql_8_193_non_strict "${const_sql_8_193}"
    testFoldConst("${const_sql_8_193}")
    qt_sql_8_194_non_strict "${const_sql_8_194}"
    testFoldConst("${const_sql_8_194}")
}