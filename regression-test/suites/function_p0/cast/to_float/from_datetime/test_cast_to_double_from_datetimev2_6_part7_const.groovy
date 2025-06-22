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


suite("test_cast_to_double_from_datetimev2_6_part7_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_7_0 = """select "2025-01-01 23:01:59.000000", cast(cast("2025-01-01 23:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_0};"""
        exception ""
    }
    def const_sql_7_1 = """select "2025-01-01 23:01:59.000001", cast(cast("2025-01-01 23:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_1};"""
        exception ""
    }
    def const_sql_7_2 = """select "2025-01-01 23:01:59.999999", cast(cast("2025-01-01 23:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_2};"""
        exception ""
    }
    def const_sql_7_3 = """select "2025-01-01 23:59:00.000000", cast(cast("2025-01-01 23:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_3};"""
        exception ""
    }
    def const_sql_7_4 = """select "2025-01-01 23:59:00.000001", cast(cast("2025-01-01 23:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_4};"""
        exception ""
    }
    def const_sql_7_5 = """select "2025-01-01 23:59:00.999999", cast(cast("2025-01-01 23:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_5};"""
        exception ""
    }
    def const_sql_7_6 = """select "2025-01-01 23:59:01.000000", cast(cast("2025-01-01 23:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_6};"""
        exception ""
    }
    def const_sql_7_7 = """select "2025-01-01 23:59:01.000001", cast(cast("2025-01-01 23:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_7};"""
        exception ""
    }
    def const_sql_7_8 = """select "2025-01-01 23:59:01.999999", cast(cast("2025-01-01 23:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_8};"""
        exception ""
    }
    def const_sql_7_9 = """select "2025-01-01 23:59:59.000000", cast(cast("2025-01-01 23:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_9};"""
        exception ""
    }
    def const_sql_7_10 = """select "2025-01-01 23:59:59.000001", cast(cast("2025-01-01 23:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_10};"""
        exception ""
    }
    def const_sql_7_11 = """select "2025-01-01 23:59:59.999999", cast(cast("2025-01-01 23:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_11};"""
        exception ""
    }
    def const_sql_7_12 = """select "2025-01-28 00:00:00.000000", cast(cast("2025-01-28 00:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_12};"""
        exception ""
    }
    def const_sql_7_13 = """select "2025-01-28 00:00:00.000001", cast(cast("2025-01-28 00:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_13};"""
        exception ""
    }
    def const_sql_7_14 = """select "2025-01-28 00:00:00.999999", cast(cast("2025-01-28 00:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_14};"""
        exception ""
    }
    def const_sql_7_15 = """select "2025-01-28 00:00:01.000000", cast(cast("2025-01-28 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_15};"""
        exception ""
    }
    def const_sql_7_16 = """select "2025-01-28 00:00:01.000001", cast(cast("2025-01-28 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_16};"""
        exception ""
    }
    def const_sql_7_17 = """select "2025-01-28 00:00:01.999999", cast(cast("2025-01-28 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_17};"""
        exception ""
    }
    def const_sql_7_18 = """select "2025-01-28 00:00:59.000000", cast(cast("2025-01-28 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_18};"""
        exception ""
    }
    def const_sql_7_19 = """select "2025-01-28 00:00:59.000001", cast(cast("2025-01-28 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_19};"""
        exception ""
    }
    def const_sql_7_20 = """select "2025-01-28 00:00:59.999999", cast(cast("2025-01-28 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_20};"""
        exception ""
    }
    def const_sql_7_21 = """select "2025-01-28 00:01:00.000000", cast(cast("2025-01-28 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_21};"""
        exception ""
    }
    def const_sql_7_22 = """select "2025-01-28 00:01:00.000001", cast(cast("2025-01-28 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_22};"""
        exception ""
    }
    def const_sql_7_23 = """select "2025-01-28 00:01:00.999999", cast(cast("2025-01-28 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_23};"""
        exception ""
    }
    def const_sql_7_24 = """select "2025-01-28 00:01:01.000000", cast(cast("2025-01-28 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_24};"""
        exception ""
    }
    def const_sql_7_25 = """select "2025-01-28 00:01:01.000001", cast(cast("2025-01-28 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_25};"""
        exception ""
    }
    def const_sql_7_26 = """select "2025-01-28 00:01:01.999999", cast(cast("2025-01-28 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_26};"""
        exception ""
    }
    def const_sql_7_27 = """select "2025-01-28 00:01:59.000000", cast(cast("2025-01-28 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_27};"""
        exception ""
    }
    def const_sql_7_28 = """select "2025-01-28 00:01:59.000001", cast(cast("2025-01-28 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_28};"""
        exception ""
    }
    def const_sql_7_29 = """select "2025-01-28 00:01:59.999999", cast(cast("2025-01-28 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_29};"""
        exception ""
    }
    def const_sql_7_30 = """select "2025-01-28 00:59:00.000000", cast(cast("2025-01-28 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_30};"""
        exception ""
    }
    def const_sql_7_31 = """select "2025-01-28 00:59:00.000001", cast(cast("2025-01-28 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_31};"""
        exception ""
    }
    def const_sql_7_32 = """select "2025-01-28 00:59:00.999999", cast(cast("2025-01-28 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_32};"""
        exception ""
    }
    def const_sql_7_33 = """select "2025-01-28 00:59:01.000000", cast(cast("2025-01-28 00:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_33};"""
        exception ""
    }
    def const_sql_7_34 = """select "2025-01-28 00:59:01.000001", cast(cast("2025-01-28 00:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_34};"""
        exception ""
    }
    def const_sql_7_35 = """select "2025-01-28 00:59:01.999999", cast(cast("2025-01-28 00:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_35};"""
        exception ""
    }
    def const_sql_7_36 = """select "2025-01-28 00:59:59.000000", cast(cast("2025-01-28 00:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_36};"""
        exception ""
    }
    def const_sql_7_37 = """select "2025-01-28 00:59:59.000001", cast(cast("2025-01-28 00:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_37};"""
        exception ""
    }
    def const_sql_7_38 = """select "2025-01-28 00:59:59.999999", cast(cast("2025-01-28 00:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_38};"""
        exception ""
    }
    def const_sql_7_39 = """select "2025-01-28 01:00:00.000000", cast(cast("2025-01-28 01:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_39};"""
        exception ""
    }
    def const_sql_7_40 = """select "2025-01-28 01:00:00.000001", cast(cast("2025-01-28 01:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_40};"""
        exception ""
    }
    def const_sql_7_41 = """select "2025-01-28 01:00:00.999999", cast(cast("2025-01-28 01:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_41};"""
        exception ""
    }
    def const_sql_7_42 = """select "2025-01-28 01:00:01.000000", cast(cast("2025-01-28 01:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_42};"""
        exception ""
    }
    def const_sql_7_43 = """select "2025-01-28 01:00:01.000001", cast(cast("2025-01-28 01:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_43};"""
        exception ""
    }
    def const_sql_7_44 = """select "2025-01-28 01:00:01.999999", cast(cast("2025-01-28 01:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_44};"""
        exception ""
    }
    def const_sql_7_45 = """select "2025-01-28 01:00:59.000000", cast(cast("2025-01-28 01:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_45};"""
        exception ""
    }
    def const_sql_7_46 = """select "2025-01-28 01:00:59.000001", cast(cast("2025-01-28 01:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_46};"""
        exception ""
    }
    def const_sql_7_47 = """select "2025-01-28 01:00:59.999999", cast(cast("2025-01-28 01:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_47};"""
        exception ""
    }
    def const_sql_7_48 = """select "2025-01-28 01:01:00.000000", cast(cast("2025-01-28 01:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_48};"""
        exception ""
    }
    def const_sql_7_49 = """select "2025-01-28 01:01:00.000001", cast(cast("2025-01-28 01:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_49};"""
        exception ""
    }
    def const_sql_7_50 = """select "2025-01-28 01:01:00.999999", cast(cast("2025-01-28 01:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_50};"""
        exception ""
    }
    def const_sql_7_51 = """select "2025-01-28 01:01:01.000000", cast(cast("2025-01-28 01:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_51};"""
        exception ""
    }
    def const_sql_7_52 = """select "2025-01-28 01:01:01.000001", cast(cast("2025-01-28 01:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_52};"""
        exception ""
    }
    def const_sql_7_53 = """select "2025-01-28 01:01:01.999999", cast(cast("2025-01-28 01:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_53};"""
        exception ""
    }
    def const_sql_7_54 = """select "2025-01-28 01:01:59.000000", cast(cast("2025-01-28 01:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_54};"""
        exception ""
    }
    def const_sql_7_55 = """select "2025-01-28 01:01:59.000001", cast(cast("2025-01-28 01:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_55};"""
        exception ""
    }
    def const_sql_7_56 = """select "2025-01-28 01:01:59.999999", cast(cast("2025-01-28 01:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_56};"""
        exception ""
    }
    def const_sql_7_57 = """select "2025-01-28 01:59:00.000000", cast(cast("2025-01-28 01:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_57};"""
        exception ""
    }
    def const_sql_7_58 = """select "2025-01-28 01:59:00.000001", cast(cast("2025-01-28 01:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_58};"""
        exception ""
    }
    def const_sql_7_59 = """select "2025-01-28 01:59:00.999999", cast(cast("2025-01-28 01:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_59};"""
        exception ""
    }
    def const_sql_7_60 = """select "2025-01-28 01:59:01.000000", cast(cast("2025-01-28 01:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_60};"""
        exception ""
    }
    def const_sql_7_61 = """select "2025-01-28 01:59:01.000001", cast(cast("2025-01-28 01:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_61};"""
        exception ""
    }
    def const_sql_7_62 = """select "2025-01-28 01:59:01.999999", cast(cast("2025-01-28 01:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_62};"""
        exception ""
    }
    def const_sql_7_63 = """select "2025-01-28 01:59:59.000000", cast(cast("2025-01-28 01:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_63};"""
        exception ""
    }
    def const_sql_7_64 = """select "2025-01-28 01:59:59.000001", cast(cast("2025-01-28 01:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_64};"""
        exception ""
    }
    def const_sql_7_65 = """select "2025-01-28 01:59:59.999999", cast(cast("2025-01-28 01:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_65};"""
        exception ""
    }
    def const_sql_7_66 = """select "2025-01-28 23:00:00.000000", cast(cast("2025-01-28 23:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_66};"""
        exception ""
    }
    def const_sql_7_67 = """select "2025-01-28 23:00:00.000001", cast(cast("2025-01-28 23:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_67};"""
        exception ""
    }
    def const_sql_7_68 = """select "2025-01-28 23:00:00.999999", cast(cast("2025-01-28 23:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_68};"""
        exception ""
    }
    def const_sql_7_69 = """select "2025-01-28 23:00:01.000000", cast(cast("2025-01-28 23:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_69};"""
        exception ""
    }
    def const_sql_7_70 = """select "2025-01-28 23:00:01.000001", cast(cast("2025-01-28 23:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_70};"""
        exception ""
    }
    def const_sql_7_71 = """select "2025-01-28 23:00:01.999999", cast(cast("2025-01-28 23:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_71};"""
        exception ""
    }
    def const_sql_7_72 = """select "2025-01-28 23:00:59.000000", cast(cast("2025-01-28 23:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_72};"""
        exception ""
    }
    def const_sql_7_73 = """select "2025-01-28 23:00:59.000001", cast(cast("2025-01-28 23:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_73};"""
        exception ""
    }
    def const_sql_7_74 = """select "2025-01-28 23:00:59.999999", cast(cast("2025-01-28 23:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_74};"""
        exception ""
    }
    def const_sql_7_75 = """select "2025-01-28 23:01:00.000000", cast(cast("2025-01-28 23:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_75};"""
        exception ""
    }
    def const_sql_7_76 = """select "2025-01-28 23:01:00.000001", cast(cast("2025-01-28 23:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_76};"""
        exception ""
    }
    def const_sql_7_77 = """select "2025-01-28 23:01:00.999999", cast(cast("2025-01-28 23:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_77};"""
        exception ""
    }
    def const_sql_7_78 = """select "2025-01-28 23:01:01.000000", cast(cast("2025-01-28 23:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_78};"""
        exception ""
    }
    def const_sql_7_79 = """select "2025-01-28 23:01:01.000001", cast(cast("2025-01-28 23:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_79};"""
        exception ""
    }
    def const_sql_7_80 = """select "2025-01-28 23:01:01.999999", cast(cast("2025-01-28 23:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_80};"""
        exception ""
    }
    def const_sql_7_81 = """select "2025-01-28 23:01:59.000000", cast(cast("2025-01-28 23:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_81};"""
        exception ""
    }
    def const_sql_7_82 = """select "2025-01-28 23:01:59.000001", cast(cast("2025-01-28 23:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_82};"""
        exception ""
    }
    def const_sql_7_83 = """select "2025-01-28 23:01:59.999999", cast(cast("2025-01-28 23:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_83};"""
        exception ""
    }
    def const_sql_7_84 = """select "2025-01-28 23:59:00.000000", cast(cast("2025-01-28 23:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_84};"""
        exception ""
    }
    def const_sql_7_85 = """select "2025-01-28 23:59:00.000001", cast(cast("2025-01-28 23:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_85};"""
        exception ""
    }
    def const_sql_7_86 = """select "2025-01-28 23:59:00.999999", cast(cast("2025-01-28 23:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_86};"""
        exception ""
    }
    def const_sql_7_87 = """select "2025-01-28 23:59:01.000000", cast(cast("2025-01-28 23:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_87};"""
        exception ""
    }
    def const_sql_7_88 = """select "2025-01-28 23:59:01.000001", cast(cast("2025-01-28 23:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_88};"""
        exception ""
    }
    def const_sql_7_89 = """select "2025-01-28 23:59:01.999999", cast(cast("2025-01-28 23:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_89};"""
        exception ""
    }
    def const_sql_7_90 = """select "2025-01-28 23:59:59.000000", cast(cast("2025-01-28 23:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_90};"""
        exception ""
    }
    def const_sql_7_91 = """select "2025-01-28 23:59:59.000001", cast(cast("2025-01-28 23:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_91};"""
        exception ""
    }
    def const_sql_7_92 = """select "2025-01-28 23:59:59.999999", cast(cast("2025-01-28 23:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_92};"""
        exception ""
    }
    def const_sql_7_93 = """select "2025-12-01 00:00:00.000000", cast(cast("2025-12-01 00:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_93};"""
        exception ""
    }
    def const_sql_7_94 = """select "2025-12-01 00:00:00.000001", cast(cast("2025-12-01 00:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_94};"""
        exception ""
    }
    def const_sql_7_95 = """select "2025-12-01 00:00:00.999999", cast(cast("2025-12-01 00:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_95};"""
        exception ""
    }
    def const_sql_7_96 = """select "2025-12-01 00:00:01.000000", cast(cast("2025-12-01 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_96};"""
        exception ""
    }
    def const_sql_7_97 = """select "2025-12-01 00:00:01.000001", cast(cast("2025-12-01 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_97};"""
        exception ""
    }
    def const_sql_7_98 = """select "2025-12-01 00:00:01.999999", cast(cast("2025-12-01 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_98};"""
        exception ""
    }
    def const_sql_7_99 = """select "2025-12-01 00:00:59.000000", cast(cast("2025-12-01 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_99};"""
        exception ""
    }
    def const_sql_7_100 = """select "2025-12-01 00:00:59.000001", cast(cast("2025-12-01 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_100};"""
        exception ""
    }
    def const_sql_7_101 = """select "2025-12-01 00:00:59.999999", cast(cast("2025-12-01 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_101};"""
        exception ""
    }
    def const_sql_7_102 = """select "2025-12-01 00:01:00.000000", cast(cast("2025-12-01 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_102};"""
        exception ""
    }
    def const_sql_7_103 = """select "2025-12-01 00:01:00.000001", cast(cast("2025-12-01 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_103};"""
        exception ""
    }
    def const_sql_7_104 = """select "2025-12-01 00:01:00.999999", cast(cast("2025-12-01 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_104};"""
        exception ""
    }
    def const_sql_7_105 = """select "2025-12-01 00:01:01.000000", cast(cast("2025-12-01 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_105};"""
        exception ""
    }
    def const_sql_7_106 = """select "2025-12-01 00:01:01.000001", cast(cast("2025-12-01 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_106};"""
        exception ""
    }
    def const_sql_7_107 = """select "2025-12-01 00:01:01.999999", cast(cast("2025-12-01 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_107};"""
        exception ""
    }
    def const_sql_7_108 = """select "2025-12-01 00:01:59.000000", cast(cast("2025-12-01 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_108};"""
        exception ""
    }
    def const_sql_7_109 = """select "2025-12-01 00:01:59.000001", cast(cast("2025-12-01 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_109};"""
        exception ""
    }
    def const_sql_7_110 = """select "2025-12-01 00:01:59.999999", cast(cast("2025-12-01 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_110};"""
        exception ""
    }
    def const_sql_7_111 = """select "2025-12-01 00:59:00.000000", cast(cast("2025-12-01 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_111};"""
        exception ""
    }
    def const_sql_7_112 = """select "2025-12-01 00:59:00.000001", cast(cast("2025-12-01 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_112};"""
        exception ""
    }
    def const_sql_7_113 = """select "2025-12-01 00:59:00.999999", cast(cast("2025-12-01 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_113};"""
        exception ""
    }
    def const_sql_7_114 = """select "2025-12-01 00:59:01.000000", cast(cast("2025-12-01 00:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_114};"""
        exception ""
    }
    def const_sql_7_115 = """select "2025-12-01 00:59:01.000001", cast(cast("2025-12-01 00:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_115};"""
        exception ""
    }
    def const_sql_7_116 = """select "2025-12-01 00:59:01.999999", cast(cast("2025-12-01 00:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_116};"""
        exception ""
    }
    def const_sql_7_117 = """select "2025-12-01 00:59:59.000000", cast(cast("2025-12-01 00:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_117};"""
        exception ""
    }
    def const_sql_7_118 = """select "2025-12-01 00:59:59.000001", cast(cast("2025-12-01 00:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_118};"""
        exception ""
    }
    def const_sql_7_119 = """select "2025-12-01 00:59:59.999999", cast(cast("2025-12-01 00:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_119};"""
        exception ""
    }
    def const_sql_7_120 = """select "2025-12-01 01:00:00.000000", cast(cast("2025-12-01 01:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_120};"""
        exception ""
    }
    def const_sql_7_121 = """select "2025-12-01 01:00:00.000001", cast(cast("2025-12-01 01:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_121};"""
        exception ""
    }
    def const_sql_7_122 = """select "2025-12-01 01:00:00.999999", cast(cast("2025-12-01 01:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_122};"""
        exception ""
    }
    def const_sql_7_123 = """select "2025-12-01 01:00:01.000000", cast(cast("2025-12-01 01:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_123};"""
        exception ""
    }
    def const_sql_7_124 = """select "2025-12-01 01:00:01.000001", cast(cast("2025-12-01 01:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_124};"""
        exception ""
    }
    def const_sql_7_125 = """select "2025-12-01 01:00:01.999999", cast(cast("2025-12-01 01:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_125};"""
        exception ""
    }
    def const_sql_7_126 = """select "2025-12-01 01:00:59.000000", cast(cast("2025-12-01 01:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_126};"""
        exception ""
    }
    def const_sql_7_127 = """select "2025-12-01 01:00:59.000001", cast(cast("2025-12-01 01:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_127};"""
        exception ""
    }
    def const_sql_7_128 = """select "2025-12-01 01:00:59.999999", cast(cast("2025-12-01 01:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_128};"""
        exception ""
    }
    def const_sql_7_129 = """select "2025-12-01 01:01:00.000000", cast(cast("2025-12-01 01:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_129};"""
        exception ""
    }
    def const_sql_7_130 = """select "2025-12-01 01:01:00.000001", cast(cast("2025-12-01 01:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_130};"""
        exception ""
    }
    def const_sql_7_131 = """select "2025-12-01 01:01:00.999999", cast(cast("2025-12-01 01:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_131};"""
        exception ""
    }
    def const_sql_7_132 = """select "2025-12-01 01:01:01.000000", cast(cast("2025-12-01 01:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_132};"""
        exception ""
    }
    def const_sql_7_133 = """select "2025-12-01 01:01:01.000001", cast(cast("2025-12-01 01:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_133};"""
        exception ""
    }
    def const_sql_7_134 = """select "2025-12-01 01:01:01.999999", cast(cast("2025-12-01 01:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_134};"""
        exception ""
    }
    def const_sql_7_135 = """select "2025-12-01 01:01:59.000000", cast(cast("2025-12-01 01:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_135};"""
        exception ""
    }
    def const_sql_7_136 = """select "2025-12-01 01:01:59.000001", cast(cast("2025-12-01 01:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_136};"""
        exception ""
    }
    def const_sql_7_137 = """select "2025-12-01 01:01:59.999999", cast(cast("2025-12-01 01:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_137};"""
        exception ""
    }
    def const_sql_7_138 = """select "2025-12-01 01:59:00.000000", cast(cast("2025-12-01 01:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_138};"""
        exception ""
    }
    def const_sql_7_139 = """select "2025-12-01 01:59:00.000001", cast(cast("2025-12-01 01:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_139};"""
        exception ""
    }
    def const_sql_7_140 = """select "2025-12-01 01:59:00.999999", cast(cast("2025-12-01 01:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_140};"""
        exception ""
    }
    def const_sql_7_141 = """select "2025-12-01 01:59:01.000000", cast(cast("2025-12-01 01:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_141};"""
        exception ""
    }
    def const_sql_7_142 = """select "2025-12-01 01:59:01.000001", cast(cast("2025-12-01 01:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_142};"""
        exception ""
    }
    def const_sql_7_143 = """select "2025-12-01 01:59:01.999999", cast(cast("2025-12-01 01:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_143};"""
        exception ""
    }
    def const_sql_7_144 = """select "2025-12-01 01:59:59.000000", cast(cast("2025-12-01 01:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_144};"""
        exception ""
    }
    def const_sql_7_145 = """select "2025-12-01 01:59:59.000001", cast(cast("2025-12-01 01:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_145};"""
        exception ""
    }
    def const_sql_7_146 = """select "2025-12-01 01:59:59.999999", cast(cast("2025-12-01 01:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_146};"""
        exception ""
    }
    def const_sql_7_147 = """select "2025-12-01 23:00:00.000000", cast(cast("2025-12-01 23:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_147};"""
        exception ""
    }
    def const_sql_7_148 = """select "2025-12-01 23:00:00.000001", cast(cast("2025-12-01 23:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_148};"""
        exception ""
    }
    def const_sql_7_149 = """select "2025-12-01 23:00:00.999999", cast(cast("2025-12-01 23:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_149};"""
        exception ""
    }
    def const_sql_7_150 = """select "2025-12-01 23:00:01.000000", cast(cast("2025-12-01 23:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_150};"""
        exception ""
    }
    def const_sql_7_151 = """select "2025-12-01 23:00:01.000001", cast(cast("2025-12-01 23:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_151};"""
        exception ""
    }
    def const_sql_7_152 = """select "2025-12-01 23:00:01.999999", cast(cast("2025-12-01 23:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_152};"""
        exception ""
    }
    def const_sql_7_153 = """select "2025-12-01 23:00:59.000000", cast(cast("2025-12-01 23:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_153};"""
        exception ""
    }
    def const_sql_7_154 = """select "2025-12-01 23:00:59.000001", cast(cast("2025-12-01 23:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_154};"""
        exception ""
    }
    def const_sql_7_155 = """select "2025-12-01 23:00:59.999999", cast(cast("2025-12-01 23:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_155};"""
        exception ""
    }
    def const_sql_7_156 = """select "2025-12-01 23:01:00.000000", cast(cast("2025-12-01 23:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_156};"""
        exception ""
    }
    def const_sql_7_157 = """select "2025-12-01 23:01:00.000001", cast(cast("2025-12-01 23:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_157};"""
        exception ""
    }
    def const_sql_7_158 = """select "2025-12-01 23:01:00.999999", cast(cast("2025-12-01 23:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_158};"""
        exception ""
    }
    def const_sql_7_159 = """select "2025-12-01 23:01:01.000000", cast(cast("2025-12-01 23:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_159};"""
        exception ""
    }
    def const_sql_7_160 = """select "2025-12-01 23:01:01.000001", cast(cast("2025-12-01 23:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_160};"""
        exception ""
    }
    def const_sql_7_161 = """select "2025-12-01 23:01:01.999999", cast(cast("2025-12-01 23:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_161};"""
        exception ""
    }
    def const_sql_7_162 = """select "2025-12-01 23:01:59.000000", cast(cast("2025-12-01 23:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_162};"""
        exception ""
    }
    def const_sql_7_163 = """select "2025-12-01 23:01:59.000001", cast(cast("2025-12-01 23:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_163};"""
        exception ""
    }
    def const_sql_7_164 = """select "2025-12-01 23:01:59.999999", cast(cast("2025-12-01 23:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_164};"""
        exception ""
    }
    def const_sql_7_165 = """select "2025-12-01 23:59:00.000000", cast(cast("2025-12-01 23:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_165};"""
        exception ""
    }
    def const_sql_7_166 = """select "2025-12-01 23:59:00.000001", cast(cast("2025-12-01 23:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_166};"""
        exception ""
    }
    def const_sql_7_167 = """select "2025-12-01 23:59:00.999999", cast(cast("2025-12-01 23:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_167};"""
        exception ""
    }
    def const_sql_7_168 = """select "2025-12-01 23:59:01.000000", cast(cast("2025-12-01 23:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_168};"""
        exception ""
    }
    def const_sql_7_169 = """select "2025-12-01 23:59:01.000001", cast(cast("2025-12-01 23:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_169};"""
        exception ""
    }
    def const_sql_7_170 = """select "2025-12-01 23:59:01.999999", cast(cast("2025-12-01 23:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_170};"""
        exception ""
    }
    def const_sql_7_171 = """select "2025-12-01 23:59:59.000000", cast(cast("2025-12-01 23:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_171};"""
        exception ""
    }
    def const_sql_7_172 = """select "2025-12-01 23:59:59.000001", cast(cast("2025-12-01 23:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_172};"""
        exception ""
    }
    def const_sql_7_173 = """select "2025-12-01 23:59:59.999999", cast(cast("2025-12-01 23:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_173};"""
        exception ""
    }
    def const_sql_7_174 = """select "2025-12-28 00:00:00.000000", cast(cast("2025-12-28 00:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_174};"""
        exception ""
    }
    def const_sql_7_175 = """select "2025-12-28 00:00:00.000001", cast(cast("2025-12-28 00:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_175};"""
        exception ""
    }
    def const_sql_7_176 = """select "2025-12-28 00:00:00.999999", cast(cast("2025-12-28 00:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_176};"""
        exception ""
    }
    def const_sql_7_177 = """select "2025-12-28 00:00:01.000000", cast(cast("2025-12-28 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_177};"""
        exception ""
    }
    def const_sql_7_178 = """select "2025-12-28 00:00:01.000001", cast(cast("2025-12-28 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_178};"""
        exception ""
    }
    def const_sql_7_179 = """select "2025-12-28 00:00:01.999999", cast(cast("2025-12-28 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_179};"""
        exception ""
    }
    def const_sql_7_180 = """select "2025-12-28 00:00:59.000000", cast(cast("2025-12-28 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_180};"""
        exception ""
    }
    def const_sql_7_181 = """select "2025-12-28 00:00:59.000001", cast(cast("2025-12-28 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_181};"""
        exception ""
    }
    def const_sql_7_182 = """select "2025-12-28 00:00:59.999999", cast(cast("2025-12-28 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_182};"""
        exception ""
    }
    def const_sql_7_183 = """select "2025-12-28 00:01:00.000000", cast(cast("2025-12-28 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_183};"""
        exception ""
    }
    def const_sql_7_184 = """select "2025-12-28 00:01:00.000001", cast(cast("2025-12-28 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_184};"""
        exception ""
    }
    def const_sql_7_185 = """select "2025-12-28 00:01:00.999999", cast(cast("2025-12-28 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_185};"""
        exception ""
    }
    def const_sql_7_186 = """select "2025-12-28 00:01:01.000000", cast(cast("2025-12-28 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_186};"""
        exception ""
    }
    def const_sql_7_187 = """select "2025-12-28 00:01:01.000001", cast(cast("2025-12-28 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_187};"""
        exception ""
    }
    def const_sql_7_188 = """select "2025-12-28 00:01:01.999999", cast(cast("2025-12-28 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_188};"""
        exception ""
    }
    def const_sql_7_189 = """select "2025-12-28 00:01:59.000000", cast(cast("2025-12-28 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_189};"""
        exception ""
    }
    def const_sql_7_190 = """select "2025-12-28 00:01:59.000001", cast(cast("2025-12-28 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_190};"""
        exception ""
    }
    def const_sql_7_191 = """select "2025-12-28 00:01:59.999999", cast(cast("2025-12-28 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_191};"""
        exception ""
    }
    def const_sql_7_192 = """select "2025-12-28 00:59:00.000000", cast(cast("2025-12-28 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_192};"""
        exception ""
    }
    def const_sql_7_193 = """select "2025-12-28 00:59:00.000001", cast(cast("2025-12-28 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_193};"""
        exception ""
    }
    def const_sql_7_194 = """select "2025-12-28 00:59:00.999999", cast(cast("2025-12-28 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_7_194};"""
        exception ""
    }

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
    qt_sql_7_64_non_strict "${const_sql_7_64}"
    testFoldConst("${const_sql_7_64}")
    qt_sql_7_65_non_strict "${const_sql_7_65}"
    testFoldConst("${const_sql_7_65}")
    qt_sql_7_66_non_strict "${const_sql_7_66}"
    testFoldConst("${const_sql_7_66}")
    qt_sql_7_67_non_strict "${const_sql_7_67}"
    testFoldConst("${const_sql_7_67}")
    qt_sql_7_68_non_strict "${const_sql_7_68}"
    testFoldConst("${const_sql_7_68}")
    qt_sql_7_69_non_strict "${const_sql_7_69}"
    testFoldConst("${const_sql_7_69}")
    qt_sql_7_70_non_strict "${const_sql_7_70}"
    testFoldConst("${const_sql_7_70}")
    qt_sql_7_71_non_strict "${const_sql_7_71}"
    testFoldConst("${const_sql_7_71}")
    qt_sql_7_72_non_strict "${const_sql_7_72}"
    testFoldConst("${const_sql_7_72}")
    qt_sql_7_73_non_strict "${const_sql_7_73}"
    testFoldConst("${const_sql_7_73}")
    qt_sql_7_74_non_strict "${const_sql_7_74}"
    testFoldConst("${const_sql_7_74}")
    qt_sql_7_75_non_strict "${const_sql_7_75}"
    testFoldConst("${const_sql_7_75}")
    qt_sql_7_76_non_strict "${const_sql_7_76}"
    testFoldConst("${const_sql_7_76}")
    qt_sql_7_77_non_strict "${const_sql_7_77}"
    testFoldConst("${const_sql_7_77}")
    qt_sql_7_78_non_strict "${const_sql_7_78}"
    testFoldConst("${const_sql_7_78}")
    qt_sql_7_79_non_strict "${const_sql_7_79}"
    testFoldConst("${const_sql_7_79}")
    qt_sql_7_80_non_strict "${const_sql_7_80}"
    testFoldConst("${const_sql_7_80}")
    qt_sql_7_81_non_strict "${const_sql_7_81}"
    testFoldConst("${const_sql_7_81}")
    qt_sql_7_82_non_strict "${const_sql_7_82}"
    testFoldConst("${const_sql_7_82}")
    qt_sql_7_83_non_strict "${const_sql_7_83}"
    testFoldConst("${const_sql_7_83}")
    qt_sql_7_84_non_strict "${const_sql_7_84}"
    testFoldConst("${const_sql_7_84}")
    qt_sql_7_85_non_strict "${const_sql_7_85}"
    testFoldConst("${const_sql_7_85}")
    qt_sql_7_86_non_strict "${const_sql_7_86}"
    testFoldConst("${const_sql_7_86}")
    qt_sql_7_87_non_strict "${const_sql_7_87}"
    testFoldConst("${const_sql_7_87}")
    qt_sql_7_88_non_strict "${const_sql_7_88}"
    testFoldConst("${const_sql_7_88}")
    qt_sql_7_89_non_strict "${const_sql_7_89}"
    testFoldConst("${const_sql_7_89}")
    qt_sql_7_90_non_strict "${const_sql_7_90}"
    testFoldConst("${const_sql_7_90}")
    qt_sql_7_91_non_strict "${const_sql_7_91}"
    testFoldConst("${const_sql_7_91}")
    qt_sql_7_92_non_strict "${const_sql_7_92}"
    testFoldConst("${const_sql_7_92}")
    qt_sql_7_93_non_strict "${const_sql_7_93}"
    testFoldConst("${const_sql_7_93}")
    qt_sql_7_94_non_strict "${const_sql_7_94}"
    testFoldConst("${const_sql_7_94}")
    qt_sql_7_95_non_strict "${const_sql_7_95}"
    testFoldConst("${const_sql_7_95}")
    qt_sql_7_96_non_strict "${const_sql_7_96}"
    testFoldConst("${const_sql_7_96}")
    qt_sql_7_97_non_strict "${const_sql_7_97}"
    testFoldConst("${const_sql_7_97}")
    qt_sql_7_98_non_strict "${const_sql_7_98}"
    testFoldConst("${const_sql_7_98}")
    qt_sql_7_99_non_strict "${const_sql_7_99}"
    testFoldConst("${const_sql_7_99}")
    qt_sql_7_100_non_strict "${const_sql_7_100}"
    testFoldConst("${const_sql_7_100}")
    qt_sql_7_101_non_strict "${const_sql_7_101}"
    testFoldConst("${const_sql_7_101}")
    qt_sql_7_102_non_strict "${const_sql_7_102}"
    testFoldConst("${const_sql_7_102}")
    qt_sql_7_103_non_strict "${const_sql_7_103}"
    testFoldConst("${const_sql_7_103}")
    qt_sql_7_104_non_strict "${const_sql_7_104}"
    testFoldConst("${const_sql_7_104}")
    qt_sql_7_105_non_strict "${const_sql_7_105}"
    testFoldConst("${const_sql_7_105}")
    qt_sql_7_106_non_strict "${const_sql_7_106}"
    testFoldConst("${const_sql_7_106}")
    qt_sql_7_107_non_strict "${const_sql_7_107}"
    testFoldConst("${const_sql_7_107}")
    qt_sql_7_108_non_strict "${const_sql_7_108}"
    testFoldConst("${const_sql_7_108}")
    qt_sql_7_109_non_strict "${const_sql_7_109}"
    testFoldConst("${const_sql_7_109}")
    qt_sql_7_110_non_strict "${const_sql_7_110}"
    testFoldConst("${const_sql_7_110}")
    qt_sql_7_111_non_strict "${const_sql_7_111}"
    testFoldConst("${const_sql_7_111}")
    qt_sql_7_112_non_strict "${const_sql_7_112}"
    testFoldConst("${const_sql_7_112}")
    qt_sql_7_113_non_strict "${const_sql_7_113}"
    testFoldConst("${const_sql_7_113}")
    qt_sql_7_114_non_strict "${const_sql_7_114}"
    testFoldConst("${const_sql_7_114}")
    qt_sql_7_115_non_strict "${const_sql_7_115}"
    testFoldConst("${const_sql_7_115}")
    qt_sql_7_116_non_strict "${const_sql_7_116}"
    testFoldConst("${const_sql_7_116}")
    qt_sql_7_117_non_strict "${const_sql_7_117}"
    testFoldConst("${const_sql_7_117}")
    qt_sql_7_118_non_strict "${const_sql_7_118}"
    testFoldConst("${const_sql_7_118}")
    qt_sql_7_119_non_strict "${const_sql_7_119}"
    testFoldConst("${const_sql_7_119}")
    qt_sql_7_120_non_strict "${const_sql_7_120}"
    testFoldConst("${const_sql_7_120}")
    qt_sql_7_121_non_strict "${const_sql_7_121}"
    testFoldConst("${const_sql_7_121}")
    qt_sql_7_122_non_strict "${const_sql_7_122}"
    testFoldConst("${const_sql_7_122}")
    qt_sql_7_123_non_strict "${const_sql_7_123}"
    testFoldConst("${const_sql_7_123}")
    qt_sql_7_124_non_strict "${const_sql_7_124}"
    testFoldConst("${const_sql_7_124}")
    qt_sql_7_125_non_strict "${const_sql_7_125}"
    testFoldConst("${const_sql_7_125}")
    qt_sql_7_126_non_strict "${const_sql_7_126}"
    testFoldConst("${const_sql_7_126}")
    qt_sql_7_127_non_strict "${const_sql_7_127}"
    testFoldConst("${const_sql_7_127}")
    qt_sql_7_128_non_strict "${const_sql_7_128}"
    testFoldConst("${const_sql_7_128}")
    qt_sql_7_129_non_strict "${const_sql_7_129}"
    testFoldConst("${const_sql_7_129}")
    qt_sql_7_130_non_strict "${const_sql_7_130}"
    testFoldConst("${const_sql_7_130}")
    qt_sql_7_131_non_strict "${const_sql_7_131}"
    testFoldConst("${const_sql_7_131}")
    qt_sql_7_132_non_strict "${const_sql_7_132}"
    testFoldConst("${const_sql_7_132}")
    qt_sql_7_133_non_strict "${const_sql_7_133}"
    testFoldConst("${const_sql_7_133}")
    qt_sql_7_134_non_strict "${const_sql_7_134}"
    testFoldConst("${const_sql_7_134}")
    qt_sql_7_135_non_strict "${const_sql_7_135}"
    testFoldConst("${const_sql_7_135}")
    qt_sql_7_136_non_strict "${const_sql_7_136}"
    testFoldConst("${const_sql_7_136}")
    qt_sql_7_137_non_strict "${const_sql_7_137}"
    testFoldConst("${const_sql_7_137}")
    qt_sql_7_138_non_strict "${const_sql_7_138}"
    testFoldConst("${const_sql_7_138}")
    qt_sql_7_139_non_strict "${const_sql_7_139}"
    testFoldConst("${const_sql_7_139}")
    qt_sql_7_140_non_strict "${const_sql_7_140}"
    testFoldConst("${const_sql_7_140}")
    qt_sql_7_141_non_strict "${const_sql_7_141}"
    testFoldConst("${const_sql_7_141}")
    qt_sql_7_142_non_strict "${const_sql_7_142}"
    testFoldConst("${const_sql_7_142}")
    qt_sql_7_143_non_strict "${const_sql_7_143}"
    testFoldConst("${const_sql_7_143}")
    qt_sql_7_144_non_strict "${const_sql_7_144}"
    testFoldConst("${const_sql_7_144}")
    qt_sql_7_145_non_strict "${const_sql_7_145}"
    testFoldConst("${const_sql_7_145}")
    qt_sql_7_146_non_strict "${const_sql_7_146}"
    testFoldConst("${const_sql_7_146}")
    qt_sql_7_147_non_strict "${const_sql_7_147}"
    testFoldConst("${const_sql_7_147}")
    qt_sql_7_148_non_strict "${const_sql_7_148}"
    testFoldConst("${const_sql_7_148}")
    qt_sql_7_149_non_strict "${const_sql_7_149}"
    testFoldConst("${const_sql_7_149}")
    qt_sql_7_150_non_strict "${const_sql_7_150}"
    testFoldConst("${const_sql_7_150}")
    qt_sql_7_151_non_strict "${const_sql_7_151}"
    testFoldConst("${const_sql_7_151}")
    qt_sql_7_152_non_strict "${const_sql_7_152}"
    testFoldConst("${const_sql_7_152}")
    qt_sql_7_153_non_strict "${const_sql_7_153}"
    testFoldConst("${const_sql_7_153}")
    qt_sql_7_154_non_strict "${const_sql_7_154}"
    testFoldConst("${const_sql_7_154}")
    qt_sql_7_155_non_strict "${const_sql_7_155}"
    testFoldConst("${const_sql_7_155}")
    qt_sql_7_156_non_strict "${const_sql_7_156}"
    testFoldConst("${const_sql_7_156}")
    qt_sql_7_157_non_strict "${const_sql_7_157}"
    testFoldConst("${const_sql_7_157}")
    qt_sql_7_158_non_strict "${const_sql_7_158}"
    testFoldConst("${const_sql_7_158}")
    qt_sql_7_159_non_strict "${const_sql_7_159}"
    testFoldConst("${const_sql_7_159}")
    qt_sql_7_160_non_strict "${const_sql_7_160}"
    testFoldConst("${const_sql_7_160}")
    qt_sql_7_161_non_strict "${const_sql_7_161}"
    testFoldConst("${const_sql_7_161}")
    qt_sql_7_162_non_strict "${const_sql_7_162}"
    testFoldConst("${const_sql_7_162}")
    qt_sql_7_163_non_strict "${const_sql_7_163}"
    testFoldConst("${const_sql_7_163}")
    qt_sql_7_164_non_strict "${const_sql_7_164}"
    testFoldConst("${const_sql_7_164}")
    qt_sql_7_165_non_strict "${const_sql_7_165}"
    testFoldConst("${const_sql_7_165}")
    qt_sql_7_166_non_strict "${const_sql_7_166}"
    testFoldConst("${const_sql_7_166}")
    qt_sql_7_167_non_strict "${const_sql_7_167}"
    testFoldConst("${const_sql_7_167}")
    qt_sql_7_168_non_strict "${const_sql_7_168}"
    testFoldConst("${const_sql_7_168}")
    qt_sql_7_169_non_strict "${const_sql_7_169}"
    testFoldConst("${const_sql_7_169}")
    qt_sql_7_170_non_strict "${const_sql_7_170}"
    testFoldConst("${const_sql_7_170}")
    qt_sql_7_171_non_strict "${const_sql_7_171}"
    testFoldConst("${const_sql_7_171}")
    qt_sql_7_172_non_strict "${const_sql_7_172}"
    testFoldConst("${const_sql_7_172}")
    qt_sql_7_173_non_strict "${const_sql_7_173}"
    testFoldConst("${const_sql_7_173}")
    qt_sql_7_174_non_strict "${const_sql_7_174}"
    testFoldConst("${const_sql_7_174}")
    qt_sql_7_175_non_strict "${const_sql_7_175}"
    testFoldConst("${const_sql_7_175}")
    qt_sql_7_176_non_strict "${const_sql_7_176}"
    testFoldConst("${const_sql_7_176}")
    qt_sql_7_177_non_strict "${const_sql_7_177}"
    testFoldConst("${const_sql_7_177}")
    qt_sql_7_178_non_strict "${const_sql_7_178}"
    testFoldConst("${const_sql_7_178}")
    qt_sql_7_179_non_strict "${const_sql_7_179}"
    testFoldConst("${const_sql_7_179}")
    qt_sql_7_180_non_strict "${const_sql_7_180}"
    testFoldConst("${const_sql_7_180}")
    qt_sql_7_181_non_strict "${const_sql_7_181}"
    testFoldConst("${const_sql_7_181}")
    qt_sql_7_182_non_strict "${const_sql_7_182}"
    testFoldConst("${const_sql_7_182}")
    qt_sql_7_183_non_strict "${const_sql_7_183}"
    testFoldConst("${const_sql_7_183}")
    qt_sql_7_184_non_strict "${const_sql_7_184}"
    testFoldConst("${const_sql_7_184}")
    qt_sql_7_185_non_strict "${const_sql_7_185}"
    testFoldConst("${const_sql_7_185}")
    qt_sql_7_186_non_strict "${const_sql_7_186}"
    testFoldConst("${const_sql_7_186}")
    qt_sql_7_187_non_strict "${const_sql_7_187}"
    testFoldConst("${const_sql_7_187}")
    qt_sql_7_188_non_strict "${const_sql_7_188}"
    testFoldConst("${const_sql_7_188}")
    qt_sql_7_189_non_strict "${const_sql_7_189}"
    testFoldConst("${const_sql_7_189}")
    qt_sql_7_190_non_strict "${const_sql_7_190}"
    testFoldConst("${const_sql_7_190}")
    qt_sql_7_191_non_strict "${const_sql_7_191}"
    testFoldConst("${const_sql_7_191}")
    qt_sql_7_192_non_strict "${const_sql_7_192}"
    testFoldConst("${const_sql_7_192}")
    qt_sql_7_193_non_strict "${const_sql_7_193}"
    testFoldConst("${const_sql_7_193}")
    qt_sql_7_194_non_strict "${const_sql_7_194}"
    testFoldConst("${const_sql_7_194}")
}