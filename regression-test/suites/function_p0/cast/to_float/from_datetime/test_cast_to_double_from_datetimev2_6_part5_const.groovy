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


suite("test_cast_to_double_from_datetimev2_6_part5_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_5_0 = """select "0100-01-01 00:00:01.000000", cast(cast("0100-01-01 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_0};"""
        exception ""
    }
    def const_sql_5_1 = """select "0100-01-01 00:00:01.000001", cast(cast("0100-01-01 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_1};"""
        exception ""
    }
    def const_sql_5_2 = """select "0100-01-01 00:00:01.999999", cast(cast("0100-01-01 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_2};"""
        exception ""
    }
    def const_sql_5_3 = """select "0100-01-01 00:00:59.000000", cast(cast("0100-01-01 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_3};"""
        exception ""
    }
    def const_sql_5_4 = """select "0100-01-01 00:00:59.000001", cast(cast("0100-01-01 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_4};"""
        exception ""
    }
    def const_sql_5_5 = """select "0100-01-01 00:00:59.999999", cast(cast("0100-01-01 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_5};"""
        exception ""
    }
    def const_sql_5_6 = """select "0100-01-01 00:01:00.000000", cast(cast("0100-01-01 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_6};"""
        exception ""
    }
    def const_sql_5_7 = """select "0100-01-01 00:01:00.000001", cast(cast("0100-01-01 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_7};"""
        exception ""
    }
    def const_sql_5_8 = """select "0100-01-01 00:01:00.999999", cast(cast("0100-01-01 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_8};"""
        exception ""
    }
    def const_sql_5_9 = """select "0100-01-01 00:01:01.000000", cast(cast("0100-01-01 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_9};"""
        exception ""
    }
    def const_sql_5_10 = """select "0100-01-01 00:01:01.000001", cast(cast("0100-01-01 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_10};"""
        exception ""
    }
    def const_sql_5_11 = """select "0100-01-01 00:01:01.999999", cast(cast("0100-01-01 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_11};"""
        exception ""
    }
    def const_sql_5_12 = """select "0100-01-01 00:01:59.000000", cast(cast("0100-01-01 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_12};"""
        exception ""
    }
    def const_sql_5_13 = """select "0100-01-01 00:01:59.000001", cast(cast("0100-01-01 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_13};"""
        exception ""
    }
    def const_sql_5_14 = """select "0100-01-01 00:01:59.999999", cast(cast("0100-01-01 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_14};"""
        exception ""
    }
    def const_sql_5_15 = """select "0100-01-01 00:59:00.000000", cast(cast("0100-01-01 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_15};"""
        exception ""
    }
    def const_sql_5_16 = """select "0100-01-01 00:59:00.000001", cast(cast("0100-01-01 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_16};"""
        exception ""
    }
    def const_sql_5_17 = """select "0100-01-01 00:59:00.999999", cast(cast("0100-01-01 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_17};"""
        exception ""
    }
    def const_sql_5_18 = """select "0100-01-01 00:59:01.000000", cast(cast("0100-01-01 00:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_18};"""
        exception ""
    }
    def const_sql_5_19 = """select "0100-01-01 00:59:01.000001", cast(cast("0100-01-01 00:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_19};"""
        exception ""
    }
    def const_sql_5_20 = """select "0100-01-01 00:59:01.999999", cast(cast("0100-01-01 00:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_20};"""
        exception ""
    }
    def const_sql_5_21 = """select "0100-01-01 00:59:59.000000", cast(cast("0100-01-01 00:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_21};"""
        exception ""
    }
    def const_sql_5_22 = """select "0100-01-01 00:59:59.000001", cast(cast("0100-01-01 00:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_22};"""
        exception ""
    }
    def const_sql_5_23 = """select "0100-01-01 00:59:59.999999", cast(cast("0100-01-01 00:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_23};"""
        exception ""
    }
    def const_sql_5_24 = """select "0100-01-01 01:00:00.000000", cast(cast("0100-01-01 01:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_24};"""
        exception ""
    }
    def const_sql_5_25 = """select "0100-01-01 01:00:00.000001", cast(cast("0100-01-01 01:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_25};"""
        exception ""
    }
    def const_sql_5_26 = """select "0100-01-01 01:00:00.999999", cast(cast("0100-01-01 01:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_26};"""
        exception ""
    }
    def const_sql_5_27 = """select "0100-01-01 01:00:01.000000", cast(cast("0100-01-01 01:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_27};"""
        exception ""
    }
    def const_sql_5_28 = """select "0100-01-01 01:00:01.000001", cast(cast("0100-01-01 01:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_28};"""
        exception ""
    }
    def const_sql_5_29 = """select "0100-01-01 01:00:01.999999", cast(cast("0100-01-01 01:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_29};"""
        exception ""
    }
    def const_sql_5_30 = """select "0100-01-01 01:00:59.000000", cast(cast("0100-01-01 01:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_30};"""
        exception ""
    }
    def const_sql_5_31 = """select "0100-01-01 01:00:59.000001", cast(cast("0100-01-01 01:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_31};"""
        exception ""
    }
    def const_sql_5_32 = """select "0100-01-01 01:00:59.999999", cast(cast("0100-01-01 01:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_32};"""
        exception ""
    }
    def const_sql_5_33 = """select "0100-01-01 01:01:00.000000", cast(cast("0100-01-01 01:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_33};"""
        exception ""
    }
    def const_sql_5_34 = """select "0100-01-01 01:01:00.000001", cast(cast("0100-01-01 01:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_34};"""
        exception ""
    }
    def const_sql_5_35 = """select "0100-01-01 01:01:00.999999", cast(cast("0100-01-01 01:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_35};"""
        exception ""
    }
    def const_sql_5_36 = """select "0100-01-01 01:01:01.000000", cast(cast("0100-01-01 01:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_36};"""
        exception ""
    }
    def const_sql_5_37 = """select "0100-01-01 01:01:01.000001", cast(cast("0100-01-01 01:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_37};"""
        exception ""
    }
    def const_sql_5_38 = """select "0100-01-01 01:01:01.999999", cast(cast("0100-01-01 01:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_38};"""
        exception ""
    }
    def const_sql_5_39 = """select "0100-01-01 01:01:59.000000", cast(cast("0100-01-01 01:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_39};"""
        exception ""
    }
    def const_sql_5_40 = """select "0100-01-01 01:01:59.000001", cast(cast("0100-01-01 01:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_40};"""
        exception ""
    }
    def const_sql_5_41 = """select "0100-01-01 01:01:59.999999", cast(cast("0100-01-01 01:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_41};"""
        exception ""
    }
    def const_sql_5_42 = """select "0100-01-01 01:59:00.000000", cast(cast("0100-01-01 01:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_42};"""
        exception ""
    }
    def const_sql_5_43 = """select "0100-01-01 01:59:00.000001", cast(cast("0100-01-01 01:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_43};"""
        exception ""
    }
    def const_sql_5_44 = """select "0100-01-01 01:59:00.999999", cast(cast("0100-01-01 01:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_44};"""
        exception ""
    }
    def const_sql_5_45 = """select "0100-01-01 01:59:01.000000", cast(cast("0100-01-01 01:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_45};"""
        exception ""
    }
    def const_sql_5_46 = """select "0100-01-01 01:59:01.000001", cast(cast("0100-01-01 01:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_46};"""
        exception ""
    }
    def const_sql_5_47 = """select "0100-01-01 01:59:01.999999", cast(cast("0100-01-01 01:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_47};"""
        exception ""
    }
    def const_sql_5_48 = """select "0100-01-01 01:59:59.000000", cast(cast("0100-01-01 01:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_48};"""
        exception ""
    }
    def const_sql_5_49 = """select "0100-01-01 01:59:59.000001", cast(cast("0100-01-01 01:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_49};"""
        exception ""
    }
    def const_sql_5_50 = """select "0100-01-01 01:59:59.999999", cast(cast("0100-01-01 01:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_50};"""
        exception ""
    }
    def const_sql_5_51 = """select "0100-01-01 23:00:00.000000", cast(cast("0100-01-01 23:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_51};"""
        exception ""
    }
    def const_sql_5_52 = """select "0100-01-01 23:00:00.000001", cast(cast("0100-01-01 23:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_52};"""
        exception ""
    }
    def const_sql_5_53 = """select "0100-01-01 23:00:00.999999", cast(cast("0100-01-01 23:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_53};"""
        exception ""
    }
    def const_sql_5_54 = """select "0100-01-01 23:00:01.000000", cast(cast("0100-01-01 23:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_54};"""
        exception ""
    }
    def const_sql_5_55 = """select "0100-01-01 23:00:01.000001", cast(cast("0100-01-01 23:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_55};"""
        exception ""
    }
    def const_sql_5_56 = """select "0100-01-01 23:00:01.999999", cast(cast("0100-01-01 23:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_56};"""
        exception ""
    }
    def const_sql_5_57 = """select "0100-01-01 23:00:59.000000", cast(cast("0100-01-01 23:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_57};"""
        exception ""
    }
    def const_sql_5_58 = """select "0100-01-01 23:00:59.000001", cast(cast("0100-01-01 23:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_58};"""
        exception ""
    }
    def const_sql_5_59 = """select "0100-01-01 23:00:59.999999", cast(cast("0100-01-01 23:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_59};"""
        exception ""
    }
    def const_sql_5_60 = """select "0100-01-01 23:01:00.000000", cast(cast("0100-01-01 23:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_60};"""
        exception ""
    }
    def const_sql_5_61 = """select "0100-01-01 23:01:00.000001", cast(cast("0100-01-01 23:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_61};"""
        exception ""
    }
    def const_sql_5_62 = """select "0100-01-01 23:01:00.999999", cast(cast("0100-01-01 23:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_62};"""
        exception ""
    }
    def const_sql_5_63 = """select "0100-01-01 23:01:01.000000", cast(cast("0100-01-01 23:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_63};"""
        exception ""
    }
    def const_sql_5_64 = """select "0100-01-01 23:01:01.000001", cast(cast("0100-01-01 23:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_64};"""
        exception ""
    }
    def const_sql_5_65 = """select "0100-01-01 23:01:01.999999", cast(cast("0100-01-01 23:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_65};"""
        exception ""
    }
    def const_sql_5_66 = """select "0100-01-01 23:01:59.000000", cast(cast("0100-01-01 23:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_66};"""
        exception ""
    }
    def const_sql_5_67 = """select "0100-01-01 23:01:59.000001", cast(cast("0100-01-01 23:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_67};"""
        exception ""
    }
    def const_sql_5_68 = """select "0100-01-01 23:01:59.999999", cast(cast("0100-01-01 23:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_68};"""
        exception ""
    }
    def const_sql_5_69 = """select "0100-01-01 23:59:00.000000", cast(cast("0100-01-01 23:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_69};"""
        exception ""
    }
    def const_sql_5_70 = """select "0100-01-01 23:59:00.000001", cast(cast("0100-01-01 23:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_70};"""
        exception ""
    }
    def const_sql_5_71 = """select "0100-01-01 23:59:00.999999", cast(cast("0100-01-01 23:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_71};"""
        exception ""
    }
    def const_sql_5_72 = """select "0100-01-01 23:59:01.000000", cast(cast("0100-01-01 23:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_72};"""
        exception ""
    }
    def const_sql_5_73 = """select "0100-01-01 23:59:01.000001", cast(cast("0100-01-01 23:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_73};"""
        exception ""
    }
    def const_sql_5_74 = """select "0100-01-01 23:59:01.999999", cast(cast("0100-01-01 23:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_74};"""
        exception ""
    }
    def const_sql_5_75 = """select "0100-01-01 23:59:59.000000", cast(cast("0100-01-01 23:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_75};"""
        exception ""
    }
    def const_sql_5_76 = """select "0100-01-01 23:59:59.000001", cast(cast("0100-01-01 23:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_76};"""
        exception ""
    }
    def const_sql_5_77 = """select "0100-01-01 23:59:59.999999", cast(cast("0100-01-01 23:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_77};"""
        exception ""
    }
    def const_sql_5_78 = """select "0100-01-28 00:00:00.000000", cast(cast("0100-01-28 00:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_78};"""
        exception ""
    }
    def const_sql_5_79 = """select "0100-01-28 00:00:00.000001", cast(cast("0100-01-28 00:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_79};"""
        exception ""
    }
    def const_sql_5_80 = """select "0100-01-28 00:00:00.999999", cast(cast("0100-01-28 00:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_80};"""
        exception ""
    }
    def const_sql_5_81 = """select "0100-01-28 00:00:01.000000", cast(cast("0100-01-28 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_81};"""
        exception ""
    }
    def const_sql_5_82 = """select "0100-01-28 00:00:01.000001", cast(cast("0100-01-28 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_82};"""
        exception ""
    }
    def const_sql_5_83 = """select "0100-01-28 00:00:01.999999", cast(cast("0100-01-28 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_83};"""
        exception ""
    }
    def const_sql_5_84 = """select "0100-01-28 00:00:59.000000", cast(cast("0100-01-28 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_84};"""
        exception ""
    }
    def const_sql_5_85 = """select "0100-01-28 00:00:59.000001", cast(cast("0100-01-28 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_85};"""
        exception ""
    }
    def const_sql_5_86 = """select "0100-01-28 00:00:59.999999", cast(cast("0100-01-28 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_86};"""
        exception ""
    }
    def const_sql_5_87 = """select "0100-01-28 00:01:00.000000", cast(cast("0100-01-28 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_87};"""
        exception ""
    }
    def const_sql_5_88 = """select "0100-01-28 00:01:00.000001", cast(cast("0100-01-28 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_88};"""
        exception ""
    }
    def const_sql_5_89 = """select "0100-01-28 00:01:00.999999", cast(cast("0100-01-28 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_89};"""
        exception ""
    }
    def const_sql_5_90 = """select "0100-01-28 00:01:01.000000", cast(cast("0100-01-28 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_90};"""
        exception ""
    }
    def const_sql_5_91 = """select "0100-01-28 00:01:01.000001", cast(cast("0100-01-28 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_91};"""
        exception ""
    }
    def const_sql_5_92 = """select "0100-01-28 00:01:01.999999", cast(cast("0100-01-28 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_92};"""
        exception ""
    }
    def const_sql_5_93 = """select "0100-01-28 00:01:59.000000", cast(cast("0100-01-28 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_93};"""
        exception ""
    }
    def const_sql_5_94 = """select "0100-01-28 00:01:59.000001", cast(cast("0100-01-28 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_94};"""
        exception ""
    }
    def const_sql_5_95 = """select "0100-01-28 00:01:59.999999", cast(cast("0100-01-28 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_95};"""
        exception ""
    }
    def const_sql_5_96 = """select "0100-01-28 00:59:00.000000", cast(cast("0100-01-28 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_96};"""
        exception ""
    }
    def const_sql_5_97 = """select "0100-01-28 00:59:00.000001", cast(cast("0100-01-28 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_97};"""
        exception ""
    }
    def const_sql_5_98 = """select "0100-01-28 00:59:00.999999", cast(cast("0100-01-28 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_98};"""
        exception ""
    }
    def const_sql_5_99 = """select "0100-01-28 00:59:01.000000", cast(cast("0100-01-28 00:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_99};"""
        exception ""
    }
    def const_sql_5_100 = """select "0100-01-28 00:59:01.000001", cast(cast("0100-01-28 00:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_100};"""
        exception ""
    }
    def const_sql_5_101 = """select "0100-01-28 00:59:01.999999", cast(cast("0100-01-28 00:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_101};"""
        exception ""
    }
    def const_sql_5_102 = """select "0100-01-28 00:59:59.000000", cast(cast("0100-01-28 00:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_102};"""
        exception ""
    }
    def const_sql_5_103 = """select "0100-01-28 00:59:59.000001", cast(cast("0100-01-28 00:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_103};"""
        exception ""
    }
    def const_sql_5_104 = """select "0100-01-28 00:59:59.999999", cast(cast("0100-01-28 00:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_104};"""
        exception ""
    }
    def const_sql_5_105 = """select "0100-01-28 01:00:00.000000", cast(cast("0100-01-28 01:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_105};"""
        exception ""
    }
    def const_sql_5_106 = """select "0100-01-28 01:00:00.000001", cast(cast("0100-01-28 01:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_106};"""
        exception ""
    }
    def const_sql_5_107 = """select "0100-01-28 01:00:00.999999", cast(cast("0100-01-28 01:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_107};"""
        exception ""
    }
    def const_sql_5_108 = """select "0100-01-28 01:00:01.000000", cast(cast("0100-01-28 01:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_108};"""
        exception ""
    }
    def const_sql_5_109 = """select "0100-01-28 01:00:01.000001", cast(cast("0100-01-28 01:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_109};"""
        exception ""
    }
    def const_sql_5_110 = """select "0100-01-28 01:00:01.999999", cast(cast("0100-01-28 01:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_110};"""
        exception ""
    }
    def const_sql_5_111 = """select "0100-01-28 01:00:59.000000", cast(cast("0100-01-28 01:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_111};"""
        exception ""
    }
    def const_sql_5_112 = """select "0100-01-28 01:00:59.000001", cast(cast("0100-01-28 01:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_112};"""
        exception ""
    }
    def const_sql_5_113 = """select "0100-01-28 01:00:59.999999", cast(cast("0100-01-28 01:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_113};"""
        exception ""
    }
    def const_sql_5_114 = """select "0100-01-28 01:01:00.000000", cast(cast("0100-01-28 01:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_114};"""
        exception ""
    }
    def const_sql_5_115 = """select "0100-01-28 01:01:00.000001", cast(cast("0100-01-28 01:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_115};"""
        exception ""
    }
    def const_sql_5_116 = """select "0100-01-28 01:01:00.999999", cast(cast("0100-01-28 01:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_116};"""
        exception ""
    }
    def const_sql_5_117 = """select "0100-01-28 01:01:01.000000", cast(cast("0100-01-28 01:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_117};"""
        exception ""
    }
    def const_sql_5_118 = """select "0100-01-28 01:01:01.000001", cast(cast("0100-01-28 01:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_118};"""
        exception ""
    }
    def const_sql_5_119 = """select "0100-01-28 01:01:01.999999", cast(cast("0100-01-28 01:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_119};"""
        exception ""
    }
    def const_sql_5_120 = """select "0100-01-28 01:01:59.000000", cast(cast("0100-01-28 01:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_120};"""
        exception ""
    }
    def const_sql_5_121 = """select "0100-01-28 01:01:59.000001", cast(cast("0100-01-28 01:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_121};"""
        exception ""
    }
    def const_sql_5_122 = """select "0100-01-28 01:01:59.999999", cast(cast("0100-01-28 01:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_122};"""
        exception ""
    }
    def const_sql_5_123 = """select "0100-01-28 01:59:00.000000", cast(cast("0100-01-28 01:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_123};"""
        exception ""
    }
    def const_sql_5_124 = """select "0100-01-28 01:59:00.000001", cast(cast("0100-01-28 01:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_124};"""
        exception ""
    }
    def const_sql_5_125 = """select "0100-01-28 01:59:00.999999", cast(cast("0100-01-28 01:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_125};"""
        exception ""
    }
    def const_sql_5_126 = """select "0100-01-28 01:59:01.000000", cast(cast("0100-01-28 01:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_126};"""
        exception ""
    }
    def const_sql_5_127 = """select "0100-01-28 01:59:01.000001", cast(cast("0100-01-28 01:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_127};"""
        exception ""
    }
    def const_sql_5_128 = """select "0100-01-28 01:59:01.999999", cast(cast("0100-01-28 01:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_128};"""
        exception ""
    }
    def const_sql_5_129 = """select "0100-01-28 01:59:59.000000", cast(cast("0100-01-28 01:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_129};"""
        exception ""
    }
    def const_sql_5_130 = """select "0100-01-28 01:59:59.000001", cast(cast("0100-01-28 01:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_130};"""
        exception ""
    }
    def const_sql_5_131 = """select "0100-01-28 01:59:59.999999", cast(cast("0100-01-28 01:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_131};"""
        exception ""
    }
    def const_sql_5_132 = """select "0100-01-28 23:00:00.000000", cast(cast("0100-01-28 23:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_132};"""
        exception ""
    }
    def const_sql_5_133 = """select "0100-01-28 23:00:00.000001", cast(cast("0100-01-28 23:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_133};"""
        exception ""
    }
    def const_sql_5_134 = """select "0100-01-28 23:00:00.999999", cast(cast("0100-01-28 23:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_134};"""
        exception ""
    }
    def const_sql_5_135 = """select "0100-01-28 23:00:01.000000", cast(cast("0100-01-28 23:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_135};"""
        exception ""
    }
    def const_sql_5_136 = """select "0100-01-28 23:00:01.000001", cast(cast("0100-01-28 23:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_136};"""
        exception ""
    }
    def const_sql_5_137 = """select "0100-01-28 23:00:01.999999", cast(cast("0100-01-28 23:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_137};"""
        exception ""
    }
    def const_sql_5_138 = """select "0100-01-28 23:00:59.000000", cast(cast("0100-01-28 23:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_138};"""
        exception ""
    }
    def const_sql_5_139 = """select "0100-01-28 23:00:59.000001", cast(cast("0100-01-28 23:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_139};"""
        exception ""
    }
    def const_sql_5_140 = """select "0100-01-28 23:00:59.999999", cast(cast("0100-01-28 23:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_140};"""
        exception ""
    }
    def const_sql_5_141 = """select "0100-01-28 23:01:00.000000", cast(cast("0100-01-28 23:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_141};"""
        exception ""
    }
    def const_sql_5_142 = """select "0100-01-28 23:01:00.000001", cast(cast("0100-01-28 23:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_142};"""
        exception ""
    }
    def const_sql_5_143 = """select "0100-01-28 23:01:00.999999", cast(cast("0100-01-28 23:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_143};"""
        exception ""
    }
    def const_sql_5_144 = """select "0100-01-28 23:01:01.000000", cast(cast("0100-01-28 23:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_144};"""
        exception ""
    }
    def const_sql_5_145 = """select "0100-01-28 23:01:01.000001", cast(cast("0100-01-28 23:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_145};"""
        exception ""
    }
    def const_sql_5_146 = """select "0100-01-28 23:01:01.999999", cast(cast("0100-01-28 23:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_146};"""
        exception ""
    }
    def const_sql_5_147 = """select "0100-01-28 23:01:59.000000", cast(cast("0100-01-28 23:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_147};"""
        exception ""
    }
    def const_sql_5_148 = """select "0100-01-28 23:01:59.000001", cast(cast("0100-01-28 23:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_148};"""
        exception ""
    }
    def const_sql_5_149 = """select "0100-01-28 23:01:59.999999", cast(cast("0100-01-28 23:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_149};"""
        exception ""
    }
    def const_sql_5_150 = """select "0100-01-28 23:59:00.000000", cast(cast("0100-01-28 23:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_150};"""
        exception ""
    }
    def const_sql_5_151 = """select "0100-01-28 23:59:00.000001", cast(cast("0100-01-28 23:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_151};"""
        exception ""
    }
    def const_sql_5_152 = """select "0100-01-28 23:59:00.999999", cast(cast("0100-01-28 23:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_152};"""
        exception ""
    }
    def const_sql_5_153 = """select "0100-01-28 23:59:01.000000", cast(cast("0100-01-28 23:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_153};"""
        exception ""
    }
    def const_sql_5_154 = """select "0100-01-28 23:59:01.000001", cast(cast("0100-01-28 23:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_154};"""
        exception ""
    }
    def const_sql_5_155 = """select "0100-01-28 23:59:01.999999", cast(cast("0100-01-28 23:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_155};"""
        exception ""
    }
    def const_sql_5_156 = """select "0100-01-28 23:59:59.000000", cast(cast("0100-01-28 23:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_156};"""
        exception ""
    }
    def const_sql_5_157 = """select "0100-01-28 23:59:59.000001", cast(cast("0100-01-28 23:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_157};"""
        exception ""
    }
    def const_sql_5_158 = """select "0100-01-28 23:59:59.999999", cast(cast("0100-01-28 23:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_158};"""
        exception ""
    }
    def const_sql_5_159 = """select "0100-12-01 00:00:00.000000", cast(cast("0100-12-01 00:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_159};"""
        exception ""
    }
    def const_sql_5_160 = """select "0100-12-01 00:00:00.000001", cast(cast("0100-12-01 00:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_160};"""
        exception ""
    }
    def const_sql_5_161 = """select "0100-12-01 00:00:00.999999", cast(cast("0100-12-01 00:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_161};"""
        exception ""
    }
    def const_sql_5_162 = """select "0100-12-01 00:00:01.000000", cast(cast("0100-12-01 00:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_162};"""
        exception ""
    }
    def const_sql_5_163 = """select "0100-12-01 00:00:01.000001", cast(cast("0100-12-01 00:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_163};"""
        exception ""
    }
    def const_sql_5_164 = """select "0100-12-01 00:00:01.999999", cast(cast("0100-12-01 00:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_164};"""
        exception ""
    }
    def const_sql_5_165 = """select "0100-12-01 00:00:59.000000", cast(cast("0100-12-01 00:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_165};"""
        exception ""
    }
    def const_sql_5_166 = """select "0100-12-01 00:00:59.000001", cast(cast("0100-12-01 00:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_166};"""
        exception ""
    }
    def const_sql_5_167 = """select "0100-12-01 00:00:59.999999", cast(cast("0100-12-01 00:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_167};"""
        exception ""
    }
    def const_sql_5_168 = """select "0100-12-01 00:01:00.000000", cast(cast("0100-12-01 00:01:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_168};"""
        exception ""
    }
    def const_sql_5_169 = """select "0100-12-01 00:01:00.000001", cast(cast("0100-12-01 00:01:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_169};"""
        exception ""
    }
    def const_sql_5_170 = """select "0100-12-01 00:01:00.999999", cast(cast("0100-12-01 00:01:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_170};"""
        exception ""
    }
    def const_sql_5_171 = """select "0100-12-01 00:01:01.000000", cast(cast("0100-12-01 00:01:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_171};"""
        exception ""
    }
    def const_sql_5_172 = """select "0100-12-01 00:01:01.000001", cast(cast("0100-12-01 00:01:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_172};"""
        exception ""
    }
    def const_sql_5_173 = """select "0100-12-01 00:01:01.999999", cast(cast("0100-12-01 00:01:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_173};"""
        exception ""
    }
    def const_sql_5_174 = """select "0100-12-01 00:01:59.000000", cast(cast("0100-12-01 00:01:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_174};"""
        exception ""
    }
    def const_sql_5_175 = """select "0100-12-01 00:01:59.000001", cast(cast("0100-12-01 00:01:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_175};"""
        exception ""
    }
    def const_sql_5_176 = """select "0100-12-01 00:01:59.999999", cast(cast("0100-12-01 00:01:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_176};"""
        exception ""
    }
    def const_sql_5_177 = """select "0100-12-01 00:59:00.000000", cast(cast("0100-12-01 00:59:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_177};"""
        exception ""
    }
    def const_sql_5_178 = """select "0100-12-01 00:59:00.000001", cast(cast("0100-12-01 00:59:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_178};"""
        exception ""
    }
    def const_sql_5_179 = """select "0100-12-01 00:59:00.999999", cast(cast("0100-12-01 00:59:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_179};"""
        exception ""
    }
    def const_sql_5_180 = """select "0100-12-01 00:59:01.000000", cast(cast("0100-12-01 00:59:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_180};"""
        exception ""
    }
    def const_sql_5_181 = """select "0100-12-01 00:59:01.000001", cast(cast("0100-12-01 00:59:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_181};"""
        exception ""
    }
    def const_sql_5_182 = """select "0100-12-01 00:59:01.999999", cast(cast("0100-12-01 00:59:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_182};"""
        exception ""
    }
    def const_sql_5_183 = """select "0100-12-01 00:59:59.000000", cast(cast("0100-12-01 00:59:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_183};"""
        exception ""
    }
    def const_sql_5_184 = """select "0100-12-01 00:59:59.000001", cast(cast("0100-12-01 00:59:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_184};"""
        exception ""
    }
    def const_sql_5_185 = """select "0100-12-01 00:59:59.999999", cast(cast("0100-12-01 00:59:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_185};"""
        exception ""
    }
    def const_sql_5_186 = """select "0100-12-01 01:00:00.000000", cast(cast("0100-12-01 01:00:00.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_186};"""
        exception ""
    }
    def const_sql_5_187 = """select "0100-12-01 01:00:00.000001", cast(cast("0100-12-01 01:00:00.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_187};"""
        exception ""
    }
    def const_sql_5_188 = """select "0100-12-01 01:00:00.999999", cast(cast("0100-12-01 01:00:00.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_188};"""
        exception ""
    }
    def const_sql_5_189 = """select "0100-12-01 01:00:01.000000", cast(cast("0100-12-01 01:00:01.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_189};"""
        exception ""
    }
    def const_sql_5_190 = """select "0100-12-01 01:00:01.000001", cast(cast("0100-12-01 01:00:01.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_190};"""
        exception ""
    }
    def const_sql_5_191 = """select "0100-12-01 01:00:01.999999", cast(cast("0100-12-01 01:00:01.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_191};"""
        exception ""
    }
    def const_sql_5_192 = """select "0100-12-01 01:00:59.000000", cast(cast("0100-12-01 01:00:59.000000" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_192};"""
        exception ""
    }
    def const_sql_5_193 = """select "0100-12-01 01:00:59.000001", cast(cast("0100-12-01 01:00:59.000001" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_193};"""
        exception ""
    }
    def const_sql_5_194 = """select "0100-12-01 01:00:59.999999", cast(cast("0100-12-01 01:00:59.999999" as datetimev2(6)) as double);"""

    test {
        sql """${const_sql_5_194};"""
        exception ""
    }

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
    qt_sql_5_64_non_strict "${const_sql_5_64}"
    testFoldConst("${const_sql_5_64}")
    qt_sql_5_65_non_strict "${const_sql_5_65}"
    testFoldConst("${const_sql_5_65}")
    qt_sql_5_66_non_strict "${const_sql_5_66}"
    testFoldConst("${const_sql_5_66}")
    qt_sql_5_67_non_strict "${const_sql_5_67}"
    testFoldConst("${const_sql_5_67}")
    qt_sql_5_68_non_strict "${const_sql_5_68}"
    testFoldConst("${const_sql_5_68}")
    qt_sql_5_69_non_strict "${const_sql_5_69}"
    testFoldConst("${const_sql_5_69}")
    qt_sql_5_70_non_strict "${const_sql_5_70}"
    testFoldConst("${const_sql_5_70}")
    qt_sql_5_71_non_strict "${const_sql_5_71}"
    testFoldConst("${const_sql_5_71}")
    qt_sql_5_72_non_strict "${const_sql_5_72}"
    testFoldConst("${const_sql_5_72}")
    qt_sql_5_73_non_strict "${const_sql_5_73}"
    testFoldConst("${const_sql_5_73}")
    qt_sql_5_74_non_strict "${const_sql_5_74}"
    testFoldConst("${const_sql_5_74}")
    qt_sql_5_75_non_strict "${const_sql_5_75}"
    testFoldConst("${const_sql_5_75}")
    qt_sql_5_76_non_strict "${const_sql_5_76}"
    testFoldConst("${const_sql_5_76}")
    qt_sql_5_77_non_strict "${const_sql_5_77}"
    testFoldConst("${const_sql_5_77}")
    qt_sql_5_78_non_strict "${const_sql_5_78}"
    testFoldConst("${const_sql_5_78}")
    qt_sql_5_79_non_strict "${const_sql_5_79}"
    testFoldConst("${const_sql_5_79}")
    qt_sql_5_80_non_strict "${const_sql_5_80}"
    testFoldConst("${const_sql_5_80}")
    qt_sql_5_81_non_strict "${const_sql_5_81}"
    testFoldConst("${const_sql_5_81}")
    qt_sql_5_82_non_strict "${const_sql_5_82}"
    testFoldConst("${const_sql_5_82}")
    qt_sql_5_83_non_strict "${const_sql_5_83}"
    testFoldConst("${const_sql_5_83}")
    qt_sql_5_84_non_strict "${const_sql_5_84}"
    testFoldConst("${const_sql_5_84}")
    qt_sql_5_85_non_strict "${const_sql_5_85}"
    testFoldConst("${const_sql_5_85}")
    qt_sql_5_86_non_strict "${const_sql_5_86}"
    testFoldConst("${const_sql_5_86}")
    qt_sql_5_87_non_strict "${const_sql_5_87}"
    testFoldConst("${const_sql_5_87}")
    qt_sql_5_88_non_strict "${const_sql_5_88}"
    testFoldConst("${const_sql_5_88}")
    qt_sql_5_89_non_strict "${const_sql_5_89}"
    testFoldConst("${const_sql_5_89}")
    qt_sql_5_90_non_strict "${const_sql_5_90}"
    testFoldConst("${const_sql_5_90}")
    qt_sql_5_91_non_strict "${const_sql_5_91}"
    testFoldConst("${const_sql_5_91}")
    qt_sql_5_92_non_strict "${const_sql_5_92}"
    testFoldConst("${const_sql_5_92}")
    qt_sql_5_93_non_strict "${const_sql_5_93}"
    testFoldConst("${const_sql_5_93}")
    qt_sql_5_94_non_strict "${const_sql_5_94}"
    testFoldConst("${const_sql_5_94}")
    qt_sql_5_95_non_strict "${const_sql_5_95}"
    testFoldConst("${const_sql_5_95}")
    qt_sql_5_96_non_strict "${const_sql_5_96}"
    testFoldConst("${const_sql_5_96}")
    qt_sql_5_97_non_strict "${const_sql_5_97}"
    testFoldConst("${const_sql_5_97}")
    qt_sql_5_98_non_strict "${const_sql_5_98}"
    testFoldConst("${const_sql_5_98}")
    qt_sql_5_99_non_strict "${const_sql_5_99}"
    testFoldConst("${const_sql_5_99}")
    qt_sql_5_100_non_strict "${const_sql_5_100}"
    testFoldConst("${const_sql_5_100}")
    qt_sql_5_101_non_strict "${const_sql_5_101}"
    testFoldConst("${const_sql_5_101}")
    qt_sql_5_102_non_strict "${const_sql_5_102}"
    testFoldConst("${const_sql_5_102}")
    qt_sql_5_103_non_strict "${const_sql_5_103}"
    testFoldConst("${const_sql_5_103}")
    qt_sql_5_104_non_strict "${const_sql_5_104}"
    testFoldConst("${const_sql_5_104}")
    qt_sql_5_105_non_strict "${const_sql_5_105}"
    testFoldConst("${const_sql_5_105}")
    qt_sql_5_106_non_strict "${const_sql_5_106}"
    testFoldConst("${const_sql_5_106}")
    qt_sql_5_107_non_strict "${const_sql_5_107}"
    testFoldConst("${const_sql_5_107}")
    qt_sql_5_108_non_strict "${const_sql_5_108}"
    testFoldConst("${const_sql_5_108}")
    qt_sql_5_109_non_strict "${const_sql_5_109}"
    testFoldConst("${const_sql_5_109}")
    qt_sql_5_110_non_strict "${const_sql_5_110}"
    testFoldConst("${const_sql_5_110}")
    qt_sql_5_111_non_strict "${const_sql_5_111}"
    testFoldConst("${const_sql_5_111}")
    qt_sql_5_112_non_strict "${const_sql_5_112}"
    testFoldConst("${const_sql_5_112}")
    qt_sql_5_113_non_strict "${const_sql_5_113}"
    testFoldConst("${const_sql_5_113}")
    qt_sql_5_114_non_strict "${const_sql_5_114}"
    testFoldConst("${const_sql_5_114}")
    qt_sql_5_115_non_strict "${const_sql_5_115}"
    testFoldConst("${const_sql_5_115}")
    qt_sql_5_116_non_strict "${const_sql_5_116}"
    testFoldConst("${const_sql_5_116}")
    qt_sql_5_117_non_strict "${const_sql_5_117}"
    testFoldConst("${const_sql_5_117}")
    qt_sql_5_118_non_strict "${const_sql_5_118}"
    testFoldConst("${const_sql_5_118}")
    qt_sql_5_119_non_strict "${const_sql_5_119}"
    testFoldConst("${const_sql_5_119}")
    qt_sql_5_120_non_strict "${const_sql_5_120}"
    testFoldConst("${const_sql_5_120}")
    qt_sql_5_121_non_strict "${const_sql_5_121}"
    testFoldConst("${const_sql_5_121}")
    qt_sql_5_122_non_strict "${const_sql_5_122}"
    testFoldConst("${const_sql_5_122}")
    qt_sql_5_123_non_strict "${const_sql_5_123}"
    testFoldConst("${const_sql_5_123}")
    qt_sql_5_124_non_strict "${const_sql_5_124}"
    testFoldConst("${const_sql_5_124}")
    qt_sql_5_125_non_strict "${const_sql_5_125}"
    testFoldConst("${const_sql_5_125}")
    qt_sql_5_126_non_strict "${const_sql_5_126}"
    testFoldConst("${const_sql_5_126}")
    qt_sql_5_127_non_strict "${const_sql_5_127}"
    testFoldConst("${const_sql_5_127}")
    qt_sql_5_128_non_strict "${const_sql_5_128}"
    testFoldConst("${const_sql_5_128}")
    qt_sql_5_129_non_strict "${const_sql_5_129}"
    testFoldConst("${const_sql_5_129}")
    qt_sql_5_130_non_strict "${const_sql_5_130}"
    testFoldConst("${const_sql_5_130}")
    qt_sql_5_131_non_strict "${const_sql_5_131}"
    testFoldConst("${const_sql_5_131}")
    qt_sql_5_132_non_strict "${const_sql_5_132}"
    testFoldConst("${const_sql_5_132}")
    qt_sql_5_133_non_strict "${const_sql_5_133}"
    testFoldConst("${const_sql_5_133}")
    qt_sql_5_134_non_strict "${const_sql_5_134}"
    testFoldConst("${const_sql_5_134}")
    qt_sql_5_135_non_strict "${const_sql_5_135}"
    testFoldConst("${const_sql_5_135}")
    qt_sql_5_136_non_strict "${const_sql_5_136}"
    testFoldConst("${const_sql_5_136}")
    qt_sql_5_137_non_strict "${const_sql_5_137}"
    testFoldConst("${const_sql_5_137}")
    qt_sql_5_138_non_strict "${const_sql_5_138}"
    testFoldConst("${const_sql_5_138}")
    qt_sql_5_139_non_strict "${const_sql_5_139}"
    testFoldConst("${const_sql_5_139}")
    qt_sql_5_140_non_strict "${const_sql_5_140}"
    testFoldConst("${const_sql_5_140}")
    qt_sql_5_141_non_strict "${const_sql_5_141}"
    testFoldConst("${const_sql_5_141}")
    qt_sql_5_142_non_strict "${const_sql_5_142}"
    testFoldConst("${const_sql_5_142}")
    qt_sql_5_143_non_strict "${const_sql_5_143}"
    testFoldConst("${const_sql_5_143}")
    qt_sql_5_144_non_strict "${const_sql_5_144}"
    testFoldConst("${const_sql_5_144}")
    qt_sql_5_145_non_strict "${const_sql_5_145}"
    testFoldConst("${const_sql_5_145}")
    qt_sql_5_146_non_strict "${const_sql_5_146}"
    testFoldConst("${const_sql_5_146}")
    qt_sql_5_147_non_strict "${const_sql_5_147}"
    testFoldConst("${const_sql_5_147}")
    qt_sql_5_148_non_strict "${const_sql_5_148}"
    testFoldConst("${const_sql_5_148}")
    qt_sql_5_149_non_strict "${const_sql_5_149}"
    testFoldConst("${const_sql_5_149}")
    qt_sql_5_150_non_strict "${const_sql_5_150}"
    testFoldConst("${const_sql_5_150}")
    qt_sql_5_151_non_strict "${const_sql_5_151}"
    testFoldConst("${const_sql_5_151}")
    qt_sql_5_152_non_strict "${const_sql_5_152}"
    testFoldConst("${const_sql_5_152}")
    qt_sql_5_153_non_strict "${const_sql_5_153}"
    testFoldConst("${const_sql_5_153}")
    qt_sql_5_154_non_strict "${const_sql_5_154}"
    testFoldConst("${const_sql_5_154}")
    qt_sql_5_155_non_strict "${const_sql_5_155}"
    testFoldConst("${const_sql_5_155}")
    qt_sql_5_156_non_strict "${const_sql_5_156}"
    testFoldConst("${const_sql_5_156}")
    qt_sql_5_157_non_strict "${const_sql_5_157}"
    testFoldConst("${const_sql_5_157}")
    qt_sql_5_158_non_strict "${const_sql_5_158}"
    testFoldConst("${const_sql_5_158}")
    qt_sql_5_159_non_strict "${const_sql_5_159}"
    testFoldConst("${const_sql_5_159}")
    qt_sql_5_160_non_strict "${const_sql_5_160}"
    testFoldConst("${const_sql_5_160}")
    qt_sql_5_161_non_strict "${const_sql_5_161}"
    testFoldConst("${const_sql_5_161}")
    qt_sql_5_162_non_strict "${const_sql_5_162}"
    testFoldConst("${const_sql_5_162}")
    qt_sql_5_163_non_strict "${const_sql_5_163}"
    testFoldConst("${const_sql_5_163}")
    qt_sql_5_164_non_strict "${const_sql_5_164}"
    testFoldConst("${const_sql_5_164}")
    qt_sql_5_165_non_strict "${const_sql_5_165}"
    testFoldConst("${const_sql_5_165}")
    qt_sql_5_166_non_strict "${const_sql_5_166}"
    testFoldConst("${const_sql_5_166}")
    qt_sql_5_167_non_strict "${const_sql_5_167}"
    testFoldConst("${const_sql_5_167}")
    qt_sql_5_168_non_strict "${const_sql_5_168}"
    testFoldConst("${const_sql_5_168}")
    qt_sql_5_169_non_strict "${const_sql_5_169}"
    testFoldConst("${const_sql_5_169}")
    qt_sql_5_170_non_strict "${const_sql_5_170}"
    testFoldConst("${const_sql_5_170}")
    qt_sql_5_171_non_strict "${const_sql_5_171}"
    testFoldConst("${const_sql_5_171}")
    qt_sql_5_172_non_strict "${const_sql_5_172}"
    testFoldConst("${const_sql_5_172}")
    qt_sql_5_173_non_strict "${const_sql_5_173}"
    testFoldConst("${const_sql_5_173}")
    qt_sql_5_174_non_strict "${const_sql_5_174}"
    testFoldConst("${const_sql_5_174}")
    qt_sql_5_175_non_strict "${const_sql_5_175}"
    testFoldConst("${const_sql_5_175}")
    qt_sql_5_176_non_strict "${const_sql_5_176}"
    testFoldConst("${const_sql_5_176}")
    qt_sql_5_177_non_strict "${const_sql_5_177}"
    testFoldConst("${const_sql_5_177}")
    qt_sql_5_178_non_strict "${const_sql_5_178}"
    testFoldConst("${const_sql_5_178}")
    qt_sql_5_179_non_strict "${const_sql_5_179}"
    testFoldConst("${const_sql_5_179}")
    qt_sql_5_180_non_strict "${const_sql_5_180}"
    testFoldConst("${const_sql_5_180}")
    qt_sql_5_181_non_strict "${const_sql_5_181}"
    testFoldConst("${const_sql_5_181}")
    qt_sql_5_182_non_strict "${const_sql_5_182}"
    testFoldConst("${const_sql_5_182}")
    qt_sql_5_183_non_strict "${const_sql_5_183}"
    testFoldConst("${const_sql_5_183}")
    qt_sql_5_184_non_strict "${const_sql_5_184}"
    testFoldConst("${const_sql_5_184}")
    qt_sql_5_185_non_strict "${const_sql_5_185}"
    testFoldConst("${const_sql_5_185}")
    qt_sql_5_186_non_strict "${const_sql_5_186}"
    testFoldConst("${const_sql_5_186}")
    qt_sql_5_187_non_strict "${const_sql_5_187}"
    testFoldConst("${const_sql_5_187}")
    qt_sql_5_188_non_strict "${const_sql_5_188}"
    testFoldConst("${const_sql_5_188}")
    qt_sql_5_189_non_strict "${const_sql_5_189}"
    testFoldConst("${const_sql_5_189}")
    qt_sql_5_190_non_strict "${const_sql_5_190}"
    testFoldConst("${const_sql_5_190}")
    qt_sql_5_191_non_strict "${const_sql_5_191}"
    testFoldConst("${const_sql_5_191}")
    qt_sql_5_192_non_strict "${const_sql_5_192}"
    testFoldConst("${const_sql_5_192}")
    qt_sql_5_193_non_strict "${const_sql_5_193}"
    testFoldConst("${const_sql_5_193}")
    qt_sql_5_194_non_strict "${const_sql_5_194}"
    testFoldConst("${const_sql_5_194}")
}