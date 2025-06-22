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


suite("test_cast_to_double_from_datetimev2_3_part6_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_6_0 = """select "0100-12-01 01:01:00.000", cast(cast("0100-12-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_0};"""
        exception ""
    }
    def const_sql_6_1 = """select "0100-12-01 01:01:00.000", cast(cast("0100-12-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_1};"""
        exception ""
    }
    def const_sql_6_2 = """select "0100-12-01 01:01:00.999", cast(cast("0100-12-01 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_2};"""
        exception ""
    }
    def const_sql_6_3 = """select "0100-12-01 01:01:01.000", cast(cast("0100-12-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_3};"""
        exception ""
    }
    def const_sql_6_4 = """select "0100-12-01 01:01:01.000", cast(cast("0100-12-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_4};"""
        exception ""
    }
    def const_sql_6_5 = """select "0100-12-01 01:01:01.999", cast(cast("0100-12-01 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_5};"""
        exception ""
    }
    def const_sql_6_6 = """select "0100-12-01 01:01:59.000", cast(cast("0100-12-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_6};"""
        exception ""
    }
    def const_sql_6_7 = """select "0100-12-01 01:01:59.000", cast(cast("0100-12-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_7};"""
        exception ""
    }
    def const_sql_6_8 = """select "0100-12-01 01:01:59.999", cast(cast("0100-12-01 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_8};"""
        exception ""
    }
    def const_sql_6_9 = """select "0100-12-01 01:59:00.000", cast(cast("0100-12-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_9};"""
        exception ""
    }
    def const_sql_6_10 = """select "0100-12-01 01:59:00.000", cast(cast("0100-12-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_10};"""
        exception ""
    }
    def const_sql_6_11 = """select "0100-12-01 01:59:00.999", cast(cast("0100-12-01 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_11};"""
        exception ""
    }
    def const_sql_6_12 = """select "0100-12-01 01:59:01.000", cast(cast("0100-12-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_12};"""
        exception ""
    }
    def const_sql_6_13 = """select "0100-12-01 01:59:01.000", cast(cast("0100-12-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_13};"""
        exception ""
    }
    def const_sql_6_14 = """select "0100-12-01 01:59:01.999", cast(cast("0100-12-01 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_14};"""
        exception ""
    }
    def const_sql_6_15 = """select "0100-12-01 01:59:59.000", cast(cast("0100-12-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_15};"""
        exception ""
    }
    def const_sql_6_16 = """select "0100-12-01 01:59:59.000", cast(cast("0100-12-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_16};"""
        exception ""
    }
    def const_sql_6_17 = """select "0100-12-01 01:59:59.999", cast(cast("0100-12-01 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_17};"""
        exception ""
    }
    def const_sql_6_18 = """select "0100-12-01 23:00:00.000", cast(cast("0100-12-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_18};"""
        exception ""
    }
    def const_sql_6_19 = """select "0100-12-01 23:00:00.000", cast(cast("0100-12-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_19};"""
        exception ""
    }
    def const_sql_6_20 = """select "0100-12-01 23:00:00.999", cast(cast("0100-12-01 23:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_20};"""
        exception ""
    }
    def const_sql_6_21 = """select "0100-12-01 23:00:01.000", cast(cast("0100-12-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_21};"""
        exception ""
    }
    def const_sql_6_22 = """select "0100-12-01 23:00:01.000", cast(cast("0100-12-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_22};"""
        exception ""
    }
    def const_sql_6_23 = """select "0100-12-01 23:00:01.999", cast(cast("0100-12-01 23:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_23};"""
        exception ""
    }
    def const_sql_6_24 = """select "0100-12-01 23:00:59.000", cast(cast("0100-12-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_24};"""
        exception ""
    }
    def const_sql_6_25 = """select "0100-12-01 23:00:59.000", cast(cast("0100-12-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_25};"""
        exception ""
    }
    def const_sql_6_26 = """select "0100-12-01 23:00:59.999", cast(cast("0100-12-01 23:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_26};"""
        exception ""
    }
    def const_sql_6_27 = """select "0100-12-01 23:01:00.000", cast(cast("0100-12-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_27};"""
        exception ""
    }
    def const_sql_6_28 = """select "0100-12-01 23:01:00.000", cast(cast("0100-12-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_28};"""
        exception ""
    }
    def const_sql_6_29 = """select "0100-12-01 23:01:00.999", cast(cast("0100-12-01 23:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_29};"""
        exception ""
    }
    def const_sql_6_30 = """select "0100-12-01 23:01:01.000", cast(cast("0100-12-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_30};"""
        exception ""
    }
    def const_sql_6_31 = """select "0100-12-01 23:01:01.000", cast(cast("0100-12-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_31};"""
        exception ""
    }
    def const_sql_6_32 = """select "0100-12-01 23:01:01.999", cast(cast("0100-12-01 23:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_32};"""
        exception ""
    }
    def const_sql_6_33 = """select "0100-12-01 23:01:59.000", cast(cast("0100-12-01 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_33};"""
        exception ""
    }
    def const_sql_6_34 = """select "0100-12-01 23:01:59.000", cast(cast("0100-12-01 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_34};"""
        exception ""
    }
    def const_sql_6_35 = """select "0100-12-01 23:01:59.999", cast(cast("0100-12-01 23:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_35};"""
        exception ""
    }
    def const_sql_6_36 = """select "0100-12-01 23:59:00.000", cast(cast("0100-12-01 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_36};"""
        exception ""
    }
    def const_sql_6_37 = """select "0100-12-01 23:59:00.000", cast(cast("0100-12-01 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_37};"""
        exception ""
    }
    def const_sql_6_38 = """select "0100-12-01 23:59:00.999", cast(cast("0100-12-01 23:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_38};"""
        exception ""
    }
    def const_sql_6_39 = """select "0100-12-01 23:59:01.000", cast(cast("0100-12-01 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_39};"""
        exception ""
    }
    def const_sql_6_40 = """select "0100-12-01 23:59:01.000", cast(cast("0100-12-01 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_40};"""
        exception ""
    }
    def const_sql_6_41 = """select "0100-12-01 23:59:01.999", cast(cast("0100-12-01 23:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_41};"""
        exception ""
    }
    def const_sql_6_42 = """select "0100-12-01 23:59:59.000", cast(cast("0100-12-01 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_42};"""
        exception ""
    }
    def const_sql_6_43 = """select "0100-12-01 23:59:59.000", cast(cast("0100-12-01 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_43};"""
        exception ""
    }
    def const_sql_6_44 = """select "0100-12-01 23:59:59.999", cast(cast("0100-12-01 23:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_44};"""
        exception ""
    }
    def const_sql_6_45 = """select "0100-12-28 00:00:00.000", cast(cast("0100-12-28 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_45};"""
        exception ""
    }
    def const_sql_6_46 = """select "0100-12-28 00:00:00.000", cast(cast("0100-12-28 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_46};"""
        exception ""
    }
    def const_sql_6_47 = """select "0100-12-28 00:00:00.999", cast(cast("0100-12-28 00:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_47};"""
        exception ""
    }
    def const_sql_6_48 = """select "0100-12-28 00:00:01.000", cast(cast("0100-12-28 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_48};"""
        exception ""
    }
    def const_sql_6_49 = """select "0100-12-28 00:00:01.000", cast(cast("0100-12-28 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_49};"""
        exception ""
    }
    def const_sql_6_50 = """select "0100-12-28 00:00:01.999", cast(cast("0100-12-28 00:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_50};"""
        exception ""
    }
    def const_sql_6_51 = """select "0100-12-28 00:00:59.000", cast(cast("0100-12-28 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_51};"""
        exception ""
    }
    def const_sql_6_52 = """select "0100-12-28 00:00:59.000", cast(cast("0100-12-28 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_52};"""
        exception ""
    }
    def const_sql_6_53 = """select "0100-12-28 00:00:59.999", cast(cast("0100-12-28 00:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_53};"""
        exception ""
    }
    def const_sql_6_54 = """select "0100-12-28 00:01:00.000", cast(cast("0100-12-28 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_54};"""
        exception ""
    }
    def const_sql_6_55 = """select "0100-12-28 00:01:00.000", cast(cast("0100-12-28 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_55};"""
        exception ""
    }
    def const_sql_6_56 = """select "0100-12-28 00:01:00.999", cast(cast("0100-12-28 00:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_56};"""
        exception ""
    }
    def const_sql_6_57 = """select "0100-12-28 00:01:01.000", cast(cast("0100-12-28 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_57};"""
        exception ""
    }
    def const_sql_6_58 = """select "0100-12-28 00:01:01.000", cast(cast("0100-12-28 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_58};"""
        exception ""
    }
    def const_sql_6_59 = """select "0100-12-28 00:01:01.999", cast(cast("0100-12-28 00:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_59};"""
        exception ""
    }
    def const_sql_6_60 = """select "0100-12-28 00:01:59.000", cast(cast("0100-12-28 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_60};"""
        exception ""
    }
    def const_sql_6_61 = """select "0100-12-28 00:01:59.000", cast(cast("0100-12-28 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_61};"""
        exception ""
    }
    def const_sql_6_62 = """select "0100-12-28 00:01:59.999", cast(cast("0100-12-28 00:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_62};"""
        exception ""
    }
    def const_sql_6_63 = """select "0100-12-28 00:59:00.000", cast(cast("0100-12-28 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_63};"""
        exception ""
    }
    def const_sql_6_64 = """select "0100-12-28 00:59:00.000", cast(cast("0100-12-28 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_64};"""
        exception ""
    }
    def const_sql_6_65 = """select "0100-12-28 00:59:00.999", cast(cast("0100-12-28 00:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_65};"""
        exception ""
    }
    def const_sql_6_66 = """select "0100-12-28 00:59:01.000", cast(cast("0100-12-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_66};"""
        exception ""
    }
    def const_sql_6_67 = """select "0100-12-28 00:59:01.000", cast(cast("0100-12-28 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_67};"""
        exception ""
    }
    def const_sql_6_68 = """select "0100-12-28 00:59:01.999", cast(cast("0100-12-28 00:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_68};"""
        exception ""
    }
    def const_sql_6_69 = """select "0100-12-28 00:59:59.000", cast(cast("0100-12-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_69};"""
        exception ""
    }
    def const_sql_6_70 = """select "0100-12-28 00:59:59.000", cast(cast("0100-12-28 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_70};"""
        exception ""
    }
    def const_sql_6_71 = """select "0100-12-28 00:59:59.999", cast(cast("0100-12-28 00:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_71};"""
        exception ""
    }
    def const_sql_6_72 = """select "0100-12-28 01:00:00.000", cast(cast("0100-12-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_72};"""
        exception ""
    }
    def const_sql_6_73 = """select "0100-12-28 01:00:00.000", cast(cast("0100-12-28 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_73};"""
        exception ""
    }
    def const_sql_6_74 = """select "0100-12-28 01:00:00.999", cast(cast("0100-12-28 01:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_74};"""
        exception ""
    }
    def const_sql_6_75 = """select "0100-12-28 01:00:01.000", cast(cast("0100-12-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_75};"""
        exception ""
    }
    def const_sql_6_76 = """select "0100-12-28 01:00:01.000", cast(cast("0100-12-28 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_76};"""
        exception ""
    }
    def const_sql_6_77 = """select "0100-12-28 01:00:01.999", cast(cast("0100-12-28 01:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_77};"""
        exception ""
    }
    def const_sql_6_78 = """select "0100-12-28 01:00:59.000", cast(cast("0100-12-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_78};"""
        exception ""
    }
    def const_sql_6_79 = """select "0100-12-28 01:00:59.000", cast(cast("0100-12-28 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_79};"""
        exception ""
    }
    def const_sql_6_80 = """select "0100-12-28 01:00:59.999", cast(cast("0100-12-28 01:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_80};"""
        exception ""
    }
    def const_sql_6_81 = """select "0100-12-28 01:01:00.000", cast(cast("0100-12-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_81};"""
        exception ""
    }
    def const_sql_6_82 = """select "0100-12-28 01:01:00.000", cast(cast("0100-12-28 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_82};"""
        exception ""
    }
    def const_sql_6_83 = """select "0100-12-28 01:01:00.999", cast(cast("0100-12-28 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_83};"""
        exception ""
    }
    def const_sql_6_84 = """select "0100-12-28 01:01:01.000", cast(cast("0100-12-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_84};"""
        exception ""
    }
    def const_sql_6_85 = """select "0100-12-28 01:01:01.000", cast(cast("0100-12-28 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_85};"""
        exception ""
    }
    def const_sql_6_86 = """select "0100-12-28 01:01:01.999", cast(cast("0100-12-28 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_86};"""
        exception ""
    }
    def const_sql_6_87 = """select "0100-12-28 01:01:59.000", cast(cast("0100-12-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_87};"""
        exception ""
    }
    def const_sql_6_88 = """select "0100-12-28 01:01:59.000", cast(cast("0100-12-28 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_88};"""
        exception ""
    }
    def const_sql_6_89 = """select "0100-12-28 01:01:59.999", cast(cast("0100-12-28 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_89};"""
        exception ""
    }
    def const_sql_6_90 = """select "0100-12-28 01:59:00.000", cast(cast("0100-12-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_90};"""
        exception ""
    }
    def const_sql_6_91 = """select "0100-12-28 01:59:00.000", cast(cast("0100-12-28 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_91};"""
        exception ""
    }
    def const_sql_6_92 = """select "0100-12-28 01:59:00.999", cast(cast("0100-12-28 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_92};"""
        exception ""
    }
    def const_sql_6_93 = """select "0100-12-28 01:59:01.000", cast(cast("0100-12-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_93};"""
        exception ""
    }
    def const_sql_6_94 = """select "0100-12-28 01:59:01.000", cast(cast("0100-12-28 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_94};"""
        exception ""
    }
    def const_sql_6_95 = """select "0100-12-28 01:59:01.999", cast(cast("0100-12-28 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_95};"""
        exception ""
    }
    def const_sql_6_96 = """select "0100-12-28 01:59:59.000", cast(cast("0100-12-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_96};"""
        exception ""
    }
    def const_sql_6_97 = """select "0100-12-28 01:59:59.000", cast(cast("0100-12-28 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_97};"""
        exception ""
    }
    def const_sql_6_98 = """select "0100-12-28 01:59:59.999", cast(cast("0100-12-28 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_98};"""
        exception ""
    }
    def const_sql_6_99 = """select "0100-12-28 23:00:00.000", cast(cast("0100-12-28 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_99};"""
        exception ""
    }
    def const_sql_6_100 = """select "0100-12-28 23:00:00.000", cast(cast("0100-12-28 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_100};"""
        exception ""
    }
    def const_sql_6_101 = """select "0100-12-28 23:00:00.999", cast(cast("0100-12-28 23:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_101};"""
        exception ""
    }
    def const_sql_6_102 = """select "0100-12-28 23:00:01.000", cast(cast("0100-12-28 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_102};"""
        exception ""
    }
    def const_sql_6_103 = """select "0100-12-28 23:00:01.000", cast(cast("0100-12-28 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_103};"""
        exception ""
    }
    def const_sql_6_104 = """select "0100-12-28 23:00:01.999", cast(cast("0100-12-28 23:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_104};"""
        exception ""
    }
    def const_sql_6_105 = """select "0100-12-28 23:00:59.000", cast(cast("0100-12-28 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_105};"""
        exception ""
    }
    def const_sql_6_106 = """select "0100-12-28 23:00:59.000", cast(cast("0100-12-28 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_106};"""
        exception ""
    }
    def const_sql_6_107 = """select "0100-12-28 23:00:59.999", cast(cast("0100-12-28 23:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_107};"""
        exception ""
    }
    def const_sql_6_108 = """select "0100-12-28 23:01:00.000", cast(cast("0100-12-28 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_108};"""
        exception ""
    }
    def const_sql_6_109 = """select "0100-12-28 23:01:00.000", cast(cast("0100-12-28 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_109};"""
        exception ""
    }
    def const_sql_6_110 = """select "0100-12-28 23:01:00.999", cast(cast("0100-12-28 23:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_110};"""
        exception ""
    }
    def const_sql_6_111 = """select "0100-12-28 23:01:01.000", cast(cast("0100-12-28 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_111};"""
        exception ""
    }
    def const_sql_6_112 = """select "0100-12-28 23:01:01.000", cast(cast("0100-12-28 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_112};"""
        exception ""
    }
    def const_sql_6_113 = """select "0100-12-28 23:01:01.999", cast(cast("0100-12-28 23:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_113};"""
        exception ""
    }
    def const_sql_6_114 = """select "0100-12-28 23:01:59.000", cast(cast("0100-12-28 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_114};"""
        exception ""
    }
    def const_sql_6_115 = """select "0100-12-28 23:01:59.000", cast(cast("0100-12-28 23:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_115};"""
        exception ""
    }
    def const_sql_6_116 = """select "0100-12-28 23:01:59.999", cast(cast("0100-12-28 23:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_116};"""
        exception ""
    }
    def const_sql_6_117 = """select "0100-12-28 23:59:00.000", cast(cast("0100-12-28 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_117};"""
        exception ""
    }
    def const_sql_6_118 = """select "0100-12-28 23:59:00.000", cast(cast("0100-12-28 23:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_118};"""
        exception ""
    }
    def const_sql_6_119 = """select "0100-12-28 23:59:00.999", cast(cast("0100-12-28 23:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_119};"""
        exception ""
    }
    def const_sql_6_120 = """select "0100-12-28 23:59:01.000", cast(cast("0100-12-28 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_120};"""
        exception ""
    }
    def const_sql_6_121 = """select "0100-12-28 23:59:01.000", cast(cast("0100-12-28 23:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_121};"""
        exception ""
    }
    def const_sql_6_122 = """select "0100-12-28 23:59:01.999", cast(cast("0100-12-28 23:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_122};"""
        exception ""
    }
    def const_sql_6_123 = """select "0100-12-28 23:59:59.000", cast(cast("0100-12-28 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_123};"""
        exception ""
    }
    def const_sql_6_124 = """select "0100-12-28 23:59:59.000", cast(cast("0100-12-28 23:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_124};"""
        exception ""
    }
    def const_sql_6_125 = """select "0100-12-28 23:59:59.999", cast(cast("0100-12-28 23:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_125};"""
        exception ""
    }
    def const_sql_6_126 = """select "2025-01-01 00:00:00.000", cast(cast("2025-01-01 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_126};"""
        exception ""
    }
    def const_sql_6_127 = """select "2025-01-01 00:00:00.000", cast(cast("2025-01-01 00:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_127};"""
        exception ""
    }
    def const_sql_6_128 = """select "2025-01-01 00:00:00.999", cast(cast("2025-01-01 00:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_128};"""
        exception ""
    }
    def const_sql_6_129 = """select "2025-01-01 00:00:01.000", cast(cast("2025-01-01 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_129};"""
        exception ""
    }
    def const_sql_6_130 = """select "2025-01-01 00:00:01.000", cast(cast("2025-01-01 00:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_130};"""
        exception ""
    }
    def const_sql_6_131 = """select "2025-01-01 00:00:01.999", cast(cast("2025-01-01 00:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_131};"""
        exception ""
    }
    def const_sql_6_132 = """select "2025-01-01 00:00:59.000", cast(cast("2025-01-01 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_132};"""
        exception ""
    }
    def const_sql_6_133 = """select "2025-01-01 00:00:59.000", cast(cast("2025-01-01 00:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_133};"""
        exception ""
    }
    def const_sql_6_134 = """select "2025-01-01 00:00:59.999", cast(cast("2025-01-01 00:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_134};"""
        exception ""
    }
    def const_sql_6_135 = """select "2025-01-01 00:01:00.000", cast(cast("2025-01-01 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_135};"""
        exception ""
    }
    def const_sql_6_136 = """select "2025-01-01 00:01:00.000", cast(cast("2025-01-01 00:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_136};"""
        exception ""
    }
    def const_sql_6_137 = """select "2025-01-01 00:01:00.999", cast(cast("2025-01-01 00:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_137};"""
        exception ""
    }
    def const_sql_6_138 = """select "2025-01-01 00:01:01.000", cast(cast("2025-01-01 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_138};"""
        exception ""
    }
    def const_sql_6_139 = """select "2025-01-01 00:01:01.000", cast(cast("2025-01-01 00:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_139};"""
        exception ""
    }
    def const_sql_6_140 = """select "2025-01-01 00:01:01.999", cast(cast("2025-01-01 00:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_140};"""
        exception ""
    }
    def const_sql_6_141 = """select "2025-01-01 00:01:59.000", cast(cast("2025-01-01 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_141};"""
        exception ""
    }
    def const_sql_6_142 = """select "2025-01-01 00:01:59.000", cast(cast("2025-01-01 00:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_142};"""
        exception ""
    }
    def const_sql_6_143 = """select "2025-01-01 00:01:59.999", cast(cast("2025-01-01 00:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_143};"""
        exception ""
    }
    def const_sql_6_144 = """select "2025-01-01 00:59:00.000", cast(cast("2025-01-01 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_144};"""
        exception ""
    }
    def const_sql_6_145 = """select "2025-01-01 00:59:00.000", cast(cast("2025-01-01 00:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_145};"""
        exception ""
    }
    def const_sql_6_146 = """select "2025-01-01 00:59:00.999", cast(cast("2025-01-01 00:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_146};"""
        exception ""
    }
    def const_sql_6_147 = """select "2025-01-01 00:59:01.000", cast(cast("2025-01-01 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_147};"""
        exception ""
    }
    def const_sql_6_148 = """select "2025-01-01 00:59:01.000", cast(cast("2025-01-01 00:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_148};"""
        exception ""
    }
    def const_sql_6_149 = """select "2025-01-01 00:59:01.999", cast(cast("2025-01-01 00:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_149};"""
        exception ""
    }
    def const_sql_6_150 = """select "2025-01-01 00:59:59.000", cast(cast("2025-01-01 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_150};"""
        exception ""
    }
    def const_sql_6_151 = """select "2025-01-01 00:59:59.000", cast(cast("2025-01-01 00:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_151};"""
        exception ""
    }
    def const_sql_6_152 = """select "2025-01-01 00:59:59.999", cast(cast("2025-01-01 00:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_152};"""
        exception ""
    }
    def const_sql_6_153 = """select "2025-01-01 01:00:00.000", cast(cast("2025-01-01 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_153};"""
        exception ""
    }
    def const_sql_6_154 = """select "2025-01-01 01:00:00.000", cast(cast("2025-01-01 01:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_154};"""
        exception ""
    }
    def const_sql_6_155 = """select "2025-01-01 01:00:00.999", cast(cast("2025-01-01 01:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_155};"""
        exception ""
    }
    def const_sql_6_156 = """select "2025-01-01 01:00:01.000", cast(cast("2025-01-01 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_156};"""
        exception ""
    }
    def const_sql_6_157 = """select "2025-01-01 01:00:01.000", cast(cast("2025-01-01 01:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_157};"""
        exception ""
    }
    def const_sql_6_158 = """select "2025-01-01 01:00:01.999", cast(cast("2025-01-01 01:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_158};"""
        exception ""
    }
    def const_sql_6_159 = """select "2025-01-01 01:00:59.000", cast(cast("2025-01-01 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_159};"""
        exception ""
    }
    def const_sql_6_160 = """select "2025-01-01 01:00:59.000", cast(cast("2025-01-01 01:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_160};"""
        exception ""
    }
    def const_sql_6_161 = """select "2025-01-01 01:00:59.999", cast(cast("2025-01-01 01:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_161};"""
        exception ""
    }
    def const_sql_6_162 = """select "2025-01-01 01:01:00.000", cast(cast("2025-01-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_162};"""
        exception ""
    }
    def const_sql_6_163 = """select "2025-01-01 01:01:00.000", cast(cast("2025-01-01 01:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_163};"""
        exception ""
    }
    def const_sql_6_164 = """select "2025-01-01 01:01:00.999", cast(cast("2025-01-01 01:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_164};"""
        exception ""
    }
    def const_sql_6_165 = """select "2025-01-01 01:01:01.000", cast(cast("2025-01-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_165};"""
        exception ""
    }
    def const_sql_6_166 = """select "2025-01-01 01:01:01.000", cast(cast("2025-01-01 01:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_166};"""
        exception ""
    }
    def const_sql_6_167 = """select "2025-01-01 01:01:01.999", cast(cast("2025-01-01 01:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_167};"""
        exception ""
    }
    def const_sql_6_168 = """select "2025-01-01 01:01:59.000", cast(cast("2025-01-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_168};"""
        exception ""
    }
    def const_sql_6_169 = """select "2025-01-01 01:01:59.000", cast(cast("2025-01-01 01:01:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_169};"""
        exception ""
    }
    def const_sql_6_170 = """select "2025-01-01 01:01:59.999", cast(cast("2025-01-01 01:01:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_170};"""
        exception ""
    }
    def const_sql_6_171 = """select "2025-01-01 01:59:00.000", cast(cast("2025-01-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_171};"""
        exception ""
    }
    def const_sql_6_172 = """select "2025-01-01 01:59:00.000", cast(cast("2025-01-01 01:59:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_172};"""
        exception ""
    }
    def const_sql_6_173 = """select "2025-01-01 01:59:00.999", cast(cast("2025-01-01 01:59:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_173};"""
        exception ""
    }
    def const_sql_6_174 = """select "2025-01-01 01:59:01.000", cast(cast("2025-01-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_174};"""
        exception ""
    }
    def const_sql_6_175 = """select "2025-01-01 01:59:01.000", cast(cast("2025-01-01 01:59:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_175};"""
        exception ""
    }
    def const_sql_6_176 = """select "2025-01-01 01:59:01.999", cast(cast("2025-01-01 01:59:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_176};"""
        exception ""
    }
    def const_sql_6_177 = """select "2025-01-01 01:59:59.000", cast(cast("2025-01-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_177};"""
        exception ""
    }
    def const_sql_6_178 = """select "2025-01-01 01:59:59.000", cast(cast("2025-01-01 01:59:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_178};"""
        exception ""
    }
    def const_sql_6_179 = """select "2025-01-01 01:59:59.999", cast(cast("2025-01-01 01:59:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_179};"""
        exception ""
    }
    def const_sql_6_180 = """select "2025-01-01 23:00:00.000", cast(cast("2025-01-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_180};"""
        exception ""
    }
    def const_sql_6_181 = """select "2025-01-01 23:00:00.000", cast(cast("2025-01-01 23:00:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_181};"""
        exception ""
    }
    def const_sql_6_182 = """select "2025-01-01 23:00:00.999", cast(cast("2025-01-01 23:00:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_182};"""
        exception ""
    }
    def const_sql_6_183 = """select "2025-01-01 23:00:01.000", cast(cast("2025-01-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_183};"""
        exception ""
    }
    def const_sql_6_184 = """select "2025-01-01 23:00:01.000", cast(cast("2025-01-01 23:00:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_184};"""
        exception ""
    }
    def const_sql_6_185 = """select "2025-01-01 23:00:01.999", cast(cast("2025-01-01 23:00:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_185};"""
        exception ""
    }
    def const_sql_6_186 = """select "2025-01-01 23:00:59.000", cast(cast("2025-01-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_186};"""
        exception ""
    }
    def const_sql_6_187 = """select "2025-01-01 23:00:59.000", cast(cast("2025-01-01 23:00:59.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_187};"""
        exception ""
    }
    def const_sql_6_188 = """select "2025-01-01 23:00:59.999", cast(cast("2025-01-01 23:00:59.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_188};"""
        exception ""
    }
    def const_sql_6_189 = """select "2025-01-01 23:01:00.000", cast(cast("2025-01-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_189};"""
        exception ""
    }
    def const_sql_6_190 = """select "2025-01-01 23:01:00.000", cast(cast("2025-01-01 23:01:00.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_190};"""
        exception ""
    }
    def const_sql_6_191 = """select "2025-01-01 23:01:00.999", cast(cast("2025-01-01 23:01:00.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_191};"""
        exception ""
    }
    def const_sql_6_192 = """select "2025-01-01 23:01:01.000", cast(cast("2025-01-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_192};"""
        exception ""
    }
    def const_sql_6_193 = """select "2025-01-01 23:01:01.000", cast(cast("2025-01-01 23:01:01.000" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_193};"""
        exception ""
    }
    def const_sql_6_194 = """select "2025-01-01 23:01:01.999", cast(cast("2025-01-01 23:01:01.999" as datetimev2(3)) as double);"""

    test {
        sql """${const_sql_6_194};"""
        exception ""
    }

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
    qt_sql_6_128_non_strict "${const_sql_6_128}"
    testFoldConst("${const_sql_6_128}")
    qt_sql_6_129_non_strict "${const_sql_6_129}"
    testFoldConst("${const_sql_6_129}")
    qt_sql_6_130_non_strict "${const_sql_6_130}"
    testFoldConst("${const_sql_6_130}")
    qt_sql_6_131_non_strict "${const_sql_6_131}"
    testFoldConst("${const_sql_6_131}")
    qt_sql_6_132_non_strict "${const_sql_6_132}"
    testFoldConst("${const_sql_6_132}")
    qt_sql_6_133_non_strict "${const_sql_6_133}"
    testFoldConst("${const_sql_6_133}")
    qt_sql_6_134_non_strict "${const_sql_6_134}"
    testFoldConst("${const_sql_6_134}")
    qt_sql_6_135_non_strict "${const_sql_6_135}"
    testFoldConst("${const_sql_6_135}")
    qt_sql_6_136_non_strict "${const_sql_6_136}"
    testFoldConst("${const_sql_6_136}")
    qt_sql_6_137_non_strict "${const_sql_6_137}"
    testFoldConst("${const_sql_6_137}")
    qt_sql_6_138_non_strict "${const_sql_6_138}"
    testFoldConst("${const_sql_6_138}")
    qt_sql_6_139_non_strict "${const_sql_6_139}"
    testFoldConst("${const_sql_6_139}")
    qt_sql_6_140_non_strict "${const_sql_6_140}"
    testFoldConst("${const_sql_6_140}")
    qt_sql_6_141_non_strict "${const_sql_6_141}"
    testFoldConst("${const_sql_6_141}")
    qt_sql_6_142_non_strict "${const_sql_6_142}"
    testFoldConst("${const_sql_6_142}")
    qt_sql_6_143_non_strict "${const_sql_6_143}"
    testFoldConst("${const_sql_6_143}")
    qt_sql_6_144_non_strict "${const_sql_6_144}"
    testFoldConst("${const_sql_6_144}")
    qt_sql_6_145_non_strict "${const_sql_6_145}"
    testFoldConst("${const_sql_6_145}")
    qt_sql_6_146_non_strict "${const_sql_6_146}"
    testFoldConst("${const_sql_6_146}")
    qt_sql_6_147_non_strict "${const_sql_6_147}"
    testFoldConst("${const_sql_6_147}")
    qt_sql_6_148_non_strict "${const_sql_6_148}"
    testFoldConst("${const_sql_6_148}")
    qt_sql_6_149_non_strict "${const_sql_6_149}"
    testFoldConst("${const_sql_6_149}")
    qt_sql_6_150_non_strict "${const_sql_6_150}"
    testFoldConst("${const_sql_6_150}")
    qt_sql_6_151_non_strict "${const_sql_6_151}"
    testFoldConst("${const_sql_6_151}")
    qt_sql_6_152_non_strict "${const_sql_6_152}"
    testFoldConst("${const_sql_6_152}")
    qt_sql_6_153_non_strict "${const_sql_6_153}"
    testFoldConst("${const_sql_6_153}")
    qt_sql_6_154_non_strict "${const_sql_6_154}"
    testFoldConst("${const_sql_6_154}")
    qt_sql_6_155_non_strict "${const_sql_6_155}"
    testFoldConst("${const_sql_6_155}")
    qt_sql_6_156_non_strict "${const_sql_6_156}"
    testFoldConst("${const_sql_6_156}")
    qt_sql_6_157_non_strict "${const_sql_6_157}"
    testFoldConst("${const_sql_6_157}")
    qt_sql_6_158_non_strict "${const_sql_6_158}"
    testFoldConst("${const_sql_6_158}")
    qt_sql_6_159_non_strict "${const_sql_6_159}"
    testFoldConst("${const_sql_6_159}")
    qt_sql_6_160_non_strict "${const_sql_6_160}"
    testFoldConst("${const_sql_6_160}")
    qt_sql_6_161_non_strict "${const_sql_6_161}"
    testFoldConst("${const_sql_6_161}")
    qt_sql_6_162_non_strict "${const_sql_6_162}"
    testFoldConst("${const_sql_6_162}")
    qt_sql_6_163_non_strict "${const_sql_6_163}"
    testFoldConst("${const_sql_6_163}")
    qt_sql_6_164_non_strict "${const_sql_6_164}"
    testFoldConst("${const_sql_6_164}")
    qt_sql_6_165_non_strict "${const_sql_6_165}"
    testFoldConst("${const_sql_6_165}")
    qt_sql_6_166_non_strict "${const_sql_6_166}"
    testFoldConst("${const_sql_6_166}")
    qt_sql_6_167_non_strict "${const_sql_6_167}"
    testFoldConst("${const_sql_6_167}")
    qt_sql_6_168_non_strict "${const_sql_6_168}"
    testFoldConst("${const_sql_6_168}")
    qt_sql_6_169_non_strict "${const_sql_6_169}"
    testFoldConst("${const_sql_6_169}")
    qt_sql_6_170_non_strict "${const_sql_6_170}"
    testFoldConst("${const_sql_6_170}")
    qt_sql_6_171_non_strict "${const_sql_6_171}"
    testFoldConst("${const_sql_6_171}")
    qt_sql_6_172_non_strict "${const_sql_6_172}"
    testFoldConst("${const_sql_6_172}")
    qt_sql_6_173_non_strict "${const_sql_6_173}"
    testFoldConst("${const_sql_6_173}")
    qt_sql_6_174_non_strict "${const_sql_6_174}"
    testFoldConst("${const_sql_6_174}")
    qt_sql_6_175_non_strict "${const_sql_6_175}"
    testFoldConst("${const_sql_6_175}")
    qt_sql_6_176_non_strict "${const_sql_6_176}"
    testFoldConst("${const_sql_6_176}")
    qt_sql_6_177_non_strict "${const_sql_6_177}"
    testFoldConst("${const_sql_6_177}")
    qt_sql_6_178_non_strict "${const_sql_6_178}"
    testFoldConst("${const_sql_6_178}")
    qt_sql_6_179_non_strict "${const_sql_6_179}"
    testFoldConst("${const_sql_6_179}")
    qt_sql_6_180_non_strict "${const_sql_6_180}"
    testFoldConst("${const_sql_6_180}")
    qt_sql_6_181_non_strict "${const_sql_6_181}"
    testFoldConst("${const_sql_6_181}")
    qt_sql_6_182_non_strict "${const_sql_6_182}"
    testFoldConst("${const_sql_6_182}")
    qt_sql_6_183_non_strict "${const_sql_6_183}"
    testFoldConst("${const_sql_6_183}")
    qt_sql_6_184_non_strict "${const_sql_6_184}"
    testFoldConst("${const_sql_6_184}")
    qt_sql_6_185_non_strict "${const_sql_6_185}"
    testFoldConst("${const_sql_6_185}")
    qt_sql_6_186_non_strict "${const_sql_6_186}"
    testFoldConst("${const_sql_6_186}")
    qt_sql_6_187_non_strict "${const_sql_6_187}"
    testFoldConst("${const_sql_6_187}")
    qt_sql_6_188_non_strict "${const_sql_6_188}"
    testFoldConst("${const_sql_6_188}")
    qt_sql_6_189_non_strict "${const_sql_6_189}"
    testFoldConst("${const_sql_6_189}")
    qt_sql_6_190_non_strict "${const_sql_6_190}"
    testFoldConst("${const_sql_6_190}")
    qt_sql_6_191_non_strict "${const_sql_6_191}"
    testFoldConst("${const_sql_6_191}")
    qt_sql_6_192_non_strict "${const_sql_6_192}"
    testFoldConst("${const_sql_6_192}")
    qt_sql_6_193_non_strict "${const_sql_6_193}"
    testFoldConst("${const_sql_6_193}")
    qt_sql_6_194_non_strict "${const_sql_6_194}"
    testFoldConst("${const_sql_6_194}")
}