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


suite("test_cast_to_float_from_datetimev2_1_part1_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_1_0 = """select "0000-12-01 01:00:59.0", cast(cast("0000-12-01 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_0};"""
        exception ""
    }
    def const_sql_1_1 = """select "0000-12-01 01:00:59.0", cast(cast("0000-12-01 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_1};"""
        exception ""
    }
    def const_sql_1_2 = """select "0000-12-01 01:00:59.9", cast(cast("0000-12-01 01:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_2};"""
        exception ""
    }
    def const_sql_1_3 = """select "0000-12-01 01:01:00.0", cast(cast("0000-12-01 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_3};"""
        exception ""
    }
    def const_sql_1_4 = """select "0000-12-01 01:01:00.0", cast(cast("0000-12-01 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_4};"""
        exception ""
    }
    def const_sql_1_5 = """select "0000-12-01 01:01:00.9", cast(cast("0000-12-01 01:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_5};"""
        exception ""
    }
    def const_sql_1_6 = """select "0000-12-01 01:01:01.0", cast(cast("0000-12-01 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_6};"""
        exception ""
    }
    def const_sql_1_7 = """select "0000-12-01 01:01:01.0", cast(cast("0000-12-01 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_7};"""
        exception ""
    }
    def const_sql_1_8 = """select "0000-12-01 01:01:01.9", cast(cast("0000-12-01 01:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_8};"""
        exception ""
    }
    def const_sql_1_9 = """select "0000-12-01 01:01:59.0", cast(cast("0000-12-01 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_9};"""
        exception ""
    }
    def const_sql_1_10 = """select "0000-12-01 01:01:59.0", cast(cast("0000-12-01 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_10};"""
        exception ""
    }
    def const_sql_1_11 = """select "0000-12-01 01:01:59.9", cast(cast("0000-12-01 01:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_11};"""
        exception ""
    }
    def const_sql_1_12 = """select "0000-12-01 01:59:00.0", cast(cast("0000-12-01 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_12};"""
        exception ""
    }
    def const_sql_1_13 = """select "0000-12-01 01:59:00.0", cast(cast("0000-12-01 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_13};"""
        exception ""
    }
    def const_sql_1_14 = """select "0000-12-01 01:59:00.9", cast(cast("0000-12-01 01:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_14};"""
        exception ""
    }
    def const_sql_1_15 = """select "0000-12-01 01:59:01.0", cast(cast("0000-12-01 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_15};"""
        exception ""
    }
    def const_sql_1_16 = """select "0000-12-01 01:59:01.0", cast(cast("0000-12-01 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_16};"""
        exception ""
    }
    def const_sql_1_17 = """select "0000-12-01 01:59:01.9", cast(cast("0000-12-01 01:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_17};"""
        exception ""
    }
    def const_sql_1_18 = """select "0000-12-01 01:59:59.0", cast(cast("0000-12-01 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_18};"""
        exception ""
    }
    def const_sql_1_19 = """select "0000-12-01 01:59:59.0", cast(cast("0000-12-01 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_19};"""
        exception ""
    }
    def const_sql_1_20 = """select "0000-12-01 01:59:59.9", cast(cast("0000-12-01 01:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_20};"""
        exception ""
    }
    def const_sql_1_21 = """select "0000-12-01 23:00:00.0", cast(cast("0000-12-01 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_21};"""
        exception ""
    }
    def const_sql_1_22 = """select "0000-12-01 23:00:00.0", cast(cast("0000-12-01 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_22};"""
        exception ""
    }
    def const_sql_1_23 = """select "0000-12-01 23:00:00.9", cast(cast("0000-12-01 23:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_23};"""
        exception ""
    }
    def const_sql_1_24 = """select "0000-12-01 23:00:01.0", cast(cast("0000-12-01 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_24};"""
        exception ""
    }
    def const_sql_1_25 = """select "0000-12-01 23:00:01.0", cast(cast("0000-12-01 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_25};"""
        exception ""
    }
    def const_sql_1_26 = """select "0000-12-01 23:00:01.9", cast(cast("0000-12-01 23:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_26};"""
        exception ""
    }
    def const_sql_1_27 = """select "0000-12-01 23:00:59.0", cast(cast("0000-12-01 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_27};"""
        exception ""
    }
    def const_sql_1_28 = """select "0000-12-01 23:00:59.0", cast(cast("0000-12-01 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_28};"""
        exception ""
    }
    def const_sql_1_29 = """select "0000-12-01 23:00:59.9", cast(cast("0000-12-01 23:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_29};"""
        exception ""
    }
    def const_sql_1_30 = """select "0000-12-01 23:01:00.0", cast(cast("0000-12-01 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_30};"""
        exception ""
    }
    def const_sql_1_31 = """select "0000-12-01 23:01:00.0", cast(cast("0000-12-01 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_31};"""
        exception ""
    }
    def const_sql_1_32 = """select "0000-12-01 23:01:00.9", cast(cast("0000-12-01 23:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_32};"""
        exception ""
    }
    def const_sql_1_33 = """select "0000-12-01 23:01:01.0", cast(cast("0000-12-01 23:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_33};"""
        exception ""
    }
    def const_sql_1_34 = """select "0000-12-01 23:01:01.0", cast(cast("0000-12-01 23:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_34};"""
        exception ""
    }
    def const_sql_1_35 = """select "0000-12-01 23:01:01.9", cast(cast("0000-12-01 23:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_35};"""
        exception ""
    }
    def const_sql_1_36 = """select "0000-12-01 23:01:59.0", cast(cast("0000-12-01 23:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_36};"""
        exception ""
    }
    def const_sql_1_37 = """select "0000-12-01 23:01:59.0", cast(cast("0000-12-01 23:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_37};"""
        exception ""
    }
    def const_sql_1_38 = """select "0000-12-01 23:01:59.9", cast(cast("0000-12-01 23:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_38};"""
        exception ""
    }
    def const_sql_1_39 = """select "0000-12-01 23:59:00.0", cast(cast("0000-12-01 23:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_39};"""
        exception ""
    }
    def const_sql_1_40 = """select "0000-12-01 23:59:00.0", cast(cast("0000-12-01 23:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_40};"""
        exception ""
    }
    def const_sql_1_41 = """select "0000-12-01 23:59:00.9", cast(cast("0000-12-01 23:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_41};"""
        exception ""
    }
    def const_sql_1_42 = """select "0000-12-01 23:59:01.0", cast(cast("0000-12-01 23:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_42};"""
        exception ""
    }
    def const_sql_1_43 = """select "0000-12-01 23:59:01.0", cast(cast("0000-12-01 23:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_43};"""
        exception ""
    }
    def const_sql_1_44 = """select "0000-12-01 23:59:01.9", cast(cast("0000-12-01 23:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_44};"""
        exception ""
    }
    def const_sql_1_45 = """select "0000-12-01 23:59:59.0", cast(cast("0000-12-01 23:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_45};"""
        exception ""
    }
    def const_sql_1_46 = """select "0000-12-01 23:59:59.0", cast(cast("0000-12-01 23:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_46};"""
        exception ""
    }
    def const_sql_1_47 = """select "0000-12-01 23:59:59.9", cast(cast("0000-12-01 23:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_47};"""
        exception ""
    }
    def const_sql_1_48 = """select "0000-12-28 00:00:00.0", cast(cast("0000-12-28 00:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_48};"""
        exception ""
    }
    def const_sql_1_49 = """select "0000-12-28 00:00:00.0", cast(cast("0000-12-28 00:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_49};"""
        exception ""
    }
    def const_sql_1_50 = """select "0000-12-28 00:00:00.9", cast(cast("0000-12-28 00:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_50};"""
        exception ""
    }
    def const_sql_1_51 = """select "0000-12-28 00:00:01.0", cast(cast("0000-12-28 00:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_51};"""
        exception ""
    }
    def const_sql_1_52 = """select "0000-12-28 00:00:01.0", cast(cast("0000-12-28 00:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_52};"""
        exception ""
    }
    def const_sql_1_53 = """select "0000-12-28 00:00:01.9", cast(cast("0000-12-28 00:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_53};"""
        exception ""
    }
    def const_sql_1_54 = """select "0000-12-28 00:00:59.0", cast(cast("0000-12-28 00:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_54};"""
        exception ""
    }
    def const_sql_1_55 = """select "0000-12-28 00:00:59.0", cast(cast("0000-12-28 00:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_55};"""
        exception ""
    }
    def const_sql_1_56 = """select "0000-12-28 00:00:59.9", cast(cast("0000-12-28 00:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_56};"""
        exception ""
    }
    def const_sql_1_57 = """select "0000-12-28 00:01:00.0", cast(cast("0000-12-28 00:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_57};"""
        exception ""
    }
    def const_sql_1_58 = """select "0000-12-28 00:01:00.0", cast(cast("0000-12-28 00:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_58};"""
        exception ""
    }
    def const_sql_1_59 = """select "0000-12-28 00:01:00.9", cast(cast("0000-12-28 00:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_59};"""
        exception ""
    }
    def const_sql_1_60 = """select "0000-12-28 00:01:01.0", cast(cast("0000-12-28 00:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_60};"""
        exception ""
    }
    def const_sql_1_61 = """select "0000-12-28 00:01:01.0", cast(cast("0000-12-28 00:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_61};"""
        exception ""
    }
    def const_sql_1_62 = """select "0000-12-28 00:01:01.9", cast(cast("0000-12-28 00:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_62};"""
        exception ""
    }
    def const_sql_1_63 = """select "0000-12-28 00:01:59.0", cast(cast("0000-12-28 00:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_63};"""
        exception ""
    }
    def const_sql_1_64 = """select "0000-12-28 00:01:59.0", cast(cast("0000-12-28 00:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_64};"""
        exception ""
    }
    def const_sql_1_65 = """select "0000-12-28 00:01:59.9", cast(cast("0000-12-28 00:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_65};"""
        exception ""
    }
    def const_sql_1_66 = """select "0000-12-28 00:59:00.0", cast(cast("0000-12-28 00:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_66};"""
        exception ""
    }
    def const_sql_1_67 = """select "0000-12-28 00:59:00.0", cast(cast("0000-12-28 00:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_67};"""
        exception ""
    }
    def const_sql_1_68 = """select "0000-12-28 00:59:00.9", cast(cast("0000-12-28 00:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_68};"""
        exception ""
    }
    def const_sql_1_69 = """select "0000-12-28 00:59:01.0", cast(cast("0000-12-28 00:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_69};"""
        exception ""
    }
    def const_sql_1_70 = """select "0000-12-28 00:59:01.0", cast(cast("0000-12-28 00:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_70};"""
        exception ""
    }
    def const_sql_1_71 = """select "0000-12-28 00:59:01.9", cast(cast("0000-12-28 00:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_71};"""
        exception ""
    }
    def const_sql_1_72 = """select "0000-12-28 00:59:59.0", cast(cast("0000-12-28 00:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_72};"""
        exception ""
    }
    def const_sql_1_73 = """select "0000-12-28 00:59:59.0", cast(cast("0000-12-28 00:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_73};"""
        exception ""
    }
    def const_sql_1_74 = """select "0000-12-28 00:59:59.9", cast(cast("0000-12-28 00:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_74};"""
        exception ""
    }
    def const_sql_1_75 = """select "0000-12-28 01:00:00.0", cast(cast("0000-12-28 01:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_75};"""
        exception ""
    }
    def const_sql_1_76 = """select "0000-12-28 01:00:00.0", cast(cast("0000-12-28 01:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_76};"""
        exception ""
    }
    def const_sql_1_77 = """select "0000-12-28 01:00:00.9", cast(cast("0000-12-28 01:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_77};"""
        exception ""
    }
    def const_sql_1_78 = """select "0000-12-28 01:00:01.0", cast(cast("0000-12-28 01:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_78};"""
        exception ""
    }
    def const_sql_1_79 = """select "0000-12-28 01:00:01.0", cast(cast("0000-12-28 01:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_79};"""
        exception ""
    }
    def const_sql_1_80 = """select "0000-12-28 01:00:01.9", cast(cast("0000-12-28 01:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_80};"""
        exception ""
    }
    def const_sql_1_81 = """select "0000-12-28 01:00:59.0", cast(cast("0000-12-28 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_81};"""
        exception ""
    }
    def const_sql_1_82 = """select "0000-12-28 01:00:59.0", cast(cast("0000-12-28 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_82};"""
        exception ""
    }
    def const_sql_1_83 = """select "0000-12-28 01:00:59.9", cast(cast("0000-12-28 01:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_83};"""
        exception ""
    }
    def const_sql_1_84 = """select "0000-12-28 01:01:00.0", cast(cast("0000-12-28 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_84};"""
        exception ""
    }
    def const_sql_1_85 = """select "0000-12-28 01:01:00.0", cast(cast("0000-12-28 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_85};"""
        exception ""
    }
    def const_sql_1_86 = """select "0000-12-28 01:01:00.9", cast(cast("0000-12-28 01:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_86};"""
        exception ""
    }
    def const_sql_1_87 = """select "0000-12-28 01:01:01.0", cast(cast("0000-12-28 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_87};"""
        exception ""
    }
    def const_sql_1_88 = """select "0000-12-28 01:01:01.0", cast(cast("0000-12-28 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_88};"""
        exception ""
    }
    def const_sql_1_89 = """select "0000-12-28 01:01:01.9", cast(cast("0000-12-28 01:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_89};"""
        exception ""
    }
    def const_sql_1_90 = """select "0000-12-28 01:01:59.0", cast(cast("0000-12-28 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_90};"""
        exception ""
    }
    def const_sql_1_91 = """select "0000-12-28 01:01:59.0", cast(cast("0000-12-28 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_91};"""
        exception ""
    }
    def const_sql_1_92 = """select "0000-12-28 01:01:59.9", cast(cast("0000-12-28 01:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_92};"""
        exception ""
    }
    def const_sql_1_93 = """select "0000-12-28 01:59:00.0", cast(cast("0000-12-28 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_93};"""
        exception ""
    }
    def const_sql_1_94 = """select "0000-12-28 01:59:00.0", cast(cast("0000-12-28 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_94};"""
        exception ""
    }
    def const_sql_1_95 = """select "0000-12-28 01:59:00.9", cast(cast("0000-12-28 01:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_95};"""
        exception ""
    }
    def const_sql_1_96 = """select "0000-12-28 01:59:01.0", cast(cast("0000-12-28 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_96};"""
        exception ""
    }
    def const_sql_1_97 = """select "0000-12-28 01:59:01.0", cast(cast("0000-12-28 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_97};"""
        exception ""
    }
    def const_sql_1_98 = """select "0000-12-28 01:59:01.9", cast(cast("0000-12-28 01:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_98};"""
        exception ""
    }
    def const_sql_1_99 = """select "0000-12-28 01:59:59.0", cast(cast("0000-12-28 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_99};"""
        exception ""
    }
    def const_sql_1_100 = """select "0000-12-28 01:59:59.0", cast(cast("0000-12-28 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_100};"""
        exception ""
    }
    def const_sql_1_101 = """select "0000-12-28 01:59:59.9", cast(cast("0000-12-28 01:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_101};"""
        exception ""
    }
    def const_sql_1_102 = """select "0000-12-28 23:00:00.0", cast(cast("0000-12-28 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_102};"""
        exception ""
    }
    def const_sql_1_103 = """select "0000-12-28 23:00:00.0", cast(cast("0000-12-28 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_103};"""
        exception ""
    }
    def const_sql_1_104 = """select "0000-12-28 23:00:00.9", cast(cast("0000-12-28 23:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_104};"""
        exception ""
    }
    def const_sql_1_105 = """select "0000-12-28 23:00:01.0", cast(cast("0000-12-28 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_105};"""
        exception ""
    }
    def const_sql_1_106 = """select "0000-12-28 23:00:01.0", cast(cast("0000-12-28 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_106};"""
        exception ""
    }
    def const_sql_1_107 = """select "0000-12-28 23:00:01.9", cast(cast("0000-12-28 23:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_107};"""
        exception ""
    }
    def const_sql_1_108 = """select "0000-12-28 23:00:59.0", cast(cast("0000-12-28 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_108};"""
        exception ""
    }
    def const_sql_1_109 = """select "0000-12-28 23:00:59.0", cast(cast("0000-12-28 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_109};"""
        exception ""
    }
    def const_sql_1_110 = """select "0000-12-28 23:00:59.9", cast(cast("0000-12-28 23:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_110};"""
        exception ""
    }
    def const_sql_1_111 = """select "0000-12-28 23:01:00.0", cast(cast("0000-12-28 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_111};"""
        exception ""
    }
    def const_sql_1_112 = """select "0000-12-28 23:01:00.0", cast(cast("0000-12-28 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_112};"""
        exception ""
    }
    def const_sql_1_113 = """select "0000-12-28 23:01:00.9", cast(cast("0000-12-28 23:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_113};"""
        exception ""
    }
    def const_sql_1_114 = """select "0000-12-28 23:01:01.0", cast(cast("0000-12-28 23:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_114};"""
        exception ""
    }
    def const_sql_1_115 = """select "0000-12-28 23:01:01.0", cast(cast("0000-12-28 23:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_115};"""
        exception ""
    }
    def const_sql_1_116 = """select "0000-12-28 23:01:01.9", cast(cast("0000-12-28 23:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_116};"""
        exception ""
    }
    def const_sql_1_117 = """select "0000-12-28 23:01:59.0", cast(cast("0000-12-28 23:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_117};"""
        exception ""
    }
    def const_sql_1_118 = """select "0000-12-28 23:01:59.0", cast(cast("0000-12-28 23:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_118};"""
        exception ""
    }
    def const_sql_1_119 = """select "0000-12-28 23:01:59.9", cast(cast("0000-12-28 23:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_119};"""
        exception ""
    }
    def const_sql_1_120 = """select "0000-12-28 23:59:00.0", cast(cast("0000-12-28 23:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_120};"""
        exception ""
    }
    def const_sql_1_121 = """select "0000-12-28 23:59:00.0", cast(cast("0000-12-28 23:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_121};"""
        exception ""
    }
    def const_sql_1_122 = """select "0000-12-28 23:59:00.9", cast(cast("0000-12-28 23:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_122};"""
        exception ""
    }
    def const_sql_1_123 = """select "0000-12-28 23:59:01.0", cast(cast("0000-12-28 23:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_123};"""
        exception ""
    }
    def const_sql_1_124 = """select "0000-12-28 23:59:01.0", cast(cast("0000-12-28 23:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_124};"""
        exception ""
    }
    def const_sql_1_125 = """select "0000-12-28 23:59:01.9", cast(cast("0000-12-28 23:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_125};"""
        exception ""
    }
    def const_sql_1_126 = """select "0000-12-28 23:59:59.0", cast(cast("0000-12-28 23:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_126};"""
        exception ""
    }
    def const_sql_1_127 = """select "0000-12-28 23:59:59.0", cast(cast("0000-12-28 23:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_127};"""
        exception ""
    }
    def const_sql_1_128 = """select "0000-12-28 23:59:59.9", cast(cast("0000-12-28 23:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_128};"""
        exception ""
    }
    def const_sql_1_129 = """select "0001-01-01 00:00:00.0", cast(cast("0001-01-01 00:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_129};"""
        exception ""
    }
    def const_sql_1_130 = """select "0001-01-01 00:00:00.0", cast(cast("0001-01-01 00:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_130};"""
        exception ""
    }
    def const_sql_1_131 = """select "0001-01-01 00:00:00.9", cast(cast("0001-01-01 00:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_131};"""
        exception ""
    }
    def const_sql_1_132 = """select "0001-01-01 00:00:01.0", cast(cast("0001-01-01 00:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_132};"""
        exception ""
    }
    def const_sql_1_133 = """select "0001-01-01 00:00:01.0", cast(cast("0001-01-01 00:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_133};"""
        exception ""
    }
    def const_sql_1_134 = """select "0001-01-01 00:00:01.9", cast(cast("0001-01-01 00:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_134};"""
        exception ""
    }
    def const_sql_1_135 = """select "0001-01-01 00:00:59.0", cast(cast("0001-01-01 00:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_135};"""
        exception ""
    }
    def const_sql_1_136 = """select "0001-01-01 00:00:59.0", cast(cast("0001-01-01 00:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_136};"""
        exception ""
    }
    def const_sql_1_137 = """select "0001-01-01 00:00:59.9", cast(cast("0001-01-01 00:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_137};"""
        exception ""
    }
    def const_sql_1_138 = """select "0001-01-01 00:01:00.0", cast(cast("0001-01-01 00:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_138};"""
        exception ""
    }
    def const_sql_1_139 = """select "0001-01-01 00:01:00.0", cast(cast("0001-01-01 00:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_139};"""
        exception ""
    }
    def const_sql_1_140 = """select "0001-01-01 00:01:00.9", cast(cast("0001-01-01 00:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_140};"""
        exception ""
    }
    def const_sql_1_141 = """select "0001-01-01 00:01:01.0", cast(cast("0001-01-01 00:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_141};"""
        exception ""
    }
    def const_sql_1_142 = """select "0001-01-01 00:01:01.0", cast(cast("0001-01-01 00:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_142};"""
        exception ""
    }
    def const_sql_1_143 = """select "0001-01-01 00:01:01.9", cast(cast("0001-01-01 00:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_143};"""
        exception ""
    }
    def const_sql_1_144 = """select "0001-01-01 00:01:59.0", cast(cast("0001-01-01 00:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_144};"""
        exception ""
    }
    def const_sql_1_145 = """select "0001-01-01 00:01:59.0", cast(cast("0001-01-01 00:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_145};"""
        exception ""
    }
    def const_sql_1_146 = """select "0001-01-01 00:01:59.9", cast(cast("0001-01-01 00:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_146};"""
        exception ""
    }
    def const_sql_1_147 = """select "0001-01-01 00:59:00.0", cast(cast("0001-01-01 00:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_147};"""
        exception ""
    }
    def const_sql_1_148 = """select "0001-01-01 00:59:00.0", cast(cast("0001-01-01 00:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_148};"""
        exception ""
    }
    def const_sql_1_149 = """select "0001-01-01 00:59:00.9", cast(cast("0001-01-01 00:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_149};"""
        exception ""
    }
    def const_sql_1_150 = """select "0001-01-01 00:59:01.0", cast(cast("0001-01-01 00:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_150};"""
        exception ""
    }
    def const_sql_1_151 = """select "0001-01-01 00:59:01.0", cast(cast("0001-01-01 00:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_151};"""
        exception ""
    }
    def const_sql_1_152 = """select "0001-01-01 00:59:01.9", cast(cast("0001-01-01 00:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_152};"""
        exception ""
    }
    def const_sql_1_153 = """select "0001-01-01 00:59:59.0", cast(cast("0001-01-01 00:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_153};"""
        exception ""
    }
    def const_sql_1_154 = """select "0001-01-01 00:59:59.0", cast(cast("0001-01-01 00:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_154};"""
        exception ""
    }
    def const_sql_1_155 = """select "0001-01-01 00:59:59.9", cast(cast("0001-01-01 00:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_155};"""
        exception ""
    }
    def const_sql_1_156 = """select "0001-01-01 01:00:00.0", cast(cast("0001-01-01 01:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_156};"""
        exception ""
    }
    def const_sql_1_157 = """select "0001-01-01 01:00:00.0", cast(cast("0001-01-01 01:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_157};"""
        exception ""
    }
    def const_sql_1_158 = """select "0001-01-01 01:00:00.9", cast(cast("0001-01-01 01:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_158};"""
        exception ""
    }
    def const_sql_1_159 = """select "0001-01-01 01:00:01.0", cast(cast("0001-01-01 01:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_159};"""
        exception ""
    }
    def const_sql_1_160 = """select "0001-01-01 01:00:01.0", cast(cast("0001-01-01 01:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_160};"""
        exception ""
    }
    def const_sql_1_161 = """select "0001-01-01 01:00:01.9", cast(cast("0001-01-01 01:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_161};"""
        exception ""
    }
    def const_sql_1_162 = """select "0001-01-01 01:00:59.0", cast(cast("0001-01-01 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_162};"""
        exception ""
    }
    def const_sql_1_163 = """select "0001-01-01 01:00:59.0", cast(cast("0001-01-01 01:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_163};"""
        exception ""
    }
    def const_sql_1_164 = """select "0001-01-01 01:00:59.9", cast(cast("0001-01-01 01:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_164};"""
        exception ""
    }
    def const_sql_1_165 = """select "0001-01-01 01:01:00.0", cast(cast("0001-01-01 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_165};"""
        exception ""
    }
    def const_sql_1_166 = """select "0001-01-01 01:01:00.0", cast(cast("0001-01-01 01:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_166};"""
        exception ""
    }
    def const_sql_1_167 = """select "0001-01-01 01:01:00.9", cast(cast("0001-01-01 01:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_167};"""
        exception ""
    }
    def const_sql_1_168 = """select "0001-01-01 01:01:01.0", cast(cast("0001-01-01 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_168};"""
        exception ""
    }
    def const_sql_1_169 = """select "0001-01-01 01:01:01.0", cast(cast("0001-01-01 01:01:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_169};"""
        exception ""
    }
    def const_sql_1_170 = """select "0001-01-01 01:01:01.9", cast(cast("0001-01-01 01:01:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_170};"""
        exception ""
    }
    def const_sql_1_171 = """select "0001-01-01 01:01:59.0", cast(cast("0001-01-01 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_171};"""
        exception ""
    }
    def const_sql_1_172 = """select "0001-01-01 01:01:59.0", cast(cast("0001-01-01 01:01:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_172};"""
        exception ""
    }
    def const_sql_1_173 = """select "0001-01-01 01:01:59.9", cast(cast("0001-01-01 01:01:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_173};"""
        exception ""
    }
    def const_sql_1_174 = """select "0001-01-01 01:59:00.0", cast(cast("0001-01-01 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_174};"""
        exception ""
    }
    def const_sql_1_175 = """select "0001-01-01 01:59:00.0", cast(cast("0001-01-01 01:59:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_175};"""
        exception ""
    }
    def const_sql_1_176 = """select "0001-01-01 01:59:00.9", cast(cast("0001-01-01 01:59:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_176};"""
        exception ""
    }
    def const_sql_1_177 = """select "0001-01-01 01:59:01.0", cast(cast("0001-01-01 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_177};"""
        exception ""
    }
    def const_sql_1_178 = """select "0001-01-01 01:59:01.0", cast(cast("0001-01-01 01:59:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_178};"""
        exception ""
    }
    def const_sql_1_179 = """select "0001-01-01 01:59:01.9", cast(cast("0001-01-01 01:59:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_179};"""
        exception ""
    }
    def const_sql_1_180 = """select "0001-01-01 01:59:59.0", cast(cast("0001-01-01 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_180};"""
        exception ""
    }
    def const_sql_1_181 = """select "0001-01-01 01:59:59.0", cast(cast("0001-01-01 01:59:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_181};"""
        exception ""
    }
    def const_sql_1_182 = """select "0001-01-01 01:59:59.9", cast(cast("0001-01-01 01:59:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_182};"""
        exception ""
    }
    def const_sql_1_183 = """select "0001-01-01 23:00:00.0", cast(cast("0001-01-01 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_183};"""
        exception ""
    }
    def const_sql_1_184 = """select "0001-01-01 23:00:00.0", cast(cast("0001-01-01 23:00:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_184};"""
        exception ""
    }
    def const_sql_1_185 = """select "0001-01-01 23:00:00.9", cast(cast("0001-01-01 23:00:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_185};"""
        exception ""
    }
    def const_sql_1_186 = """select "0001-01-01 23:00:01.0", cast(cast("0001-01-01 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_186};"""
        exception ""
    }
    def const_sql_1_187 = """select "0001-01-01 23:00:01.0", cast(cast("0001-01-01 23:00:01.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_187};"""
        exception ""
    }
    def const_sql_1_188 = """select "0001-01-01 23:00:01.9", cast(cast("0001-01-01 23:00:01.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_188};"""
        exception ""
    }
    def const_sql_1_189 = """select "0001-01-01 23:00:59.0", cast(cast("0001-01-01 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_189};"""
        exception ""
    }
    def const_sql_1_190 = """select "0001-01-01 23:00:59.0", cast(cast("0001-01-01 23:00:59.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_190};"""
        exception ""
    }
    def const_sql_1_191 = """select "0001-01-01 23:00:59.9", cast(cast("0001-01-01 23:00:59.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_191};"""
        exception ""
    }
    def const_sql_1_192 = """select "0001-01-01 23:01:00.0", cast(cast("0001-01-01 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_192};"""
        exception ""
    }
    def const_sql_1_193 = """select "0001-01-01 23:01:00.0", cast(cast("0001-01-01 23:01:00.0" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_193};"""
        exception ""
    }
    def const_sql_1_194 = """select "0001-01-01 23:01:00.9", cast(cast("0001-01-01 23:01:00.9" as datetimev2(1)) as float);"""

    test {
        sql """${const_sql_1_194};"""
        exception ""
    }

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