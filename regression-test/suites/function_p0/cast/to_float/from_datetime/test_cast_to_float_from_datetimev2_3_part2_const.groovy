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


suite("test_cast_to_float_from_datetimev2_3_part2_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_2_0 = """select "0001-01-01 23:01:01.000", cast(cast("0001-01-01 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_0};"""
        exception ""
    }
    def const_sql_2_1 = """select "0001-01-01 23:01:01.000", cast(cast("0001-01-01 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_1};"""
        exception ""
    }
    def const_sql_2_2 = """select "0001-01-01 23:01:01.999", cast(cast("0001-01-01 23:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_2};"""
        exception ""
    }
    def const_sql_2_3 = """select "0001-01-01 23:01:59.000", cast(cast("0001-01-01 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_3};"""
        exception ""
    }
    def const_sql_2_4 = """select "0001-01-01 23:01:59.000", cast(cast("0001-01-01 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_4};"""
        exception ""
    }
    def const_sql_2_5 = """select "0001-01-01 23:01:59.999", cast(cast("0001-01-01 23:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_5};"""
        exception ""
    }
    def const_sql_2_6 = """select "0001-01-01 23:59:00.000", cast(cast("0001-01-01 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_6};"""
        exception ""
    }
    def const_sql_2_7 = """select "0001-01-01 23:59:00.000", cast(cast("0001-01-01 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_7};"""
        exception ""
    }
    def const_sql_2_8 = """select "0001-01-01 23:59:00.999", cast(cast("0001-01-01 23:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_8};"""
        exception ""
    }
    def const_sql_2_9 = """select "0001-01-01 23:59:01.000", cast(cast("0001-01-01 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_9};"""
        exception ""
    }
    def const_sql_2_10 = """select "0001-01-01 23:59:01.000", cast(cast("0001-01-01 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_10};"""
        exception ""
    }
    def const_sql_2_11 = """select "0001-01-01 23:59:01.999", cast(cast("0001-01-01 23:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_11};"""
        exception ""
    }
    def const_sql_2_12 = """select "0001-01-01 23:59:59.000", cast(cast("0001-01-01 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_12};"""
        exception ""
    }
    def const_sql_2_13 = """select "0001-01-01 23:59:59.000", cast(cast("0001-01-01 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_13};"""
        exception ""
    }
    def const_sql_2_14 = """select "0001-01-01 23:59:59.999", cast(cast("0001-01-01 23:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_14};"""
        exception ""
    }
    def const_sql_2_15 = """select "0001-01-28 00:00:00.000", cast(cast("0001-01-28 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_15};"""
        exception ""
    }
    def const_sql_2_16 = """select "0001-01-28 00:00:00.000", cast(cast("0001-01-28 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_16};"""
        exception ""
    }
    def const_sql_2_17 = """select "0001-01-28 00:00:00.999", cast(cast("0001-01-28 00:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_17};"""
        exception ""
    }
    def const_sql_2_18 = """select "0001-01-28 00:00:01.000", cast(cast("0001-01-28 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_18};"""
        exception ""
    }
    def const_sql_2_19 = """select "0001-01-28 00:00:01.000", cast(cast("0001-01-28 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_19};"""
        exception ""
    }
    def const_sql_2_20 = """select "0001-01-28 00:00:01.999", cast(cast("0001-01-28 00:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_20};"""
        exception ""
    }
    def const_sql_2_21 = """select "0001-01-28 00:00:59.000", cast(cast("0001-01-28 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_21};"""
        exception ""
    }
    def const_sql_2_22 = """select "0001-01-28 00:00:59.000", cast(cast("0001-01-28 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_22};"""
        exception ""
    }
    def const_sql_2_23 = """select "0001-01-28 00:00:59.999", cast(cast("0001-01-28 00:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_23};"""
        exception ""
    }
    def const_sql_2_24 = """select "0001-01-28 00:01:00.000", cast(cast("0001-01-28 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_24};"""
        exception ""
    }
    def const_sql_2_25 = """select "0001-01-28 00:01:00.000", cast(cast("0001-01-28 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_25};"""
        exception ""
    }
    def const_sql_2_26 = """select "0001-01-28 00:01:00.999", cast(cast("0001-01-28 00:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_26};"""
        exception ""
    }
    def const_sql_2_27 = """select "0001-01-28 00:01:01.000", cast(cast("0001-01-28 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_27};"""
        exception ""
    }
    def const_sql_2_28 = """select "0001-01-28 00:01:01.000", cast(cast("0001-01-28 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_28};"""
        exception ""
    }
    def const_sql_2_29 = """select "0001-01-28 00:01:01.999", cast(cast("0001-01-28 00:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_29};"""
        exception ""
    }
    def const_sql_2_30 = """select "0001-01-28 00:01:59.000", cast(cast("0001-01-28 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_30};"""
        exception ""
    }
    def const_sql_2_31 = """select "0001-01-28 00:01:59.000", cast(cast("0001-01-28 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_31};"""
        exception ""
    }
    def const_sql_2_32 = """select "0001-01-28 00:01:59.999", cast(cast("0001-01-28 00:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_32};"""
        exception ""
    }
    def const_sql_2_33 = """select "0001-01-28 00:59:00.000", cast(cast("0001-01-28 00:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_33};"""
        exception ""
    }
    def const_sql_2_34 = """select "0001-01-28 00:59:00.000", cast(cast("0001-01-28 00:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_34};"""
        exception ""
    }
    def const_sql_2_35 = """select "0001-01-28 00:59:00.999", cast(cast("0001-01-28 00:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_35};"""
        exception ""
    }
    def const_sql_2_36 = """select "0001-01-28 00:59:01.000", cast(cast("0001-01-28 00:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_36};"""
        exception ""
    }
    def const_sql_2_37 = """select "0001-01-28 00:59:01.000", cast(cast("0001-01-28 00:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_37};"""
        exception ""
    }
    def const_sql_2_38 = """select "0001-01-28 00:59:01.999", cast(cast("0001-01-28 00:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_38};"""
        exception ""
    }
    def const_sql_2_39 = """select "0001-01-28 00:59:59.000", cast(cast("0001-01-28 00:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_39};"""
        exception ""
    }
    def const_sql_2_40 = """select "0001-01-28 00:59:59.000", cast(cast("0001-01-28 00:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_40};"""
        exception ""
    }
    def const_sql_2_41 = """select "0001-01-28 00:59:59.999", cast(cast("0001-01-28 00:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_41};"""
        exception ""
    }
    def const_sql_2_42 = """select "0001-01-28 01:00:00.000", cast(cast("0001-01-28 01:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_42};"""
        exception ""
    }
    def const_sql_2_43 = """select "0001-01-28 01:00:00.000", cast(cast("0001-01-28 01:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_43};"""
        exception ""
    }
    def const_sql_2_44 = """select "0001-01-28 01:00:00.999", cast(cast("0001-01-28 01:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_44};"""
        exception ""
    }
    def const_sql_2_45 = """select "0001-01-28 01:00:01.000", cast(cast("0001-01-28 01:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_45};"""
        exception ""
    }
    def const_sql_2_46 = """select "0001-01-28 01:00:01.000", cast(cast("0001-01-28 01:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_46};"""
        exception ""
    }
    def const_sql_2_47 = """select "0001-01-28 01:00:01.999", cast(cast("0001-01-28 01:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_47};"""
        exception ""
    }
    def const_sql_2_48 = """select "0001-01-28 01:00:59.000", cast(cast("0001-01-28 01:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_48};"""
        exception ""
    }
    def const_sql_2_49 = """select "0001-01-28 01:00:59.000", cast(cast("0001-01-28 01:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_49};"""
        exception ""
    }
    def const_sql_2_50 = """select "0001-01-28 01:00:59.999", cast(cast("0001-01-28 01:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_50};"""
        exception ""
    }
    def const_sql_2_51 = """select "0001-01-28 01:01:00.000", cast(cast("0001-01-28 01:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_51};"""
        exception ""
    }
    def const_sql_2_52 = """select "0001-01-28 01:01:00.000", cast(cast("0001-01-28 01:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_52};"""
        exception ""
    }
    def const_sql_2_53 = """select "0001-01-28 01:01:00.999", cast(cast("0001-01-28 01:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_53};"""
        exception ""
    }
    def const_sql_2_54 = """select "0001-01-28 01:01:01.000", cast(cast("0001-01-28 01:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_54};"""
        exception ""
    }
    def const_sql_2_55 = """select "0001-01-28 01:01:01.000", cast(cast("0001-01-28 01:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_55};"""
        exception ""
    }
    def const_sql_2_56 = """select "0001-01-28 01:01:01.999", cast(cast("0001-01-28 01:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_56};"""
        exception ""
    }
    def const_sql_2_57 = """select "0001-01-28 01:01:59.000", cast(cast("0001-01-28 01:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_57};"""
        exception ""
    }
    def const_sql_2_58 = """select "0001-01-28 01:01:59.000", cast(cast("0001-01-28 01:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_58};"""
        exception ""
    }
    def const_sql_2_59 = """select "0001-01-28 01:01:59.999", cast(cast("0001-01-28 01:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_59};"""
        exception ""
    }
    def const_sql_2_60 = """select "0001-01-28 01:59:00.000", cast(cast("0001-01-28 01:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_60};"""
        exception ""
    }
    def const_sql_2_61 = """select "0001-01-28 01:59:00.000", cast(cast("0001-01-28 01:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_61};"""
        exception ""
    }
    def const_sql_2_62 = """select "0001-01-28 01:59:00.999", cast(cast("0001-01-28 01:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_62};"""
        exception ""
    }
    def const_sql_2_63 = """select "0001-01-28 01:59:01.000", cast(cast("0001-01-28 01:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_63};"""
        exception ""
    }
    def const_sql_2_64 = """select "0001-01-28 01:59:01.000", cast(cast("0001-01-28 01:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_64};"""
        exception ""
    }
    def const_sql_2_65 = """select "0001-01-28 01:59:01.999", cast(cast("0001-01-28 01:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_65};"""
        exception ""
    }
    def const_sql_2_66 = """select "0001-01-28 01:59:59.000", cast(cast("0001-01-28 01:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_66};"""
        exception ""
    }
    def const_sql_2_67 = """select "0001-01-28 01:59:59.000", cast(cast("0001-01-28 01:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_67};"""
        exception ""
    }
    def const_sql_2_68 = """select "0001-01-28 01:59:59.999", cast(cast("0001-01-28 01:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_68};"""
        exception ""
    }
    def const_sql_2_69 = """select "0001-01-28 23:00:00.000", cast(cast("0001-01-28 23:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_69};"""
        exception ""
    }
    def const_sql_2_70 = """select "0001-01-28 23:00:00.000", cast(cast("0001-01-28 23:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_70};"""
        exception ""
    }
    def const_sql_2_71 = """select "0001-01-28 23:00:00.999", cast(cast("0001-01-28 23:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_71};"""
        exception ""
    }
    def const_sql_2_72 = """select "0001-01-28 23:00:01.000", cast(cast("0001-01-28 23:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_72};"""
        exception ""
    }
    def const_sql_2_73 = """select "0001-01-28 23:00:01.000", cast(cast("0001-01-28 23:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_73};"""
        exception ""
    }
    def const_sql_2_74 = """select "0001-01-28 23:00:01.999", cast(cast("0001-01-28 23:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_74};"""
        exception ""
    }
    def const_sql_2_75 = """select "0001-01-28 23:00:59.000", cast(cast("0001-01-28 23:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_75};"""
        exception ""
    }
    def const_sql_2_76 = """select "0001-01-28 23:00:59.000", cast(cast("0001-01-28 23:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_76};"""
        exception ""
    }
    def const_sql_2_77 = """select "0001-01-28 23:00:59.999", cast(cast("0001-01-28 23:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_77};"""
        exception ""
    }
    def const_sql_2_78 = """select "0001-01-28 23:01:00.000", cast(cast("0001-01-28 23:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_78};"""
        exception ""
    }
    def const_sql_2_79 = """select "0001-01-28 23:01:00.000", cast(cast("0001-01-28 23:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_79};"""
        exception ""
    }
    def const_sql_2_80 = """select "0001-01-28 23:01:00.999", cast(cast("0001-01-28 23:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_80};"""
        exception ""
    }
    def const_sql_2_81 = """select "0001-01-28 23:01:01.000", cast(cast("0001-01-28 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_81};"""
        exception ""
    }
    def const_sql_2_82 = """select "0001-01-28 23:01:01.000", cast(cast("0001-01-28 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_82};"""
        exception ""
    }
    def const_sql_2_83 = """select "0001-01-28 23:01:01.999", cast(cast("0001-01-28 23:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_83};"""
        exception ""
    }
    def const_sql_2_84 = """select "0001-01-28 23:01:59.000", cast(cast("0001-01-28 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_84};"""
        exception ""
    }
    def const_sql_2_85 = """select "0001-01-28 23:01:59.000", cast(cast("0001-01-28 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_85};"""
        exception ""
    }
    def const_sql_2_86 = """select "0001-01-28 23:01:59.999", cast(cast("0001-01-28 23:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_86};"""
        exception ""
    }
    def const_sql_2_87 = """select "0001-01-28 23:59:00.000", cast(cast("0001-01-28 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_87};"""
        exception ""
    }
    def const_sql_2_88 = """select "0001-01-28 23:59:00.000", cast(cast("0001-01-28 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_88};"""
        exception ""
    }
    def const_sql_2_89 = """select "0001-01-28 23:59:00.999", cast(cast("0001-01-28 23:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_89};"""
        exception ""
    }
    def const_sql_2_90 = """select "0001-01-28 23:59:01.000", cast(cast("0001-01-28 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_90};"""
        exception ""
    }
    def const_sql_2_91 = """select "0001-01-28 23:59:01.000", cast(cast("0001-01-28 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_91};"""
        exception ""
    }
    def const_sql_2_92 = """select "0001-01-28 23:59:01.999", cast(cast("0001-01-28 23:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_92};"""
        exception ""
    }
    def const_sql_2_93 = """select "0001-01-28 23:59:59.000", cast(cast("0001-01-28 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_93};"""
        exception ""
    }
    def const_sql_2_94 = """select "0001-01-28 23:59:59.000", cast(cast("0001-01-28 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_94};"""
        exception ""
    }
    def const_sql_2_95 = """select "0001-01-28 23:59:59.999", cast(cast("0001-01-28 23:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_95};"""
        exception ""
    }
    def const_sql_2_96 = """select "0001-12-01 00:00:00.000", cast(cast("0001-12-01 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_96};"""
        exception ""
    }
    def const_sql_2_97 = """select "0001-12-01 00:00:00.000", cast(cast("0001-12-01 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_97};"""
        exception ""
    }
    def const_sql_2_98 = """select "0001-12-01 00:00:00.999", cast(cast("0001-12-01 00:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_98};"""
        exception ""
    }
    def const_sql_2_99 = """select "0001-12-01 00:00:01.000", cast(cast("0001-12-01 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_99};"""
        exception ""
    }
    def const_sql_2_100 = """select "0001-12-01 00:00:01.000", cast(cast("0001-12-01 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_100};"""
        exception ""
    }
    def const_sql_2_101 = """select "0001-12-01 00:00:01.999", cast(cast("0001-12-01 00:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_101};"""
        exception ""
    }
    def const_sql_2_102 = """select "0001-12-01 00:00:59.000", cast(cast("0001-12-01 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_102};"""
        exception ""
    }
    def const_sql_2_103 = """select "0001-12-01 00:00:59.000", cast(cast("0001-12-01 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_103};"""
        exception ""
    }
    def const_sql_2_104 = """select "0001-12-01 00:00:59.999", cast(cast("0001-12-01 00:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_104};"""
        exception ""
    }
    def const_sql_2_105 = """select "0001-12-01 00:01:00.000", cast(cast("0001-12-01 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_105};"""
        exception ""
    }
    def const_sql_2_106 = """select "0001-12-01 00:01:00.000", cast(cast("0001-12-01 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_106};"""
        exception ""
    }
    def const_sql_2_107 = """select "0001-12-01 00:01:00.999", cast(cast("0001-12-01 00:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_107};"""
        exception ""
    }
    def const_sql_2_108 = """select "0001-12-01 00:01:01.000", cast(cast("0001-12-01 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_108};"""
        exception ""
    }
    def const_sql_2_109 = """select "0001-12-01 00:01:01.000", cast(cast("0001-12-01 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_109};"""
        exception ""
    }
    def const_sql_2_110 = """select "0001-12-01 00:01:01.999", cast(cast("0001-12-01 00:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_110};"""
        exception ""
    }
    def const_sql_2_111 = """select "0001-12-01 00:01:59.000", cast(cast("0001-12-01 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_111};"""
        exception ""
    }
    def const_sql_2_112 = """select "0001-12-01 00:01:59.000", cast(cast("0001-12-01 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_112};"""
        exception ""
    }
    def const_sql_2_113 = """select "0001-12-01 00:01:59.999", cast(cast("0001-12-01 00:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_113};"""
        exception ""
    }
    def const_sql_2_114 = """select "0001-12-01 00:59:00.000", cast(cast("0001-12-01 00:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_114};"""
        exception ""
    }
    def const_sql_2_115 = """select "0001-12-01 00:59:00.000", cast(cast("0001-12-01 00:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_115};"""
        exception ""
    }
    def const_sql_2_116 = """select "0001-12-01 00:59:00.999", cast(cast("0001-12-01 00:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_116};"""
        exception ""
    }
    def const_sql_2_117 = """select "0001-12-01 00:59:01.000", cast(cast("0001-12-01 00:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_117};"""
        exception ""
    }
    def const_sql_2_118 = """select "0001-12-01 00:59:01.000", cast(cast("0001-12-01 00:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_118};"""
        exception ""
    }
    def const_sql_2_119 = """select "0001-12-01 00:59:01.999", cast(cast("0001-12-01 00:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_119};"""
        exception ""
    }
    def const_sql_2_120 = """select "0001-12-01 00:59:59.000", cast(cast("0001-12-01 00:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_120};"""
        exception ""
    }
    def const_sql_2_121 = """select "0001-12-01 00:59:59.000", cast(cast("0001-12-01 00:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_121};"""
        exception ""
    }
    def const_sql_2_122 = """select "0001-12-01 00:59:59.999", cast(cast("0001-12-01 00:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_122};"""
        exception ""
    }
    def const_sql_2_123 = """select "0001-12-01 01:00:00.000", cast(cast("0001-12-01 01:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_123};"""
        exception ""
    }
    def const_sql_2_124 = """select "0001-12-01 01:00:00.000", cast(cast("0001-12-01 01:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_124};"""
        exception ""
    }
    def const_sql_2_125 = """select "0001-12-01 01:00:00.999", cast(cast("0001-12-01 01:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_125};"""
        exception ""
    }
    def const_sql_2_126 = """select "0001-12-01 01:00:01.000", cast(cast("0001-12-01 01:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_126};"""
        exception ""
    }
    def const_sql_2_127 = """select "0001-12-01 01:00:01.000", cast(cast("0001-12-01 01:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_127};"""
        exception ""
    }
    def const_sql_2_128 = """select "0001-12-01 01:00:01.999", cast(cast("0001-12-01 01:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_128};"""
        exception ""
    }
    def const_sql_2_129 = """select "0001-12-01 01:00:59.000", cast(cast("0001-12-01 01:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_129};"""
        exception ""
    }
    def const_sql_2_130 = """select "0001-12-01 01:00:59.000", cast(cast("0001-12-01 01:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_130};"""
        exception ""
    }
    def const_sql_2_131 = """select "0001-12-01 01:00:59.999", cast(cast("0001-12-01 01:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_131};"""
        exception ""
    }
    def const_sql_2_132 = """select "0001-12-01 01:01:00.000", cast(cast("0001-12-01 01:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_132};"""
        exception ""
    }
    def const_sql_2_133 = """select "0001-12-01 01:01:00.000", cast(cast("0001-12-01 01:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_133};"""
        exception ""
    }
    def const_sql_2_134 = """select "0001-12-01 01:01:00.999", cast(cast("0001-12-01 01:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_134};"""
        exception ""
    }
    def const_sql_2_135 = """select "0001-12-01 01:01:01.000", cast(cast("0001-12-01 01:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_135};"""
        exception ""
    }
    def const_sql_2_136 = """select "0001-12-01 01:01:01.000", cast(cast("0001-12-01 01:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_136};"""
        exception ""
    }
    def const_sql_2_137 = """select "0001-12-01 01:01:01.999", cast(cast("0001-12-01 01:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_137};"""
        exception ""
    }
    def const_sql_2_138 = """select "0001-12-01 01:01:59.000", cast(cast("0001-12-01 01:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_138};"""
        exception ""
    }
    def const_sql_2_139 = """select "0001-12-01 01:01:59.000", cast(cast("0001-12-01 01:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_139};"""
        exception ""
    }
    def const_sql_2_140 = """select "0001-12-01 01:01:59.999", cast(cast("0001-12-01 01:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_140};"""
        exception ""
    }
    def const_sql_2_141 = """select "0001-12-01 01:59:00.000", cast(cast("0001-12-01 01:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_141};"""
        exception ""
    }
    def const_sql_2_142 = """select "0001-12-01 01:59:00.000", cast(cast("0001-12-01 01:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_142};"""
        exception ""
    }
    def const_sql_2_143 = """select "0001-12-01 01:59:00.999", cast(cast("0001-12-01 01:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_143};"""
        exception ""
    }
    def const_sql_2_144 = """select "0001-12-01 01:59:01.000", cast(cast("0001-12-01 01:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_144};"""
        exception ""
    }
    def const_sql_2_145 = """select "0001-12-01 01:59:01.000", cast(cast("0001-12-01 01:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_145};"""
        exception ""
    }
    def const_sql_2_146 = """select "0001-12-01 01:59:01.999", cast(cast("0001-12-01 01:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_146};"""
        exception ""
    }
    def const_sql_2_147 = """select "0001-12-01 01:59:59.000", cast(cast("0001-12-01 01:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_147};"""
        exception ""
    }
    def const_sql_2_148 = """select "0001-12-01 01:59:59.000", cast(cast("0001-12-01 01:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_148};"""
        exception ""
    }
    def const_sql_2_149 = """select "0001-12-01 01:59:59.999", cast(cast("0001-12-01 01:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_149};"""
        exception ""
    }
    def const_sql_2_150 = """select "0001-12-01 23:00:00.000", cast(cast("0001-12-01 23:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_150};"""
        exception ""
    }
    def const_sql_2_151 = """select "0001-12-01 23:00:00.000", cast(cast("0001-12-01 23:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_151};"""
        exception ""
    }
    def const_sql_2_152 = """select "0001-12-01 23:00:00.999", cast(cast("0001-12-01 23:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_152};"""
        exception ""
    }
    def const_sql_2_153 = """select "0001-12-01 23:00:01.000", cast(cast("0001-12-01 23:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_153};"""
        exception ""
    }
    def const_sql_2_154 = """select "0001-12-01 23:00:01.000", cast(cast("0001-12-01 23:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_154};"""
        exception ""
    }
    def const_sql_2_155 = """select "0001-12-01 23:00:01.999", cast(cast("0001-12-01 23:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_155};"""
        exception ""
    }
    def const_sql_2_156 = """select "0001-12-01 23:00:59.000", cast(cast("0001-12-01 23:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_156};"""
        exception ""
    }
    def const_sql_2_157 = """select "0001-12-01 23:00:59.000", cast(cast("0001-12-01 23:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_157};"""
        exception ""
    }
    def const_sql_2_158 = """select "0001-12-01 23:00:59.999", cast(cast("0001-12-01 23:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_158};"""
        exception ""
    }
    def const_sql_2_159 = """select "0001-12-01 23:01:00.000", cast(cast("0001-12-01 23:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_159};"""
        exception ""
    }
    def const_sql_2_160 = """select "0001-12-01 23:01:00.000", cast(cast("0001-12-01 23:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_160};"""
        exception ""
    }
    def const_sql_2_161 = """select "0001-12-01 23:01:00.999", cast(cast("0001-12-01 23:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_161};"""
        exception ""
    }
    def const_sql_2_162 = """select "0001-12-01 23:01:01.000", cast(cast("0001-12-01 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_162};"""
        exception ""
    }
    def const_sql_2_163 = """select "0001-12-01 23:01:01.000", cast(cast("0001-12-01 23:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_163};"""
        exception ""
    }
    def const_sql_2_164 = """select "0001-12-01 23:01:01.999", cast(cast("0001-12-01 23:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_164};"""
        exception ""
    }
    def const_sql_2_165 = """select "0001-12-01 23:01:59.000", cast(cast("0001-12-01 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_165};"""
        exception ""
    }
    def const_sql_2_166 = """select "0001-12-01 23:01:59.000", cast(cast("0001-12-01 23:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_166};"""
        exception ""
    }
    def const_sql_2_167 = """select "0001-12-01 23:01:59.999", cast(cast("0001-12-01 23:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_167};"""
        exception ""
    }
    def const_sql_2_168 = """select "0001-12-01 23:59:00.000", cast(cast("0001-12-01 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_168};"""
        exception ""
    }
    def const_sql_2_169 = """select "0001-12-01 23:59:00.000", cast(cast("0001-12-01 23:59:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_169};"""
        exception ""
    }
    def const_sql_2_170 = """select "0001-12-01 23:59:00.999", cast(cast("0001-12-01 23:59:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_170};"""
        exception ""
    }
    def const_sql_2_171 = """select "0001-12-01 23:59:01.000", cast(cast("0001-12-01 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_171};"""
        exception ""
    }
    def const_sql_2_172 = """select "0001-12-01 23:59:01.000", cast(cast("0001-12-01 23:59:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_172};"""
        exception ""
    }
    def const_sql_2_173 = """select "0001-12-01 23:59:01.999", cast(cast("0001-12-01 23:59:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_173};"""
        exception ""
    }
    def const_sql_2_174 = """select "0001-12-01 23:59:59.000", cast(cast("0001-12-01 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_174};"""
        exception ""
    }
    def const_sql_2_175 = """select "0001-12-01 23:59:59.000", cast(cast("0001-12-01 23:59:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_175};"""
        exception ""
    }
    def const_sql_2_176 = """select "0001-12-01 23:59:59.999", cast(cast("0001-12-01 23:59:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_176};"""
        exception ""
    }
    def const_sql_2_177 = """select "0001-12-28 00:00:00.000", cast(cast("0001-12-28 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_177};"""
        exception ""
    }
    def const_sql_2_178 = """select "0001-12-28 00:00:00.000", cast(cast("0001-12-28 00:00:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_178};"""
        exception ""
    }
    def const_sql_2_179 = """select "0001-12-28 00:00:00.999", cast(cast("0001-12-28 00:00:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_179};"""
        exception ""
    }
    def const_sql_2_180 = """select "0001-12-28 00:00:01.000", cast(cast("0001-12-28 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_180};"""
        exception ""
    }
    def const_sql_2_181 = """select "0001-12-28 00:00:01.000", cast(cast("0001-12-28 00:00:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_181};"""
        exception ""
    }
    def const_sql_2_182 = """select "0001-12-28 00:00:01.999", cast(cast("0001-12-28 00:00:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_182};"""
        exception ""
    }
    def const_sql_2_183 = """select "0001-12-28 00:00:59.000", cast(cast("0001-12-28 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_183};"""
        exception ""
    }
    def const_sql_2_184 = """select "0001-12-28 00:00:59.000", cast(cast("0001-12-28 00:00:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_184};"""
        exception ""
    }
    def const_sql_2_185 = """select "0001-12-28 00:00:59.999", cast(cast("0001-12-28 00:00:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_185};"""
        exception ""
    }
    def const_sql_2_186 = """select "0001-12-28 00:01:00.000", cast(cast("0001-12-28 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_186};"""
        exception ""
    }
    def const_sql_2_187 = """select "0001-12-28 00:01:00.000", cast(cast("0001-12-28 00:01:00.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_187};"""
        exception ""
    }
    def const_sql_2_188 = """select "0001-12-28 00:01:00.999", cast(cast("0001-12-28 00:01:00.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_188};"""
        exception ""
    }
    def const_sql_2_189 = """select "0001-12-28 00:01:01.000", cast(cast("0001-12-28 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_189};"""
        exception ""
    }
    def const_sql_2_190 = """select "0001-12-28 00:01:01.000", cast(cast("0001-12-28 00:01:01.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_190};"""
        exception ""
    }
    def const_sql_2_191 = """select "0001-12-28 00:01:01.999", cast(cast("0001-12-28 00:01:01.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_191};"""
        exception ""
    }
    def const_sql_2_192 = """select "0001-12-28 00:01:59.000", cast(cast("0001-12-28 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_192};"""
        exception ""
    }
    def const_sql_2_193 = """select "0001-12-28 00:01:59.000", cast(cast("0001-12-28 00:01:59.000" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_193};"""
        exception ""
    }
    def const_sql_2_194 = """select "0001-12-28 00:01:59.999", cast(cast("0001-12-28 00:01:59.999" as datetimev2(3)) as float);"""

    test {
        sql """${const_sql_2_194};"""
        exception ""
    }

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
    qt_sql_2_64_non_strict "${const_sql_2_64}"
    testFoldConst("${const_sql_2_64}")
    qt_sql_2_65_non_strict "${const_sql_2_65}"
    testFoldConst("${const_sql_2_65}")
    qt_sql_2_66_non_strict "${const_sql_2_66}"
    testFoldConst("${const_sql_2_66}")
    qt_sql_2_67_non_strict "${const_sql_2_67}"
    testFoldConst("${const_sql_2_67}")
    qt_sql_2_68_non_strict "${const_sql_2_68}"
    testFoldConst("${const_sql_2_68}")
    qt_sql_2_69_non_strict "${const_sql_2_69}"
    testFoldConst("${const_sql_2_69}")
    qt_sql_2_70_non_strict "${const_sql_2_70}"
    testFoldConst("${const_sql_2_70}")
    qt_sql_2_71_non_strict "${const_sql_2_71}"
    testFoldConst("${const_sql_2_71}")
    qt_sql_2_72_non_strict "${const_sql_2_72}"
    testFoldConst("${const_sql_2_72}")
    qt_sql_2_73_non_strict "${const_sql_2_73}"
    testFoldConst("${const_sql_2_73}")
    qt_sql_2_74_non_strict "${const_sql_2_74}"
    testFoldConst("${const_sql_2_74}")
    qt_sql_2_75_non_strict "${const_sql_2_75}"
    testFoldConst("${const_sql_2_75}")
    qt_sql_2_76_non_strict "${const_sql_2_76}"
    testFoldConst("${const_sql_2_76}")
    qt_sql_2_77_non_strict "${const_sql_2_77}"
    testFoldConst("${const_sql_2_77}")
    qt_sql_2_78_non_strict "${const_sql_2_78}"
    testFoldConst("${const_sql_2_78}")
    qt_sql_2_79_non_strict "${const_sql_2_79}"
    testFoldConst("${const_sql_2_79}")
    qt_sql_2_80_non_strict "${const_sql_2_80}"
    testFoldConst("${const_sql_2_80}")
    qt_sql_2_81_non_strict "${const_sql_2_81}"
    testFoldConst("${const_sql_2_81}")
    qt_sql_2_82_non_strict "${const_sql_2_82}"
    testFoldConst("${const_sql_2_82}")
    qt_sql_2_83_non_strict "${const_sql_2_83}"
    testFoldConst("${const_sql_2_83}")
    qt_sql_2_84_non_strict "${const_sql_2_84}"
    testFoldConst("${const_sql_2_84}")
    qt_sql_2_85_non_strict "${const_sql_2_85}"
    testFoldConst("${const_sql_2_85}")
    qt_sql_2_86_non_strict "${const_sql_2_86}"
    testFoldConst("${const_sql_2_86}")
    qt_sql_2_87_non_strict "${const_sql_2_87}"
    testFoldConst("${const_sql_2_87}")
    qt_sql_2_88_non_strict "${const_sql_2_88}"
    testFoldConst("${const_sql_2_88}")
    qt_sql_2_89_non_strict "${const_sql_2_89}"
    testFoldConst("${const_sql_2_89}")
    qt_sql_2_90_non_strict "${const_sql_2_90}"
    testFoldConst("${const_sql_2_90}")
    qt_sql_2_91_non_strict "${const_sql_2_91}"
    testFoldConst("${const_sql_2_91}")
    qt_sql_2_92_non_strict "${const_sql_2_92}"
    testFoldConst("${const_sql_2_92}")
    qt_sql_2_93_non_strict "${const_sql_2_93}"
    testFoldConst("${const_sql_2_93}")
    qt_sql_2_94_non_strict "${const_sql_2_94}"
    testFoldConst("${const_sql_2_94}")
    qt_sql_2_95_non_strict "${const_sql_2_95}"
    testFoldConst("${const_sql_2_95}")
    qt_sql_2_96_non_strict "${const_sql_2_96}"
    testFoldConst("${const_sql_2_96}")
    qt_sql_2_97_non_strict "${const_sql_2_97}"
    testFoldConst("${const_sql_2_97}")
    qt_sql_2_98_non_strict "${const_sql_2_98}"
    testFoldConst("${const_sql_2_98}")
    qt_sql_2_99_non_strict "${const_sql_2_99}"
    testFoldConst("${const_sql_2_99}")
    qt_sql_2_100_non_strict "${const_sql_2_100}"
    testFoldConst("${const_sql_2_100}")
    qt_sql_2_101_non_strict "${const_sql_2_101}"
    testFoldConst("${const_sql_2_101}")
    qt_sql_2_102_non_strict "${const_sql_2_102}"
    testFoldConst("${const_sql_2_102}")
    qt_sql_2_103_non_strict "${const_sql_2_103}"
    testFoldConst("${const_sql_2_103}")
    qt_sql_2_104_non_strict "${const_sql_2_104}"
    testFoldConst("${const_sql_2_104}")
    qt_sql_2_105_non_strict "${const_sql_2_105}"
    testFoldConst("${const_sql_2_105}")
    qt_sql_2_106_non_strict "${const_sql_2_106}"
    testFoldConst("${const_sql_2_106}")
    qt_sql_2_107_non_strict "${const_sql_2_107}"
    testFoldConst("${const_sql_2_107}")
    qt_sql_2_108_non_strict "${const_sql_2_108}"
    testFoldConst("${const_sql_2_108}")
    qt_sql_2_109_non_strict "${const_sql_2_109}"
    testFoldConst("${const_sql_2_109}")
    qt_sql_2_110_non_strict "${const_sql_2_110}"
    testFoldConst("${const_sql_2_110}")
    qt_sql_2_111_non_strict "${const_sql_2_111}"
    testFoldConst("${const_sql_2_111}")
    qt_sql_2_112_non_strict "${const_sql_2_112}"
    testFoldConst("${const_sql_2_112}")
    qt_sql_2_113_non_strict "${const_sql_2_113}"
    testFoldConst("${const_sql_2_113}")
    qt_sql_2_114_non_strict "${const_sql_2_114}"
    testFoldConst("${const_sql_2_114}")
    qt_sql_2_115_non_strict "${const_sql_2_115}"
    testFoldConst("${const_sql_2_115}")
    qt_sql_2_116_non_strict "${const_sql_2_116}"
    testFoldConst("${const_sql_2_116}")
    qt_sql_2_117_non_strict "${const_sql_2_117}"
    testFoldConst("${const_sql_2_117}")
    qt_sql_2_118_non_strict "${const_sql_2_118}"
    testFoldConst("${const_sql_2_118}")
    qt_sql_2_119_non_strict "${const_sql_2_119}"
    testFoldConst("${const_sql_2_119}")
    qt_sql_2_120_non_strict "${const_sql_2_120}"
    testFoldConst("${const_sql_2_120}")
    qt_sql_2_121_non_strict "${const_sql_2_121}"
    testFoldConst("${const_sql_2_121}")
    qt_sql_2_122_non_strict "${const_sql_2_122}"
    testFoldConst("${const_sql_2_122}")
    qt_sql_2_123_non_strict "${const_sql_2_123}"
    testFoldConst("${const_sql_2_123}")
    qt_sql_2_124_non_strict "${const_sql_2_124}"
    testFoldConst("${const_sql_2_124}")
    qt_sql_2_125_non_strict "${const_sql_2_125}"
    testFoldConst("${const_sql_2_125}")
    qt_sql_2_126_non_strict "${const_sql_2_126}"
    testFoldConst("${const_sql_2_126}")
    qt_sql_2_127_non_strict "${const_sql_2_127}"
    testFoldConst("${const_sql_2_127}")
    qt_sql_2_128_non_strict "${const_sql_2_128}"
    testFoldConst("${const_sql_2_128}")
    qt_sql_2_129_non_strict "${const_sql_2_129}"
    testFoldConst("${const_sql_2_129}")
    qt_sql_2_130_non_strict "${const_sql_2_130}"
    testFoldConst("${const_sql_2_130}")
    qt_sql_2_131_non_strict "${const_sql_2_131}"
    testFoldConst("${const_sql_2_131}")
    qt_sql_2_132_non_strict "${const_sql_2_132}"
    testFoldConst("${const_sql_2_132}")
    qt_sql_2_133_non_strict "${const_sql_2_133}"
    testFoldConst("${const_sql_2_133}")
    qt_sql_2_134_non_strict "${const_sql_2_134}"
    testFoldConst("${const_sql_2_134}")
    qt_sql_2_135_non_strict "${const_sql_2_135}"
    testFoldConst("${const_sql_2_135}")
    qt_sql_2_136_non_strict "${const_sql_2_136}"
    testFoldConst("${const_sql_2_136}")
    qt_sql_2_137_non_strict "${const_sql_2_137}"
    testFoldConst("${const_sql_2_137}")
    qt_sql_2_138_non_strict "${const_sql_2_138}"
    testFoldConst("${const_sql_2_138}")
    qt_sql_2_139_non_strict "${const_sql_2_139}"
    testFoldConst("${const_sql_2_139}")
    qt_sql_2_140_non_strict "${const_sql_2_140}"
    testFoldConst("${const_sql_2_140}")
    qt_sql_2_141_non_strict "${const_sql_2_141}"
    testFoldConst("${const_sql_2_141}")
    qt_sql_2_142_non_strict "${const_sql_2_142}"
    testFoldConst("${const_sql_2_142}")
    qt_sql_2_143_non_strict "${const_sql_2_143}"
    testFoldConst("${const_sql_2_143}")
    qt_sql_2_144_non_strict "${const_sql_2_144}"
    testFoldConst("${const_sql_2_144}")
    qt_sql_2_145_non_strict "${const_sql_2_145}"
    testFoldConst("${const_sql_2_145}")
    qt_sql_2_146_non_strict "${const_sql_2_146}"
    testFoldConst("${const_sql_2_146}")
    qt_sql_2_147_non_strict "${const_sql_2_147}"
    testFoldConst("${const_sql_2_147}")
    qt_sql_2_148_non_strict "${const_sql_2_148}"
    testFoldConst("${const_sql_2_148}")
    qt_sql_2_149_non_strict "${const_sql_2_149}"
    testFoldConst("${const_sql_2_149}")
    qt_sql_2_150_non_strict "${const_sql_2_150}"
    testFoldConst("${const_sql_2_150}")
    qt_sql_2_151_non_strict "${const_sql_2_151}"
    testFoldConst("${const_sql_2_151}")
    qt_sql_2_152_non_strict "${const_sql_2_152}"
    testFoldConst("${const_sql_2_152}")
    qt_sql_2_153_non_strict "${const_sql_2_153}"
    testFoldConst("${const_sql_2_153}")
    qt_sql_2_154_non_strict "${const_sql_2_154}"
    testFoldConst("${const_sql_2_154}")
    qt_sql_2_155_non_strict "${const_sql_2_155}"
    testFoldConst("${const_sql_2_155}")
    qt_sql_2_156_non_strict "${const_sql_2_156}"
    testFoldConst("${const_sql_2_156}")
    qt_sql_2_157_non_strict "${const_sql_2_157}"
    testFoldConst("${const_sql_2_157}")
    qt_sql_2_158_non_strict "${const_sql_2_158}"
    testFoldConst("${const_sql_2_158}")
    qt_sql_2_159_non_strict "${const_sql_2_159}"
    testFoldConst("${const_sql_2_159}")
    qt_sql_2_160_non_strict "${const_sql_2_160}"
    testFoldConst("${const_sql_2_160}")
    qt_sql_2_161_non_strict "${const_sql_2_161}"
    testFoldConst("${const_sql_2_161}")
    qt_sql_2_162_non_strict "${const_sql_2_162}"
    testFoldConst("${const_sql_2_162}")
    qt_sql_2_163_non_strict "${const_sql_2_163}"
    testFoldConst("${const_sql_2_163}")
    qt_sql_2_164_non_strict "${const_sql_2_164}"
    testFoldConst("${const_sql_2_164}")
    qt_sql_2_165_non_strict "${const_sql_2_165}"
    testFoldConst("${const_sql_2_165}")
    qt_sql_2_166_non_strict "${const_sql_2_166}"
    testFoldConst("${const_sql_2_166}")
    qt_sql_2_167_non_strict "${const_sql_2_167}"
    testFoldConst("${const_sql_2_167}")
    qt_sql_2_168_non_strict "${const_sql_2_168}"
    testFoldConst("${const_sql_2_168}")
    qt_sql_2_169_non_strict "${const_sql_2_169}"
    testFoldConst("${const_sql_2_169}")
    qt_sql_2_170_non_strict "${const_sql_2_170}"
    testFoldConst("${const_sql_2_170}")
    qt_sql_2_171_non_strict "${const_sql_2_171}"
    testFoldConst("${const_sql_2_171}")
    qt_sql_2_172_non_strict "${const_sql_2_172}"
    testFoldConst("${const_sql_2_172}")
    qt_sql_2_173_non_strict "${const_sql_2_173}"
    testFoldConst("${const_sql_2_173}")
    qt_sql_2_174_non_strict "${const_sql_2_174}"
    testFoldConst("${const_sql_2_174}")
    qt_sql_2_175_non_strict "${const_sql_2_175}"
    testFoldConst("${const_sql_2_175}")
    qt_sql_2_176_non_strict "${const_sql_2_176}"
    testFoldConst("${const_sql_2_176}")
    qt_sql_2_177_non_strict "${const_sql_2_177}"
    testFoldConst("${const_sql_2_177}")
    qt_sql_2_178_non_strict "${const_sql_2_178}"
    testFoldConst("${const_sql_2_178}")
    qt_sql_2_179_non_strict "${const_sql_2_179}"
    testFoldConst("${const_sql_2_179}")
    qt_sql_2_180_non_strict "${const_sql_2_180}"
    testFoldConst("${const_sql_2_180}")
    qt_sql_2_181_non_strict "${const_sql_2_181}"
    testFoldConst("${const_sql_2_181}")
    qt_sql_2_182_non_strict "${const_sql_2_182}"
    testFoldConst("${const_sql_2_182}")
    qt_sql_2_183_non_strict "${const_sql_2_183}"
    testFoldConst("${const_sql_2_183}")
    qt_sql_2_184_non_strict "${const_sql_2_184}"
    testFoldConst("${const_sql_2_184}")
    qt_sql_2_185_non_strict "${const_sql_2_185}"
    testFoldConst("${const_sql_2_185}")
    qt_sql_2_186_non_strict "${const_sql_2_186}"
    testFoldConst("${const_sql_2_186}")
    qt_sql_2_187_non_strict "${const_sql_2_187}"
    testFoldConst("${const_sql_2_187}")
    qt_sql_2_188_non_strict "${const_sql_2_188}"
    testFoldConst("${const_sql_2_188}")
    qt_sql_2_189_non_strict "${const_sql_2_189}"
    testFoldConst("${const_sql_2_189}")
    qt_sql_2_190_non_strict "${const_sql_2_190}"
    testFoldConst("${const_sql_2_190}")
    qt_sql_2_191_non_strict "${const_sql_2_191}"
    testFoldConst("${const_sql_2_191}")
    qt_sql_2_192_non_strict "${const_sql_2_192}"
    testFoldConst("${const_sql_2_192}")
    qt_sql_2_193_non_strict "${const_sql_2_193}"
    testFoldConst("${const_sql_2_193}")
    qt_sql_2_194_non_strict "${const_sql_2_194}"
    testFoldConst("${const_sql_2_194}")
}