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


suite("test_cast_to_float_from_timev2_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "00:00:00.000000", cast(cast("00:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_0};"""
        exception ""
    }
    def const_sql_0_1 = """select "00:00:01.000000", cast(cast("00:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_1};"""
        exception ""
    }
    def const_sql_0_2 = """select "00:00:10.000000", cast(cast("00:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_2};"""
        exception ""
    }
    def const_sql_0_3 = """select "00:00:59.000000", cast(cast("00:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_3};"""
        exception ""
    }
    def const_sql_0_4 = """select "00:01:00.000000", cast(cast("00:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_4};"""
        exception ""
    }
    def const_sql_0_5 = """select "00:01:01.000000", cast(cast("00:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_5};"""
        exception ""
    }
    def const_sql_0_6 = """select "00:01:10.000000", cast(cast("00:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_6};"""
        exception ""
    }
    def const_sql_0_7 = """select "00:01:59.000000", cast(cast("00:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_7};"""
        exception ""
    }
    def const_sql_0_8 = """select "00:10:00.000000", cast(cast("00:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_8};"""
        exception ""
    }
    def const_sql_0_9 = """select "00:10:01.000000", cast(cast("00:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_9};"""
        exception ""
    }
    def const_sql_0_10 = """select "00:10:10.000000", cast(cast("00:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_10};"""
        exception ""
    }
    def const_sql_0_11 = """select "00:10:59.000000", cast(cast("00:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_11};"""
        exception ""
    }
    def const_sql_0_12 = """select "00:59:00.000000", cast(cast("00:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_12};"""
        exception ""
    }
    def const_sql_0_13 = """select "00:59:01.000000", cast(cast("00:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_13};"""
        exception ""
    }
    def const_sql_0_14 = """select "00:59:10.000000", cast(cast("00:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_14};"""
        exception ""
    }
    def const_sql_0_15 = """select "00:59:59.000000", cast(cast("00:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_15};"""
        exception ""
    }
    def const_sql_0_16 = """select "01:00:00.000000", cast(cast("01:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_16};"""
        exception ""
    }
    def const_sql_0_17 = """select "01:00:01.000000", cast(cast("01:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_17};"""
        exception ""
    }
    def const_sql_0_18 = """select "01:00:10.000000", cast(cast("01:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_18};"""
        exception ""
    }
    def const_sql_0_19 = """select "01:00:59.000000", cast(cast("01:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_19};"""
        exception ""
    }
    def const_sql_0_20 = """select "01:01:00.000000", cast(cast("01:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_20};"""
        exception ""
    }
    def const_sql_0_21 = """select "01:01:01.000000", cast(cast("01:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_21};"""
        exception ""
    }
    def const_sql_0_22 = """select "01:01:10.000000", cast(cast("01:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_22};"""
        exception ""
    }
    def const_sql_0_23 = """select "01:01:59.000000", cast(cast("01:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_23};"""
        exception ""
    }
    def const_sql_0_24 = """select "01:10:00.000000", cast(cast("01:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_24};"""
        exception ""
    }
    def const_sql_0_25 = """select "01:10:01.000000", cast(cast("01:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_25};"""
        exception ""
    }
    def const_sql_0_26 = """select "01:10:10.000000", cast(cast("01:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_26};"""
        exception ""
    }
    def const_sql_0_27 = """select "01:10:59.000000", cast(cast("01:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_27};"""
        exception ""
    }
    def const_sql_0_28 = """select "01:59:00.000000", cast(cast("01:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_28};"""
        exception ""
    }
    def const_sql_0_29 = """select "01:59:01.000000", cast(cast("01:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_29};"""
        exception ""
    }
    def const_sql_0_30 = """select "01:59:10.000000", cast(cast("01:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_30};"""
        exception ""
    }
    def const_sql_0_31 = """select "01:59:59.000000", cast(cast("01:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_31};"""
        exception ""
    }
    def const_sql_0_32 = """select "10:00:00.000000", cast(cast("10:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_32};"""
        exception ""
    }
    def const_sql_0_33 = """select "10:00:01.000000", cast(cast("10:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_33};"""
        exception ""
    }
    def const_sql_0_34 = """select "10:00:10.000000", cast(cast("10:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_34};"""
        exception ""
    }
    def const_sql_0_35 = """select "10:00:59.000000", cast(cast("10:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_35};"""
        exception ""
    }
    def const_sql_0_36 = """select "10:01:00.000000", cast(cast("10:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_36};"""
        exception ""
    }
    def const_sql_0_37 = """select "10:01:01.000000", cast(cast("10:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_37};"""
        exception ""
    }
    def const_sql_0_38 = """select "10:01:10.000000", cast(cast("10:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_38};"""
        exception ""
    }
    def const_sql_0_39 = """select "10:01:59.000000", cast(cast("10:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_39};"""
        exception ""
    }
    def const_sql_0_40 = """select "10:10:00.000000", cast(cast("10:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_40};"""
        exception ""
    }
    def const_sql_0_41 = """select "10:10:01.000000", cast(cast("10:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_41};"""
        exception ""
    }
    def const_sql_0_42 = """select "10:10:10.000000", cast(cast("10:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_42};"""
        exception ""
    }
    def const_sql_0_43 = """select "10:10:59.000000", cast(cast("10:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_43};"""
        exception ""
    }
    def const_sql_0_44 = """select "10:59:00.000000", cast(cast("10:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_44};"""
        exception ""
    }
    def const_sql_0_45 = """select "10:59:01.000000", cast(cast("10:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_45};"""
        exception ""
    }
    def const_sql_0_46 = """select "10:59:10.000000", cast(cast("10:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_46};"""
        exception ""
    }
    def const_sql_0_47 = """select "10:59:59.000000", cast(cast("10:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_47};"""
        exception ""
    }
    def const_sql_0_48 = """select "100:00:00.000000", cast(cast("100:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_48};"""
        exception ""
    }
    def const_sql_0_49 = """select "100:00:01.000000", cast(cast("100:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_49};"""
        exception ""
    }
    def const_sql_0_50 = """select "100:00:10.000000", cast(cast("100:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_50};"""
        exception ""
    }
    def const_sql_0_51 = """select "100:00:59.000000", cast(cast("100:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_51};"""
        exception ""
    }
    def const_sql_0_52 = """select "100:01:00.000000", cast(cast("100:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_52};"""
        exception ""
    }
    def const_sql_0_53 = """select "100:01:01.000000", cast(cast("100:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_53};"""
        exception ""
    }
    def const_sql_0_54 = """select "100:01:10.000000", cast(cast("100:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_54};"""
        exception ""
    }
    def const_sql_0_55 = """select "100:01:59.000000", cast(cast("100:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_55};"""
        exception ""
    }
    def const_sql_0_56 = """select "100:10:00.000000", cast(cast("100:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_56};"""
        exception ""
    }
    def const_sql_0_57 = """select "100:10:01.000000", cast(cast("100:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_57};"""
        exception ""
    }
    def const_sql_0_58 = """select "100:10:10.000000", cast(cast("100:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_58};"""
        exception ""
    }
    def const_sql_0_59 = """select "100:10:59.000000", cast(cast("100:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_59};"""
        exception ""
    }
    def const_sql_0_60 = """select "100:59:00.000000", cast(cast("100:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_60};"""
        exception ""
    }
    def const_sql_0_61 = """select "100:59:01.000000", cast(cast("100:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_61};"""
        exception ""
    }
    def const_sql_0_62 = """select "100:59:10.000000", cast(cast("100:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_62};"""
        exception ""
    }
    def const_sql_0_63 = """select "100:59:59.000000", cast(cast("100:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_63};"""
        exception ""
    }
    def const_sql_0_64 = """select "838:00:00.000000", cast(cast("838:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_64};"""
        exception ""
    }
    def const_sql_0_65 = """select "838:00:01.000000", cast(cast("838:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_65};"""
        exception ""
    }
    def const_sql_0_66 = """select "838:00:10.000000", cast(cast("838:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_66};"""
        exception ""
    }
    def const_sql_0_67 = """select "838:00:59.000000", cast(cast("838:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_67};"""
        exception ""
    }
    def const_sql_0_68 = """select "838:01:00.000000", cast(cast("838:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_68};"""
        exception ""
    }
    def const_sql_0_69 = """select "838:01:01.000000", cast(cast("838:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_69};"""
        exception ""
    }
    def const_sql_0_70 = """select "838:01:10.000000", cast(cast("838:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_70};"""
        exception ""
    }
    def const_sql_0_71 = """select "838:01:59.000000", cast(cast("838:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_71};"""
        exception ""
    }
    def const_sql_0_72 = """select "838:10:00.000000", cast(cast("838:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_72};"""
        exception ""
    }
    def const_sql_0_73 = """select "838:10:01.000000", cast(cast("838:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_73};"""
        exception ""
    }
    def const_sql_0_74 = """select "838:10:10.000000", cast(cast("838:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_74};"""
        exception ""
    }
    def const_sql_0_75 = """select "838:10:59.000000", cast(cast("838:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_75};"""
        exception ""
    }
    def const_sql_0_76 = """select "838:59:00.000000", cast(cast("838:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_76};"""
        exception ""
    }
    def const_sql_0_77 = """select "838:59:01.000000", cast(cast("838:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_77};"""
        exception ""
    }
    def const_sql_0_78 = """select "838:59:10.000000", cast(cast("838:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_78};"""
        exception ""
    }
    def const_sql_0_79 = """select "838:59:59.000000", cast(cast("838:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_79};"""
        exception ""
    }
    def const_sql_0_80 = """select "00:00:00.000000", cast(cast("00:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_80};"""
        exception ""
    }
    def const_sql_0_81 = """select "-00:00:01.000000", cast(cast("-00:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_81};"""
        exception ""
    }
    def const_sql_0_82 = """select "-00:00:10.000000", cast(cast("-00:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_82};"""
        exception ""
    }
    def const_sql_0_83 = """select "-00:00:59.000000", cast(cast("-00:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_83};"""
        exception ""
    }
    def const_sql_0_84 = """select "-00:01:00.000000", cast(cast("-00:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_84};"""
        exception ""
    }
    def const_sql_0_85 = """select "-00:01:01.000000", cast(cast("-00:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_85};"""
        exception ""
    }
    def const_sql_0_86 = """select "-00:01:10.000000", cast(cast("-00:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_86};"""
        exception ""
    }
    def const_sql_0_87 = """select "-00:01:59.000000", cast(cast("-00:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_87};"""
        exception ""
    }
    def const_sql_0_88 = """select "-00:10:00.000000", cast(cast("-00:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_88};"""
        exception ""
    }
    def const_sql_0_89 = """select "-00:10:01.000000", cast(cast("-00:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_89};"""
        exception ""
    }
    def const_sql_0_90 = """select "-00:10:10.000000", cast(cast("-00:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_90};"""
        exception ""
    }
    def const_sql_0_91 = """select "-00:10:59.000000", cast(cast("-00:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_91};"""
        exception ""
    }
    def const_sql_0_92 = """select "-00:59:00.000000", cast(cast("-00:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_92};"""
        exception ""
    }
    def const_sql_0_93 = """select "-00:59:01.000000", cast(cast("-00:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_93};"""
        exception ""
    }
    def const_sql_0_94 = """select "-00:59:10.000000", cast(cast("-00:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_94};"""
        exception ""
    }
    def const_sql_0_95 = """select "-00:59:59.000000", cast(cast("-00:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_95};"""
        exception ""
    }
    def const_sql_0_96 = """select "-01:00:00.000000", cast(cast("-01:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_96};"""
        exception ""
    }
    def const_sql_0_97 = """select "-01:00:01.000000", cast(cast("-01:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_97};"""
        exception ""
    }
    def const_sql_0_98 = """select "-01:00:10.000000", cast(cast("-01:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_98};"""
        exception ""
    }
    def const_sql_0_99 = """select "-01:00:59.000000", cast(cast("-01:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_99};"""
        exception ""
    }
    def const_sql_0_100 = """select "-01:01:00.000000", cast(cast("-01:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_100};"""
        exception ""
    }
    def const_sql_0_101 = """select "-01:01:01.000000", cast(cast("-01:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_101};"""
        exception ""
    }
    def const_sql_0_102 = """select "-01:01:10.000000", cast(cast("-01:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_102};"""
        exception ""
    }
    def const_sql_0_103 = """select "-01:01:59.000000", cast(cast("-01:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_103};"""
        exception ""
    }
    def const_sql_0_104 = """select "-01:10:00.000000", cast(cast("-01:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_104};"""
        exception ""
    }
    def const_sql_0_105 = """select "-01:10:01.000000", cast(cast("-01:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_105};"""
        exception ""
    }
    def const_sql_0_106 = """select "-01:10:10.000000", cast(cast("-01:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_106};"""
        exception ""
    }
    def const_sql_0_107 = """select "-01:10:59.000000", cast(cast("-01:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_107};"""
        exception ""
    }
    def const_sql_0_108 = """select "-01:59:00.000000", cast(cast("-01:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_108};"""
        exception ""
    }
    def const_sql_0_109 = """select "-01:59:01.000000", cast(cast("-01:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_109};"""
        exception ""
    }
    def const_sql_0_110 = """select "-01:59:10.000000", cast(cast("-01:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_110};"""
        exception ""
    }
    def const_sql_0_111 = """select "-01:59:59.000000", cast(cast("-01:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_111};"""
        exception ""
    }
    def const_sql_0_112 = """select "-10:00:00.000000", cast(cast("-10:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_112};"""
        exception ""
    }
    def const_sql_0_113 = """select "-10:00:01.000000", cast(cast("-10:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_113};"""
        exception ""
    }
    def const_sql_0_114 = """select "-10:00:10.000000", cast(cast("-10:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_114};"""
        exception ""
    }
    def const_sql_0_115 = """select "-10:00:59.000000", cast(cast("-10:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_115};"""
        exception ""
    }
    def const_sql_0_116 = """select "-10:01:00.000000", cast(cast("-10:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_116};"""
        exception ""
    }
    def const_sql_0_117 = """select "-10:01:01.000000", cast(cast("-10:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_117};"""
        exception ""
    }
    def const_sql_0_118 = """select "-10:01:10.000000", cast(cast("-10:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_118};"""
        exception ""
    }
    def const_sql_0_119 = """select "-10:01:59.000000", cast(cast("-10:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_119};"""
        exception ""
    }
    def const_sql_0_120 = """select "-10:10:00.000000", cast(cast("-10:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_120};"""
        exception ""
    }
    def const_sql_0_121 = """select "-10:10:01.000000", cast(cast("-10:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_121};"""
        exception ""
    }
    def const_sql_0_122 = """select "-10:10:10.000000", cast(cast("-10:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_122};"""
        exception ""
    }
    def const_sql_0_123 = """select "-10:10:59.000000", cast(cast("-10:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_123};"""
        exception ""
    }
    def const_sql_0_124 = """select "-10:59:00.000000", cast(cast("-10:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_124};"""
        exception ""
    }
    def const_sql_0_125 = """select "-10:59:01.000000", cast(cast("-10:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_125};"""
        exception ""
    }
    def const_sql_0_126 = """select "-10:59:10.000000", cast(cast("-10:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_126};"""
        exception ""
    }
    def const_sql_0_127 = """select "-10:59:59.000000", cast(cast("-10:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_127};"""
        exception ""
    }
    def const_sql_0_128 = """select "-100:00:00.000000", cast(cast("-100:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_128};"""
        exception ""
    }
    def const_sql_0_129 = """select "-100:00:01.000000", cast(cast("-100:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_129};"""
        exception ""
    }
    def const_sql_0_130 = """select "-100:00:10.000000", cast(cast("-100:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_130};"""
        exception ""
    }
    def const_sql_0_131 = """select "-100:00:59.000000", cast(cast("-100:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_131};"""
        exception ""
    }
    def const_sql_0_132 = """select "-100:01:00.000000", cast(cast("-100:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_132};"""
        exception ""
    }
    def const_sql_0_133 = """select "-100:01:01.000000", cast(cast("-100:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_133};"""
        exception ""
    }
    def const_sql_0_134 = """select "-100:01:10.000000", cast(cast("-100:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_134};"""
        exception ""
    }
    def const_sql_0_135 = """select "-100:01:59.000000", cast(cast("-100:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_135};"""
        exception ""
    }
    def const_sql_0_136 = """select "-100:10:00.000000", cast(cast("-100:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_136};"""
        exception ""
    }
    def const_sql_0_137 = """select "-100:10:01.000000", cast(cast("-100:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_137};"""
        exception ""
    }
    def const_sql_0_138 = """select "-100:10:10.000000", cast(cast("-100:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_138};"""
        exception ""
    }
    def const_sql_0_139 = """select "-100:10:59.000000", cast(cast("-100:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_139};"""
        exception ""
    }
    def const_sql_0_140 = """select "-100:59:00.000000", cast(cast("-100:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_140};"""
        exception ""
    }
    def const_sql_0_141 = """select "-100:59:01.000000", cast(cast("-100:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_141};"""
        exception ""
    }
    def const_sql_0_142 = """select "-100:59:10.000000", cast(cast("-100:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_142};"""
        exception ""
    }
    def const_sql_0_143 = """select "-100:59:59.000000", cast(cast("-100:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_143};"""
        exception ""
    }
    def const_sql_0_144 = """select "-838:00:00.000000", cast(cast("-838:00:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_144};"""
        exception ""
    }
    def const_sql_0_145 = """select "-838:00:01.000000", cast(cast("-838:00:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_145};"""
        exception ""
    }
    def const_sql_0_146 = """select "-838:00:10.000000", cast(cast("-838:00:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_146};"""
        exception ""
    }
    def const_sql_0_147 = """select "-838:00:59.000000", cast(cast("-838:00:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_147};"""
        exception ""
    }
    def const_sql_0_148 = """select "-838:01:00.000000", cast(cast("-838:01:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_148};"""
        exception ""
    }
    def const_sql_0_149 = """select "-838:01:01.000000", cast(cast("-838:01:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_149};"""
        exception ""
    }
    def const_sql_0_150 = """select "-838:01:10.000000", cast(cast("-838:01:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_150};"""
        exception ""
    }
    def const_sql_0_151 = """select "-838:01:59.000000", cast(cast("-838:01:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_151};"""
        exception ""
    }
    def const_sql_0_152 = """select "-838:10:00.000000", cast(cast("-838:10:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_152};"""
        exception ""
    }
    def const_sql_0_153 = """select "-838:10:01.000000", cast(cast("-838:10:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_153};"""
        exception ""
    }
    def const_sql_0_154 = """select "-838:10:10.000000", cast(cast("-838:10:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_154};"""
        exception ""
    }
    def const_sql_0_155 = """select "-838:10:59.000000", cast(cast("-838:10:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_155};"""
        exception ""
    }
    def const_sql_0_156 = """select "-838:59:00.000000", cast(cast("-838:59:00.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_156};"""
        exception ""
    }
    def const_sql_0_157 = """select "-838:59:01.000000", cast(cast("-838:59:01.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_157};"""
        exception ""
    }
    def const_sql_0_158 = """select "-838:59:10.000000", cast(cast("-838:59:10.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_158};"""
        exception ""
    }
    def const_sql_0_159 = """select "-838:59:59.000000", cast(cast("-838:59:59.000000" as timev2) as float);"""

    test {
        sql """${const_sql_0_159};"""
        exception ""
    }

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
    qt_sql_0_16_non_strict "${const_sql_0_16}"
    testFoldConst("${const_sql_0_16}")
    qt_sql_0_17_non_strict "${const_sql_0_17}"
    testFoldConst("${const_sql_0_17}")
    qt_sql_0_18_non_strict "${const_sql_0_18}"
    testFoldConst("${const_sql_0_18}")
    qt_sql_0_19_non_strict "${const_sql_0_19}"
    testFoldConst("${const_sql_0_19}")
    qt_sql_0_20_non_strict "${const_sql_0_20}"
    testFoldConst("${const_sql_0_20}")
    qt_sql_0_21_non_strict "${const_sql_0_21}"
    testFoldConst("${const_sql_0_21}")
    qt_sql_0_22_non_strict "${const_sql_0_22}"
    testFoldConst("${const_sql_0_22}")
    qt_sql_0_23_non_strict "${const_sql_0_23}"
    testFoldConst("${const_sql_0_23}")
    qt_sql_0_24_non_strict "${const_sql_0_24}"
    testFoldConst("${const_sql_0_24}")
    qt_sql_0_25_non_strict "${const_sql_0_25}"
    testFoldConst("${const_sql_0_25}")
    qt_sql_0_26_non_strict "${const_sql_0_26}"
    testFoldConst("${const_sql_0_26}")
    qt_sql_0_27_non_strict "${const_sql_0_27}"
    testFoldConst("${const_sql_0_27}")
    qt_sql_0_28_non_strict "${const_sql_0_28}"
    testFoldConst("${const_sql_0_28}")
    qt_sql_0_29_non_strict "${const_sql_0_29}"
    testFoldConst("${const_sql_0_29}")
    qt_sql_0_30_non_strict "${const_sql_0_30}"
    testFoldConst("${const_sql_0_30}")
    qt_sql_0_31_non_strict "${const_sql_0_31}"
    testFoldConst("${const_sql_0_31}")
    qt_sql_0_32_non_strict "${const_sql_0_32}"
    testFoldConst("${const_sql_0_32}")
    qt_sql_0_33_non_strict "${const_sql_0_33}"
    testFoldConst("${const_sql_0_33}")
    qt_sql_0_34_non_strict "${const_sql_0_34}"
    testFoldConst("${const_sql_0_34}")
    qt_sql_0_35_non_strict "${const_sql_0_35}"
    testFoldConst("${const_sql_0_35}")
    qt_sql_0_36_non_strict "${const_sql_0_36}"
    testFoldConst("${const_sql_0_36}")
    qt_sql_0_37_non_strict "${const_sql_0_37}"
    testFoldConst("${const_sql_0_37}")
    qt_sql_0_38_non_strict "${const_sql_0_38}"
    testFoldConst("${const_sql_0_38}")
    qt_sql_0_39_non_strict "${const_sql_0_39}"
    testFoldConst("${const_sql_0_39}")
    qt_sql_0_40_non_strict "${const_sql_0_40}"
    testFoldConst("${const_sql_0_40}")
    qt_sql_0_41_non_strict "${const_sql_0_41}"
    testFoldConst("${const_sql_0_41}")
    qt_sql_0_42_non_strict "${const_sql_0_42}"
    testFoldConst("${const_sql_0_42}")
    qt_sql_0_43_non_strict "${const_sql_0_43}"
    testFoldConst("${const_sql_0_43}")
    qt_sql_0_44_non_strict "${const_sql_0_44}"
    testFoldConst("${const_sql_0_44}")
    qt_sql_0_45_non_strict "${const_sql_0_45}"
    testFoldConst("${const_sql_0_45}")
    qt_sql_0_46_non_strict "${const_sql_0_46}"
    testFoldConst("${const_sql_0_46}")
    qt_sql_0_47_non_strict "${const_sql_0_47}"
    testFoldConst("${const_sql_0_47}")
    qt_sql_0_48_non_strict "${const_sql_0_48}"
    testFoldConst("${const_sql_0_48}")
    qt_sql_0_49_non_strict "${const_sql_0_49}"
    testFoldConst("${const_sql_0_49}")
    qt_sql_0_50_non_strict "${const_sql_0_50}"
    testFoldConst("${const_sql_0_50}")
    qt_sql_0_51_non_strict "${const_sql_0_51}"
    testFoldConst("${const_sql_0_51}")
    qt_sql_0_52_non_strict "${const_sql_0_52}"
    testFoldConst("${const_sql_0_52}")
    qt_sql_0_53_non_strict "${const_sql_0_53}"
    testFoldConst("${const_sql_0_53}")
    qt_sql_0_54_non_strict "${const_sql_0_54}"
    testFoldConst("${const_sql_0_54}")
    qt_sql_0_55_non_strict "${const_sql_0_55}"
    testFoldConst("${const_sql_0_55}")
    qt_sql_0_56_non_strict "${const_sql_0_56}"
    testFoldConst("${const_sql_0_56}")
    qt_sql_0_57_non_strict "${const_sql_0_57}"
    testFoldConst("${const_sql_0_57}")
    qt_sql_0_58_non_strict "${const_sql_0_58}"
    testFoldConst("${const_sql_0_58}")
    qt_sql_0_59_non_strict "${const_sql_0_59}"
    testFoldConst("${const_sql_0_59}")
    qt_sql_0_60_non_strict "${const_sql_0_60}"
    testFoldConst("${const_sql_0_60}")
    qt_sql_0_61_non_strict "${const_sql_0_61}"
    testFoldConst("${const_sql_0_61}")
    qt_sql_0_62_non_strict "${const_sql_0_62}"
    testFoldConst("${const_sql_0_62}")
    qt_sql_0_63_non_strict "${const_sql_0_63}"
    testFoldConst("${const_sql_0_63}")
    qt_sql_0_64_non_strict "${const_sql_0_64}"
    testFoldConst("${const_sql_0_64}")
    qt_sql_0_65_non_strict "${const_sql_0_65}"
    testFoldConst("${const_sql_0_65}")
    qt_sql_0_66_non_strict "${const_sql_0_66}"
    testFoldConst("${const_sql_0_66}")
    qt_sql_0_67_non_strict "${const_sql_0_67}"
    testFoldConst("${const_sql_0_67}")
    qt_sql_0_68_non_strict "${const_sql_0_68}"
    testFoldConst("${const_sql_0_68}")
    qt_sql_0_69_non_strict "${const_sql_0_69}"
    testFoldConst("${const_sql_0_69}")
    qt_sql_0_70_non_strict "${const_sql_0_70}"
    testFoldConst("${const_sql_0_70}")
    qt_sql_0_71_non_strict "${const_sql_0_71}"
    testFoldConst("${const_sql_0_71}")
    qt_sql_0_72_non_strict "${const_sql_0_72}"
    testFoldConst("${const_sql_0_72}")
    qt_sql_0_73_non_strict "${const_sql_0_73}"
    testFoldConst("${const_sql_0_73}")
    qt_sql_0_74_non_strict "${const_sql_0_74}"
    testFoldConst("${const_sql_0_74}")
    qt_sql_0_75_non_strict "${const_sql_0_75}"
    testFoldConst("${const_sql_0_75}")
    qt_sql_0_76_non_strict "${const_sql_0_76}"
    testFoldConst("${const_sql_0_76}")
    qt_sql_0_77_non_strict "${const_sql_0_77}"
    testFoldConst("${const_sql_0_77}")
    qt_sql_0_78_non_strict "${const_sql_0_78}"
    testFoldConst("${const_sql_0_78}")
    qt_sql_0_79_non_strict "${const_sql_0_79}"
    testFoldConst("${const_sql_0_79}")
    qt_sql_0_80_non_strict "${const_sql_0_80}"
    testFoldConst("${const_sql_0_80}")
    qt_sql_0_81_non_strict "${const_sql_0_81}"
    testFoldConst("${const_sql_0_81}")
    qt_sql_0_82_non_strict "${const_sql_0_82}"
    testFoldConst("${const_sql_0_82}")
    qt_sql_0_83_non_strict "${const_sql_0_83}"
    testFoldConst("${const_sql_0_83}")
    qt_sql_0_84_non_strict "${const_sql_0_84}"
    testFoldConst("${const_sql_0_84}")
    qt_sql_0_85_non_strict "${const_sql_0_85}"
    testFoldConst("${const_sql_0_85}")
    qt_sql_0_86_non_strict "${const_sql_0_86}"
    testFoldConst("${const_sql_0_86}")
    qt_sql_0_87_non_strict "${const_sql_0_87}"
    testFoldConst("${const_sql_0_87}")
    qt_sql_0_88_non_strict "${const_sql_0_88}"
    testFoldConst("${const_sql_0_88}")
    qt_sql_0_89_non_strict "${const_sql_0_89}"
    testFoldConst("${const_sql_0_89}")
    qt_sql_0_90_non_strict "${const_sql_0_90}"
    testFoldConst("${const_sql_0_90}")
    qt_sql_0_91_non_strict "${const_sql_0_91}"
    testFoldConst("${const_sql_0_91}")
    qt_sql_0_92_non_strict "${const_sql_0_92}"
    testFoldConst("${const_sql_0_92}")
    qt_sql_0_93_non_strict "${const_sql_0_93}"
    testFoldConst("${const_sql_0_93}")
    qt_sql_0_94_non_strict "${const_sql_0_94}"
    testFoldConst("${const_sql_0_94}")
    qt_sql_0_95_non_strict "${const_sql_0_95}"
    testFoldConst("${const_sql_0_95}")
    qt_sql_0_96_non_strict "${const_sql_0_96}"
    testFoldConst("${const_sql_0_96}")
    qt_sql_0_97_non_strict "${const_sql_0_97}"
    testFoldConst("${const_sql_0_97}")
    qt_sql_0_98_non_strict "${const_sql_0_98}"
    testFoldConst("${const_sql_0_98}")
    qt_sql_0_99_non_strict "${const_sql_0_99}"
    testFoldConst("${const_sql_0_99}")
    qt_sql_0_100_non_strict "${const_sql_0_100}"
    testFoldConst("${const_sql_0_100}")
    qt_sql_0_101_non_strict "${const_sql_0_101}"
    testFoldConst("${const_sql_0_101}")
    qt_sql_0_102_non_strict "${const_sql_0_102}"
    testFoldConst("${const_sql_0_102}")
    qt_sql_0_103_non_strict "${const_sql_0_103}"
    testFoldConst("${const_sql_0_103}")
    qt_sql_0_104_non_strict "${const_sql_0_104}"
    testFoldConst("${const_sql_0_104}")
    qt_sql_0_105_non_strict "${const_sql_0_105}"
    testFoldConst("${const_sql_0_105}")
    qt_sql_0_106_non_strict "${const_sql_0_106}"
    testFoldConst("${const_sql_0_106}")
    qt_sql_0_107_non_strict "${const_sql_0_107}"
    testFoldConst("${const_sql_0_107}")
    qt_sql_0_108_non_strict "${const_sql_0_108}"
    testFoldConst("${const_sql_0_108}")
    qt_sql_0_109_non_strict "${const_sql_0_109}"
    testFoldConst("${const_sql_0_109}")
    qt_sql_0_110_non_strict "${const_sql_0_110}"
    testFoldConst("${const_sql_0_110}")
    qt_sql_0_111_non_strict "${const_sql_0_111}"
    testFoldConst("${const_sql_0_111}")
    qt_sql_0_112_non_strict "${const_sql_0_112}"
    testFoldConst("${const_sql_0_112}")
    qt_sql_0_113_non_strict "${const_sql_0_113}"
    testFoldConst("${const_sql_0_113}")
    qt_sql_0_114_non_strict "${const_sql_0_114}"
    testFoldConst("${const_sql_0_114}")
    qt_sql_0_115_non_strict "${const_sql_0_115}"
    testFoldConst("${const_sql_0_115}")
    qt_sql_0_116_non_strict "${const_sql_0_116}"
    testFoldConst("${const_sql_0_116}")
    qt_sql_0_117_non_strict "${const_sql_0_117}"
    testFoldConst("${const_sql_0_117}")
    qt_sql_0_118_non_strict "${const_sql_0_118}"
    testFoldConst("${const_sql_0_118}")
    qt_sql_0_119_non_strict "${const_sql_0_119}"
    testFoldConst("${const_sql_0_119}")
    qt_sql_0_120_non_strict "${const_sql_0_120}"
    testFoldConst("${const_sql_0_120}")
    qt_sql_0_121_non_strict "${const_sql_0_121}"
    testFoldConst("${const_sql_0_121}")
    qt_sql_0_122_non_strict "${const_sql_0_122}"
    testFoldConst("${const_sql_0_122}")
    qt_sql_0_123_non_strict "${const_sql_0_123}"
    testFoldConst("${const_sql_0_123}")
    qt_sql_0_124_non_strict "${const_sql_0_124}"
    testFoldConst("${const_sql_0_124}")
    qt_sql_0_125_non_strict "${const_sql_0_125}"
    testFoldConst("${const_sql_0_125}")
    qt_sql_0_126_non_strict "${const_sql_0_126}"
    testFoldConst("${const_sql_0_126}")
    qt_sql_0_127_non_strict "${const_sql_0_127}"
    testFoldConst("${const_sql_0_127}")
    qt_sql_0_128_non_strict "${const_sql_0_128}"
    testFoldConst("${const_sql_0_128}")
    qt_sql_0_129_non_strict "${const_sql_0_129}"
    testFoldConst("${const_sql_0_129}")
    qt_sql_0_130_non_strict "${const_sql_0_130}"
    testFoldConst("${const_sql_0_130}")
    qt_sql_0_131_non_strict "${const_sql_0_131}"
    testFoldConst("${const_sql_0_131}")
    qt_sql_0_132_non_strict "${const_sql_0_132}"
    testFoldConst("${const_sql_0_132}")
    qt_sql_0_133_non_strict "${const_sql_0_133}"
    testFoldConst("${const_sql_0_133}")
    qt_sql_0_134_non_strict "${const_sql_0_134}"
    testFoldConst("${const_sql_0_134}")
    qt_sql_0_135_non_strict "${const_sql_0_135}"
    testFoldConst("${const_sql_0_135}")
    qt_sql_0_136_non_strict "${const_sql_0_136}"
    testFoldConst("${const_sql_0_136}")
    qt_sql_0_137_non_strict "${const_sql_0_137}"
    testFoldConst("${const_sql_0_137}")
    qt_sql_0_138_non_strict "${const_sql_0_138}"
    testFoldConst("${const_sql_0_138}")
    qt_sql_0_139_non_strict "${const_sql_0_139}"
    testFoldConst("${const_sql_0_139}")
    qt_sql_0_140_non_strict "${const_sql_0_140}"
    testFoldConst("${const_sql_0_140}")
    qt_sql_0_141_non_strict "${const_sql_0_141}"
    testFoldConst("${const_sql_0_141}")
    qt_sql_0_142_non_strict "${const_sql_0_142}"
    testFoldConst("${const_sql_0_142}")
    qt_sql_0_143_non_strict "${const_sql_0_143}"
    testFoldConst("${const_sql_0_143}")
    qt_sql_0_144_non_strict "${const_sql_0_144}"
    testFoldConst("${const_sql_0_144}")
    qt_sql_0_145_non_strict "${const_sql_0_145}"
    testFoldConst("${const_sql_0_145}")
    qt_sql_0_146_non_strict "${const_sql_0_146}"
    testFoldConst("${const_sql_0_146}")
    qt_sql_0_147_non_strict "${const_sql_0_147}"
    testFoldConst("${const_sql_0_147}")
    qt_sql_0_148_non_strict "${const_sql_0_148}"
    testFoldConst("${const_sql_0_148}")
    qt_sql_0_149_non_strict "${const_sql_0_149}"
    testFoldConst("${const_sql_0_149}")
    qt_sql_0_150_non_strict "${const_sql_0_150}"
    testFoldConst("${const_sql_0_150}")
    qt_sql_0_151_non_strict "${const_sql_0_151}"
    testFoldConst("${const_sql_0_151}")
    qt_sql_0_152_non_strict "${const_sql_0_152}"
    testFoldConst("${const_sql_0_152}")
    qt_sql_0_153_non_strict "${const_sql_0_153}"
    testFoldConst("${const_sql_0_153}")
    qt_sql_0_154_non_strict "${const_sql_0_154}"
    testFoldConst("${const_sql_0_154}")
    qt_sql_0_155_non_strict "${const_sql_0_155}"
    testFoldConst("${const_sql_0_155}")
    qt_sql_0_156_non_strict "${const_sql_0_156}"
    testFoldConst("${const_sql_0_156}")
    qt_sql_0_157_non_strict "${const_sql_0_157}"
    testFoldConst("${const_sql_0_157}")
    qt_sql_0_158_non_strict "${const_sql_0_158}"
    testFoldConst("${const_sql_0_158}")
    qt_sql_0_159_non_strict "${const_sql_0_159}"
    testFoldConst("${const_sql_0_159}")
}