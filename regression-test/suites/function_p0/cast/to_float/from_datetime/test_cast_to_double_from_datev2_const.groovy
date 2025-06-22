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


suite("test_cast_to_double_from_datev2_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"

    sql "set enable_strict_cast=true;"
    def const_sql_0_0 = """select "0000-01-01", cast(cast("0000-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_0};"""
        exception ""
    }
    def const_sql_0_1 = """select "0000-01-02", cast(cast("0000-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_1};"""
        exception ""
    }
    def const_sql_0_2 = """select "0000-01-09", cast(cast("0000-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_2};"""
        exception ""
    }
    def const_sql_0_3 = """select "0000-01-10", cast(cast("0000-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_3};"""
        exception ""
    }
    def const_sql_0_4 = """select "0000-01-11", cast(cast("0000-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_4};"""
        exception ""
    }
    def const_sql_0_5 = """select "0000-01-28", cast(cast("0000-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_5};"""
        exception ""
    }
    def const_sql_0_6 = """select "0000-09-01", cast(cast("0000-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_6};"""
        exception ""
    }
    def const_sql_0_7 = """select "0000-09-02", cast(cast("0000-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_7};"""
        exception ""
    }
    def const_sql_0_8 = """select "0000-09-09", cast(cast("0000-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_8};"""
        exception ""
    }
    def const_sql_0_9 = """select "0000-09-10", cast(cast("0000-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_9};"""
        exception ""
    }
    def const_sql_0_10 = """select "0000-09-11", cast(cast("0000-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_10};"""
        exception ""
    }
    def const_sql_0_11 = """select "0000-09-28", cast(cast("0000-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_11};"""
        exception ""
    }
    def const_sql_0_12 = """select "0000-10-01", cast(cast("0000-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_12};"""
        exception ""
    }
    def const_sql_0_13 = """select "0000-10-02", cast(cast("0000-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_13};"""
        exception ""
    }
    def const_sql_0_14 = """select "0000-10-09", cast(cast("0000-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_14};"""
        exception ""
    }
    def const_sql_0_15 = """select "0000-10-10", cast(cast("0000-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_15};"""
        exception ""
    }
    def const_sql_0_16 = """select "0000-10-11", cast(cast("0000-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_16};"""
        exception ""
    }
    def const_sql_0_17 = """select "0000-10-28", cast(cast("0000-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_17};"""
        exception ""
    }
    def const_sql_0_18 = """select "0000-11-01", cast(cast("0000-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_18};"""
        exception ""
    }
    def const_sql_0_19 = """select "0000-11-02", cast(cast("0000-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_19};"""
        exception ""
    }
    def const_sql_0_20 = """select "0000-11-09", cast(cast("0000-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_20};"""
        exception ""
    }
    def const_sql_0_21 = """select "0000-11-10", cast(cast("0000-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_21};"""
        exception ""
    }
    def const_sql_0_22 = """select "0000-11-11", cast(cast("0000-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_22};"""
        exception ""
    }
    def const_sql_0_23 = """select "0000-11-28", cast(cast("0000-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_23};"""
        exception ""
    }
    def const_sql_0_24 = """select "0000-12-01", cast(cast("0000-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_24};"""
        exception ""
    }
    def const_sql_0_25 = """select "0000-12-02", cast(cast("0000-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_25};"""
        exception ""
    }
    def const_sql_0_26 = """select "0000-12-09", cast(cast("0000-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_26};"""
        exception ""
    }
    def const_sql_0_27 = """select "0000-12-10", cast(cast("0000-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_27};"""
        exception ""
    }
    def const_sql_0_28 = """select "0000-12-11", cast(cast("0000-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_28};"""
        exception ""
    }
    def const_sql_0_29 = """select "0000-12-28", cast(cast("0000-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_29};"""
        exception ""
    }
    def const_sql_0_30 = """select "0001-01-01", cast(cast("0001-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_30};"""
        exception ""
    }
    def const_sql_0_31 = """select "0001-01-02", cast(cast("0001-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_31};"""
        exception ""
    }
    def const_sql_0_32 = """select "0001-01-09", cast(cast("0001-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_32};"""
        exception ""
    }
    def const_sql_0_33 = """select "0001-01-10", cast(cast("0001-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_33};"""
        exception ""
    }
    def const_sql_0_34 = """select "0001-01-11", cast(cast("0001-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_34};"""
        exception ""
    }
    def const_sql_0_35 = """select "0001-01-28", cast(cast("0001-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_35};"""
        exception ""
    }
    def const_sql_0_36 = """select "0001-09-01", cast(cast("0001-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_36};"""
        exception ""
    }
    def const_sql_0_37 = """select "0001-09-02", cast(cast("0001-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_37};"""
        exception ""
    }
    def const_sql_0_38 = """select "0001-09-09", cast(cast("0001-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_38};"""
        exception ""
    }
    def const_sql_0_39 = """select "0001-09-10", cast(cast("0001-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_39};"""
        exception ""
    }
    def const_sql_0_40 = """select "0001-09-11", cast(cast("0001-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_40};"""
        exception ""
    }
    def const_sql_0_41 = """select "0001-09-28", cast(cast("0001-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_41};"""
        exception ""
    }
    def const_sql_0_42 = """select "0001-10-01", cast(cast("0001-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_42};"""
        exception ""
    }
    def const_sql_0_43 = """select "0001-10-02", cast(cast("0001-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_43};"""
        exception ""
    }
    def const_sql_0_44 = """select "0001-10-09", cast(cast("0001-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_44};"""
        exception ""
    }
    def const_sql_0_45 = """select "0001-10-10", cast(cast("0001-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_45};"""
        exception ""
    }
    def const_sql_0_46 = """select "0001-10-11", cast(cast("0001-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_46};"""
        exception ""
    }
    def const_sql_0_47 = """select "0001-10-28", cast(cast("0001-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_47};"""
        exception ""
    }
    def const_sql_0_48 = """select "0001-11-01", cast(cast("0001-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_48};"""
        exception ""
    }
    def const_sql_0_49 = """select "0001-11-02", cast(cast("0001-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_49};"""
        exception ""
    }
    def const_sql_0_50 = """select "0001-11-09", cast(cast("0001-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_50};"""
        exception ""
    }
    def const_sql_0_51 = """select "0001-11-10", cast(cast("0001-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_51};"""
        exception ""
    }
    def const_sql_0_52 = """select "0001-11-11", cast(cast("0001-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_52};"""
        exception ""
    }
    def const_sql_0_53 = """select "0001-11-28", cast(cast("0001-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_53};"""
        exception ""
    }
    def const_sql_0_54 = """select "0001-12-01", cast(cast("0001-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_54};"""
        exception ""
    }
    def const_sql_0_55 = """select "0001-12-02", cast(cast("0001-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_55};"""
        exception ""
    }
    def const_sql_0_56 = """select "0001-12-09", cast(cast("0001-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_56};"""
        exception ""
    }
    def const_sql_0_57 = """select "0001-12-10", cast(cast("0001-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_57};"""
        exception ""
    }
    def const_sql_0_58 = """select "0001-12-11", cast(cast("0001-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_58};"""
        exception ""
    }
    def const_sql_0_59 = """select "0001-12-28", cast(cast("0001-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_59};"""
        exception ""
    }
    def const_sql_0_60 = """select "0009-01-01", cast(cast("0009-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_60};"""
        exception ""
    }
    def const_sql_0_61 = """select "0009-01-02", cast(cast("0009-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_61};"""
        exception ""
    }
    def const_sql_0_62 = """select "0009-01-09", cast(cast("0009-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_62};"""
        exception ""
    }
    def const_sql_0_63 = """select "0009-01-10", cast(cast("0009-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_63};"""
        exception ""
    }
    def const_sql_0_64 = """select "0009-01-11", cast(cast("0009-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_64};"""
        exception ""
    }
    def const_sql_0_65 = """select "0009-01-28", cast(cast("0009-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_65};"""
        exception ""
    }
    def const_sql_0_66 = """select "0009-09-01", cast(cast("0009-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_66};"""
        exception ""
    }
    def const_sql_0_67 = """select "0009-09-02", cast(cast("0009-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_67};"""
        exception ""
    }
    def const_sql_0_68 = """select "0009-09-09", cast(cast("0009-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_68};"""
        exception ""
    }
    def const_sql_0_69 = """select "0009-09-10", cast(cast("0009-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_69};"""
        exception ""
    }
    def const_sql_0_70 = """select "0009-09-11", cast(cast("0009-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_70};"""
        exception ""
    }
    def const_sql_0_71 = """select "0009-09-28", cast(cast("0009-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_71};"""
        exception ""
    }
    def const_sql_0_72 = """select "0009-10-01", cast(cast("0009-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_72};"""
        exception ""
    }
    def const_sql_0_73 = """select "0009-10-02", cast(cast("0009-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_73};"""
        exception ""
    }
    def const_sql_0_74 = """select "0009-10-09", cast(cast("0009-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_74};"""
        exception ""
    }
    def const_sql_0_75 = """select "0009-10-10", cast(cast("0009-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_75};"""
        exception ""
    }
    def const_sql_0_76 = """select "0009-10-11", cast(cast("0009-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_76};"""
        exception ""
    }
    def const_sql_0_77 = """select "0009-10-28", cast(cast("0009-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_77};"""
        exception ""
    }
    def const_sql_0_78 = """select "0009-11-01", cast(cast("0009-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_78};"""
        exception ""
    }
    def const_sql_0_79 = """select "0009-11-02", cast(cast("0009-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_79};"""
        exception ""
    }
    def const_sql_0_80 = """select "0009-11-09", cast(cast("0009-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_80};"""
        exception ""
    }
    def const_sql_0_81 = """select "0009-11-10", cast(cast("0009-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_81};"""
        exception ""
    }
    def const_sql_0_82 = """select "0009-11-11", cast(cast("0009-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_82};"""
        exception ""
    }
    def const_sql_0_83 = """select "0009-11-28", cast(cast("0009-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_83};"""
        exception ""
    }
    def const_sql_0_84 = """select "0009-12-01", cast(cast("0009-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_84};"""
        exception ""
    }
    def const_sql_0_85 = """select "0009-12-02", cast(cast("0009-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_85};"""
        exception ""
    }
    def const_sql_0_86 = """select "0009-12-09", cast(cast("0009-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_86};"""
        exception ""
    }
    def const_sql_0_87 = """select "0009-12-10", cast(cast("0009-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_87};"""
        exception ""
    }
    def const_sql_0_88 = """select "0009-12-11", cast(cast("0009-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_88};"""
        exception ""
    }
    def const_sql_0_89 = """select "0009-12-28", cast(cast("0009-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_89};"""
        exception ""
    }
    def const_sql_0_90 = """select "0010-01-01", cast(cast("0010-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_90};"""
        exception ""
    }
    def const_sql_0_91 = """select "0010-01-02", cast(cast("0010-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_91};"""
        exception ""
    }
    def const_sql_0_92 = """select "0010-01-09", cast(cast("0010-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_92};"""
        exception ""
    }
    def const_sql_0_93 = """select "0010-01-10", cast(cast("0010-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_93};"""
        exception ""
    }
    def const_sql_0_94 = """select "0010-01-11", cast(cast("0010-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_94};"""
        exception ""
    }
    def const_sql_0_95 = """select "0010-01-28", cast(cast("0010-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_95};"""
        exception ""
    }
    def const_sql_0_96 = """select "0010-09-01", cast(cast("0010-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_96};"""
        exception ""
    }
    def const_sql_0_97 = """select "0010-09-02", cast(cast("0010-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_97};"""
        exception ""
    }
    def const_sql_0_98 = """select "0010-09-09", cast(cast("0010-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_98};"""
        exception ""
    }
    def const_sql_0_99 = """select "0010-09-10", cast(cast("0010-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_99};"""
        exception ""
    }
    def const_sql_0_100 = """select "0010-09-11", cast(cast("0010-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_100};"""
        exception ""
    }
    def const_sql_0_101 = """select "0010-09-28", cast(cast("0010-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_101};"""
        exception ""
    }
    def const_sql_0_102 = """select "0010-10-01", cast(cast("0010-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_102};"""
        exception ""
    }
    def const_sql_0_103 = """select "0010-10-02", cast(cast("0010-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_103};"""
        exception ""
    }
    def const_sql_0_104 = """select "0010-10-09", cast(cast("0010-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_104};"""
        exception ""
    }
    def const_sql_0_105 = """select "0010-10-10", cast(cast("0010-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_105};"""
        exception ""
    }
    def const_sql_0_106 = """select "0010-10-11", cast(cast("0010-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_106};"""
        exception ""
    }
    def const_sql_0_107 = """select "0010-10-28", cast(cast("0010-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_107};"""
        exception ""
    }
    def const_sql_0_108 = """select "0010-11-01", cast(cast("0010-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_108};"""
        exception ""
    }
    def const_sql_0_109 = """select "0010-11-02", cast(cast("0010-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_109};"""
        exception ""
    }
    def const_sql_0_110 = """select "0010-11-09", cast(cast("0010-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_110};"""
        exception ""
    }
    def const_sql_0_111 = """select "0010-11-10", cast(cast("0010-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_111};"""
        exception ""
    }
    def const_sql_0_112 = """select "0010-11-11", cast(cast("0010-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_112};"""
        exception ""
    }
    def const_sql_0_113 = """select "0010-11-28", cast(cast("0010-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_113};"""
        exception ""
    }
    def const_sql_0_114 = """select "0010-12-01", cast(cast("0010-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_114};"""
        exception ""
    }
    def const_sql_0_115 = """select "0010-12-02", cast(cast("0010-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_115};"""
        exception ""
    }
    def const_sql_0_116 = """select "0010-12-09", cast(cast("0010-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_116};"""
        exception ""
    }
    def const_sql_0_117 = """select "0010-12-10", cast(cast("0010-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_117};"""
        exception ""
    }
    def const_sql_0_118 = """select "0010-12-11", cast(cast("0010-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_118};"""
        exception ""
    }
    def const_sql_0_119 = """select "0010-12-28", cast(cast("0010-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_119};"""
        exception ""
    }
    def const_sql_0_120 = """select "0011-01-01", cast(cast("0011-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_120};"""
        exception ""
    }
    def const_sql_0_121 = """select "0011-01-02", cast(cast("0011-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_121};"""
        exception ""
    }
    def const_sql_0_122 = """select "0011-01-09", cast(cast("0011-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_122};"""
        exception ""
    }
    def const_sql_0_123 = """select "0011-01-10", cast(cast("0011-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_123};"""
        exception ""
    }
    def const_sql_0_124 = """select "0011-01-11", cast(cast("0011-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_124};"""
        exception ""
    }
    def const_sql_0_125 = """select "0011-01-28", cast(cast("0011-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_125};"""
        exception ""
    }
    def const_sql_0_126 = """select "0011-09-01", cast(cast("0011-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_126};"""
        exception ""
    }
    def const_sql_0_127 = """select "0011-09-02", cast(cast("0011-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_127};"""
        exception ""
    }
    def const_sql_0_128 = """select "0011-09-09", cast(cast("0011-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_128};"""
        exception ""
    }
    def const_sql_0_129 = """select "0011-09-10", cast(cast("0011-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_129};"""
        exception ""
    }
    def const_sql_0_130 = """select "0011-09-11", cast(cast("0011-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_130};"""
        exception ""
    }
    def const_sql_0_131 = """select "0011-09-28", cast(cast("0011-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_131};"""
        exception ""
    }
    def const_sql_0_132 = """select "0011-10-01", cast(cast("0011-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_132};"""
        exception ""
    }
    def const_sql_0_133 = """select "0011-10-02", cast(cast("0011-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_133};"""
        exception ""
    }
    def const_sql_0_134 = """select "0011-10-09", cast(cast("0011-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_134};"""
        exception ""
    }
    def const_sql_0_135 = """select "0011-10-10", cast(cast("0011-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_135};"""
        exception ""
    }
    def const_sql_0_136 = """select "0011-10-11", cast(cast("0011-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_136};"""
        exception ""
    }
    def const_sql_0_137 = """select "0011-10-28", cast(cast("0011-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_137};"""
        exception ""
    }
    def const_sql_0_138 = """select "0011-11-01", cast(cast("0011-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_138};"""
        exception ""
    }
    def const_sql_0_139 = """select "0011-11-02", cast(cast("0011-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_139};"""
        exception ""
    }
    def const_sql_0_140 = """select "0011-11-09", cast(cast("0011-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_140};"""
        exception ""
    }
    def const_sql_0_141 = """select "0011-11-10", cast(cast("0011-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_141};"""
        exception ""
    }
    def const_sql_0_142 = """select "0011-11-11", cast(cast("0011-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_142};"""
        exception ""
    }
    def const_sql_0_143 = """select "0011-11-28", cast(cast("0011-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_143};"""
        exception ""
    }
    def const_sql_0_144 = """select "0011-12-01", cast(cast("0011-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_144};"""
        exception ""
    }
    def const_sql_0_145 = """select "0011-12-02", cast(cast("0011-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_145};"""
        exception ""
    }
    def const_sql_0_146 = """select "0011-12-09", cast(cast("0011-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_146};"""
        exception ""
    }
    def const_sql_0_147 = """select "0011-12-10", cast(cast("0011-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_147};"""
        exception ""
    }
    def const_sql_0_148 = """select "0011-12-11", cast(cast("0011-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_148};"""
        exception ""
    }
    def const_sql_0_149 = """select "0011-12-28", cast(cast("0011-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_149};"""
        exception ""
    }
    def const_sql_0_150 = """select "0099-01-01", cast(cast("0099-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_150};"""
        exception ""
    }
    def const_sql_0_151 = """select "0099-01-02", cast(cast("0099-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_151};"""
        exception ""
    }
    def const_sql_0_152 = """select "0099-01-09", cast(cast("0099-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_152};"""
        exception ""
    }
    def const_sql_0_153 = """select "0099-01-10", cast(cast("0099-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_153};"""
        exception ""
    }
    def const_sql_0_154 = """select "0099-01-11", cast(cast("0099-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_154};"""
        exception ""
    }
    def const_sql_0_155 = """select "0099-01-28", cast(cast("0099-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_155};"""
        exception ""
    }
    def const_sql_0_156 = """select "0099-09-01", cast(cast("0099-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_156};"""
        exception ""
    }
    def const_sql_0_157 = """select "0099-09-02", cast(cast("0099-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_157};"""
        exception ""
    }
    def const_sql_0_158 = """select "0099-09-09", cast(cast("0099-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_158};"""
        exception ""
    }
    def const_sql_0_159 = """select "0099-09-10", cast(cast("0099-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_159};"""
        exception ""
    }
    def const_sql_0_160 = """select "0099-09-11", cast(cast("0099-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_160};"""
        exception ""
    }
    def const_sql_0_161 = """select "0099-09-28", cast(cast("0099-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_161};"""
        exception ""
    }
    def const_sql_0_162 = """select "0099-10-01", cast(cast("0099-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_162};"""
        exception ""
    }
    def const_sql_0_163 = """select "0099-10-02", cast(cast("0099-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_163};"""
        exception ""
    }
    def const_sql_0_164 = """select "0099-10-09", cast(cast("0099-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_164};"""
        exception ""
    }
    def const_sql_0_165 = """select "0099-10-10", cast(cast("0099-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_165};"""
        exception ""
    }
    def const_sql_0_166 = """select "0099-10-11", cast(cast("0099-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_166};"""
        exception ""
    }
    def const_sql_0_167 = """select "0099-10-28", cast(cast("0099-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_167};"""
        exception ""
    }
    def const_sql_0_168 = """select "0099-11-01", cast(cast("0099-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_168};"""
        exception ""
    }
    def const_sql_0_169 = """select "0099-11-02", cast(cast("0099-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_169};"""
        exception ""
    }
    def const_sql_0_170 = """select "0099-11-09", cast(cast("0099-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_170};"""
        exception ""
    }
    def const_sql_0_171 = """select "0099-11-10", cast(cast("0099-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_171};"""
        exception ""
    }
    def const_sql_0_172 = """select "0099-11-11", cast(cast("0099-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_172};"""
        exception ""
    }
    def const_sql_0_173 = """select "0099-11-28", cast(cast("0099-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_173};"""
        exception ""
    }
    def const_sql_0_174 = """select "0099-12-01", cast(cast("0099-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_174};"""
        exception ""
    }
    def const_sql_0_175 = """select "0099-12-02", cast(cast("0099-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_175};"""
        exception ""
    }
    def const_sql_0_176 = """select "0099-12-09", cast(cast("0099-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_176};"""
        exception ""
    }
    def const_sql_0_177 = """select "0099-12-10", cast(cast("0099-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_177};"""
        exception ""
    }
    def const_sql_0_178 = """select "0099-12-11", cast(cast("0099-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_178};"""
        exception ""
    }
    def const_sql_0_179 = """select "0099-12-28", cast(cast("0099-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_179};"""
        exception ""
    }
    def const_sql_0_180 = """select "0100-01-01", cast(cast("0100-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_180};"""
        exception ""
    }
    def const_sql_0_181 = """select "0100-01-02", cast(cast("0100-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_181};"""
        exception ""
    }
    def const_sql_0_182 = """select "0100-01-09", cast(cast("0100-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_182};"""
        exception ""
    }
    def const_sql_0_183 = """select "0100-01-10", cast(cast("0100-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_183};"""
        exception ""
    }
    def const_sql_0_184 = """select "0100-01-11", cast(cast("0100-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_184};"""
        exception ""
    }
    def const_sql_0_185 = """select "0100-01-28", cast(cast("0100-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_185};"""
        exception ""
    }
    def const_sql_0_186 = """select "0100-09-01", cast(cast("0100-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_186};"""
        exception ""
    }
    def const_sql_0_187 = """select "0100-09-02", cast(cast("0100-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_187};"""
        exception ""
    }
    def const_sql_0_188 = """select "0100-09-09", cast(cast("0100-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_188};"""
        exception ""
    }
    def const_sql_0_189 = """select "0100-09-10", cast(cast("0100-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_189};"""
        exception ""
    }
    def const_sql_0_190 = """select "0100-09-11", cast(cast("0100-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_190};"""
        exception ""
    }
    def const_sql_0_191 = """select "0100-09-28", cast(cast("0100-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_191};"""
        exception ""
    }
    def const_sql_0_192 = """select "0100-10-01", cast(cast("0100-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_192};"""
        exception ""
    }
    def const_sql_0_193 = """select "0100-10-02", cast(cast("0100-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_193};"""
        exception ""
    }
    def const_sql_0_194 = """select "0100-10-09", cast(cast("0100-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_194};"""
        exception ""
    }
    def const_sql_0_195 = """select "0100-10-10", cast(cast("0100-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_195};"""
        exception ""
    }
    def const_sql_0_196 = """select "0100-10-11", cast(cast("0100-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_196};"""
        exception ""
    }
    def const_sql_0_197 = """select "0100-10-28", cast(cast("0100-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_197};"""
        exception ""
    }
    def const_sql_0_198 = """select "0100-11-01", cast(cast("0100-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_198};"""
        exception ""
    }
    def const_sql_0_199 = """select "0100-11-02", cast(cast("0100-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_199};"""
        exception ""
    }
    def const_sql_0_200 = """select "0100-11-09", cast(cast("0100-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_200};"""
        exception ""
    }
    def const_sql_0_201 = """select "0100-11-10", cast(cast("0100-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_201};"""
        exception ""
    }
    def const_sql_0_202 = """select "0100-11-11", cast(cast("0100-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_202};"""
        exception ""
    }
    def const_sql_0_203 = """select "0100-11-28", cast(cast("0100-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_203};"""
        exception ""
    }
    def const_sql_0_204 = """select "0100-12-01", cast(cast("0100-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_204};"""
        exception ""
    }
    def const_sql_0_205 = """select "0100-12-02", cast(cast("0100-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_205};"""
        exception ""
    }
    def const_sql_0_206 = """select "0100-12-09", cast(cast("0100-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_206};"""
        exception ""
    }
    def const_sql_0_207 = """select "0100-12-10", cast(cast("0100-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_207};"""
        exception ""
    }
    def const_sql_0_208 = """select "0100-12-11", cast(cast("0100-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_208};"""
        exception ""
    }
    def const_sql_0_209 = """select "0100-12-28", cast(cast("0100-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_209};"""
        exception ""
    }
    def const_sql_0_210 = """select "0101-01-01", cast(cast("0101-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_210};"""
        exception ""
    }
    def const_sql_0_211 = """select "0101-01-02", cast(cast("0101-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_211};"""
        exception ""
    }
    def const_sql_0_212 = """select "0101-01-09", cast(cast("0101-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_212};"""
        exception ""
    }
    def const_sql_0_213 = """select "0101-01-10", cast(cast("0101-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_213};"""
        exception ""
    }
    def const_sql_0_214 = """select "0101-01-11", cast(cast("0101-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_214};"""
        exception ""
    }
    def const_sql_0_215 = """select "0101-01-28", cast(cast("0101-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_215};"""
        exception ""
    }
    def const_sql_0_216 = """select "0101-09-01", cast(cast("0101-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_216};"""
        exception ""
    }
    def const_sql_0_217 = """select "0101-09-02", cast(cast("0101-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_217};"""
        exception ""
    }
    def const_sql_0_218 = """select "0101-09-09", cast(cast("0101-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_218};"""
        exception ""
    }
    def const_sql_0_219 = """select "0101-09-10", cast(cast("0101-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_219};"""
        exception ""
    }
    def const_sql_0_220 = """select "0101-09-11", cast(cast("0101-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_220};"""
        exception ""
    }
    def const_sql_0_221 = """select "0101-09-28", cast(cast("0101-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_221};"""
        exception ""
    }
    def const_sql_0_222 = """select "0101-10-01", cast(cast("0101-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_222};"""
        exception ""
    }
    def const_sql_0_223 = """select "0101-10-02", cast(cast("0101-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_223};"""
        exception ""
    }
    def const_sql_0_224 = """select "0101-10-09", cast(cast("0101-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_224};"""
        exception ""
    }
    def const_sql_0_225 = """select "0101-10-10", cast(cast("0101-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_225};"""
        exception ""
    }
    def const_sql_0_226 = """select "0101-10-11", cast(cast("0101-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_226};"""
        exception ""
    }
    def const_sql_0_227 = """select "0101-10-28", cast(cast("0101-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_227};"""
        exception ""
    }
    def const_sql_0_228 = """select "0101-11-01", cast(cast("0101-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_228};"""
        exception ""
    }
    def const_sql_0_229 = """select "0101-11-02", cast(cast("0101-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_229};"""
        exception ""
    }
    def const_sql_0_230 = """select "0101-11-09", cast(cast("0101-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_230};"""
        exception ""
    }
    def const_sql_0_231 = """select "0101-11-10", cast(cast("0101-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_231};"""
        exception ""
    }
    def const_sql_0_232 = """select "0101-11-11", cast(cast("0101-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_232};"""
        exception ""
    }
    def const_sql_0_233 = """select "0101-11-28", cast(cast("0101-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_233};"""
        exception ""
    }
    def const_sql_0_234 = """select "0101-12-01", cast(cast("0101-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_234};"""
        exception ""
    }
    def const_sql_0_235 = """select "0101-12-02", cast(cast("0101-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_235};"""
        exception ""
    }
    def const_sql_0_236 = """select "0101-12-09", cast(cast("0101-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_236};"""
        exception ""
    }
    def const_sql_0_237 = """select "0101-12-10", cast(cast("0101-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_237};"""
        exception ""
    }
    def const_sql_0_238 = """select "0101-12-11", cast(cast("0101-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_238};"""
        exception ""
    }
    def const_sql_0_239 = """select "0101-12-28", cast(cast("0101-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_239};"""
        exception ""
    }
    def const_sql_0_240 = """select "0999-01-01", cast(cast("0999-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_240};"""
        exception ""
    }
    def const_sql_0_241 = """select "0999-01-02", cast(cast("0999-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_241};"""
        exception ""
    }
    def const_sql_0_242 = """select "0999-01-09", cast(cast("0999-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_242};"""
        exception ""
    }
    def const_sql_0_243 = """select "0999-01-10", cast(cast("0999-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_243};"""
        exception ""
    }
    def const_sql_0_244 = """select "0999-01-11", cast(cast("0999-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_244};"""
        exception ""
    }
    def const_sql_0_245 = """select "0999-01-28", cast(cast("0999-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_245};"""
        exception ""
    }
    def const_sql_0_246 = """select "0999-09-01", cast(cast("0999-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_246};"""
        exception ""
    }
    def const_sql_0_247 = """select "0999-09-02", cast(cast("0999-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_247};"""
        exception ""
    }
    def const_sql_0_248 = """select "0999-09-09", cast(cast("0999-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_248};"""
        exception ""
    }
    def const_sql_0_249 = """select "0999-09-10", cast(cast("0999-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_249};"""
        exception ""
    }
    def const_sql_0_250 = """select "0999-09-11", cast(cast("0999-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_250};"""
        exception ""
    }
    def const_sql_0_251 = """select "0999-09-28", cast(cast("0999-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_251};"""
        exception ""
    }
    def const_sql_0_252 = """select "0999-10-01", cast(cast("0999-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_252};"""
        exception ""
    }
    def const_sql_0_253 = """select "0999-10-02", cast(cast("0999-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_253};"""
        exception ""
    }
    def const_sql_0_254 = """select "0999-10-09", cast(cast("0999-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_254};"""
        exception ""
    }
    def const_sql_0_255 = """select "0999-10-10", cast(cast("0999-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_255};"""
        exception ""
    }
    def const_sql_0_256 = """select "0999-10-11", cast(cast("0999-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_256};"""
        exception ""
    }
    def const_sql_0_257 = """select "0999-10-28", cast(cast("0999-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_257};"""
        exception ""
    }
    def const_sql_0_258 = """select "0999-11-01", cast(cast("0999-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_258};"""
        exception ""
    }
    def const_sql_0_259 = """select "0999-11-02", cast(cast("0999-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_259};"""
        exception ""
    }
    def const_sql_0_260 = """select "0999-11-09", cast(cast("0999-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_260};"""
        exception ""
    }
    def const_sql_0_261 = """select "0999-11-10", cast(cast("0999-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_261};"""
        exception ""
    }
    def const_sql_0_262 = """select "0999-11-11", cast(cast("0999-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_262};"""
        exception ""
    }
    def const_sql_0_263 = """select "0999-11-28", cast(cast("0999-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_263};"""
        exception ""
    }
    def const_sql_0_264 = """select "0999-12-01", cast(cast("0999-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_264};"""
        exception ""
    }
    def const_sql_0_265 = """select "0999-12-02", cast(cast("0999-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_265};"""
        exception ""
    }
    def const_sql_0_266 = """select "0999-12-09", cast(cast("0999-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_266};"""
        exception ""
    }
    def const_sql_0_267 = """select "0999-12-10", cast(cast("0999-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_267};"""
        exception ""
    }
    def const_sql_0_268 = """select "0999-12-11", cast(cast("0999-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_268};"""
        exception ""
    }
    def const_sql_0_269 = """select "0999-12-28", cast(cast("0999-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_269};"""
        exception ""
    }
    def const_sql_0_270 = """select "1000-01-01", cast(cast("1000-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_270};"""
        exception ""
    }
    def const_sql_0_271 = """select "1000-01-02", cast(cast("1000-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_271};"""
        exception ""
    }
    def const_sql_0_272 = """select "1000-01-09", cast(cast("1000-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_272};"""
        exception ""
    }
    def const_sql_0_273 = """select "1000-01-10", cast(cast("1000-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_273};"""
        exception ""
    }
    def const_sql_0_274 = """select "1000-01-11", cast(cast("1000-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_274};"""
        exception ""
    }
    def const_sql_0_275 = """select "1000-01-28", cast(cast("1000-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_275};"""
        exception ""
    }
    def const_sql_0_276 = """select "1000-09-01", cast(cast("1000-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_276};"""
        exception ""
    }
    def const_sql_0_277 = """select "1000-09-02", cast(cast("1000-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_277};"""
        exception ""
    }
    def const_sql_0_278 = """select "1000-09-09", cast(cast("1000-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_278};"""
        exception ""
    }
    def const_sql_0_279 = """select "1000-09-10", cast(cast("1000-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_279};"""
        exception ""
    }
    def const_sql_0_280 = """select "1000-09-11", cast(cast("1000-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_280};"""
        exception ""
    }
    def const_sql_0_281 = """select "1000-09-28", cast(cast("1000-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_281};"""
        exception ""
    }
    def const_sql_0_282 = """select "1000-10-01", cast(cast("1000-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_282};"""
        exception ""
    }
    def const_sql_0_283 = """select "1000-10-02", cast(cast("1000-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_283};"""
        exception ""
    }
    def const_sql_0_284 = """select "1000-10-09", cast(cast("1000-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_284};"""
        exception ""
    }
    def const_sql_0_285 = """select "1000-10-10", cast(cast("1000-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_285};"""
        exception ""
    }
    def const_sql_0_286 = """select "1000-10-11", cast(cast("1000-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_286};"""
        exception ""
    }
    def const_sql_0_287 = """select "1000-10-28", cast(cast("1000-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_287};"""
        exception ""
    }
    def const_sql_0_288 = """select "1000-11-01", cast(cast("1000-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_288};"""
        exception ""
    }
    def const_sql_0_289 = """select "1000-11-02", cast(cast("1000-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_289};"""
        exception ""
    }
    def const_sql_0_290 = """select "1000-11-09", cast(cast("1000-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_290};"""
        exception ""
    }
    def const_sql_0_291 = """select "1000-11-10", cast(cast("1000-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_291};"""
        exception ""
    }
    def const_sql_0_292 = """select "1000-11-11", cast(cast("1000-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_292};"""
        exception ""
    }
    def const_sql_0_293 = """select "1000-11-28", cast(cast("1000-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_293};"""
        exception ""
    }
    def const_sql_0_294 = """select "1000-12-01", cast(cast("1000-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_294};"""
        exception ""
    }
    def const_sql_0_295 = """select "1000-12-02", cast(cast("1000-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_295};"""
        exception ""
    }
    def const_sql_0_296 = """select "1000-12-09", cast(cast("1000-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_296};"""
        exception ""
    }
    def const_sql_0_297 = """select "1000-12-10", cast(cast("1000-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_297};"""
        exception ""
    }
    def const_sql_0_298 = """select "1000-12-11", cast(cast("1000-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_298};"""
        exception ""
    }
    def const_sql_0_299 = """select "1000-12-28", cast(cast("1000-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_299};"""
        exception ""
    }
    def const_sql_0_300 = """select "1001-01-01", cast(cast("1001-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_300};"""
        exception ""
    }
    def const_sql_0_301 = """select "1001-01-02", cast(cast("1001-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_301};"""
        exception ""
    }
    def const_sql_0_302 = """select "1001-01-09", cast(cast("1001-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_302};"""
        exception ""
    }
    def const_sql_0_303 = """select "1001-01-10", cast(cast("1001-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_303};"""
        exception ""
    }
    def const_sql_0_304 = """select "1001-01-11", cast(cast("1001-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_304};"""
        exception ""
    }
    def const_sql_0_305 = """select "1001-01-28", cast(cast("1001-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_305};"""
        exception ""
    }
    def const_sql_0_306 = """select "1001-09-01", cast(cast("1001-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_306};"""
        exception ""
    }
    def const_sql_0_307 = """select "1001-09-02", cast(cast("1001-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_307};"""
        exception ""
    }
    def const_sql_0_308 = """select "1001-09-09", cast(cast("1001-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_308};"""
        exception ""
    }
    def const_sql_0_309 = """select "1001-09-10", cast(cast("1001-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_309};"""
        exception ""
    }
    def const_sql_0_310 = """select "1001-09-11", cast(cast("1001-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_310};"""
        exception ""
    }
    def const_sql_0_311 = """select "1001-09-28", cast(cast("1001-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_311};"""
        exception ""
    }
    def const_sql_0_312 = """select "1001-10-01", cast(cast("1001-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_312};"""
        exception ""
    }
    def const_sql_0_313 = """select "1001-10-02", cast(cast("1001-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_313};"""
        exception ""
    }
    def const_sql_0_314 = """select "1001-10-09", cast(cast("1001-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_314};"""
        exception ""
    }
    def const_sql_0_315 = """select "1001-10-10", cast(cast("1001-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_315};"""
        exception ""
    }
    def const_sql_0_316 = """select "1001-10-11", cast(cast("1001-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_316};"""
        exception ""
    }
    def const_sql_0_317 = """select "1001-10-28", cast(cast("1001-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_317};"""
        exception ""
    }
    def const_sql_0_318 = """select "1001-11-01", cast(cast("1001-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_318};"""
        exception ""
    }
    def const_sql_0_319 = """select "1001-11-02", cast(cast("1001-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_319};"""
        exception ""
    }
    def const_sql_0_320 = """select "1001-11-09", cast(cast("1001-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_320};"""
        exception ""
    }
    def const_sql_0_321 = """select "1001-11-10", cast(cast("1001-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_321};"""
        exception ""
    }
    def const_sql_0_322 = """select "1001-11-11", cast(cast("1001-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_322};"""
        exception ""
    }
    def const_sql_0_323 = """select "1001-11-28", cast(cast("1001-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_323};"""
        exception ""
    }
    def const_sql_0_324 = """select "1001-12-01", cast(cast("1001-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_324};"""
        exception ""
    }
    def const_sql_0_325 = """select "1001-12-02", cast(cast("1001-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_325};"""
        exception ""
    }
    def const_sql_0_326 = """select "1001-12-09", cast(cast("1001-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_326};"""
        exception ""
    }
    def const_sql_0_327 = """select "1001-12-10", cast(cast("1001-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_327};"""
        exception ""
    }
    def const_sql_0_328 = """select "1001-12-11", cast(cast("1001-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_328};"""
        exception ""
    }
    def const_sql_0_329 = """select "1001-12-28", cast(cast("1001-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_329};"""
        exception ""
    }
    def const_sql_0_330 = """select "1999-01-01", cast(cast("1999-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_330};"""
        exception ""
    }
    def const_sql_0_331 = """select "1999-01-02", cast(cast("1999-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_331};"""
        exception ""
    }
    def const_sql_0_332 = """select "1999-01-09", cast(cast("1999-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_332};"""
        exception ""
    }
    def const_sql_0_333 = """select "1999-01-10", cast(cast("1999-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_333};"""
        exception ""
    }
    def const_sql_0_334 = """select "1999-01-11", cast(cast("1999-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_334};"""
        exception ""
    }
    def const_sql_0_335 = """select "1999-01-28", cast(cast("1999-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_335};"""
        exception ""
    }
    def const_sql_0_336 = """select "1999-09-01", cast(cast("1999-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_336};"""
        exception ""
    }
    def const_sql_0_337 = """select "1999-09-02", cast(cast("1999-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_337};"""
        exception ""
    }
    def const_sql_0_338 = """select "1999-09-09", cast(cast("1999-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_338};"""
        exception ""
    }
    def const_sql_0_339 = """select "1999-09-10", cast(cast("1999-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_339};"""
        exception ""
    }
    def const_sql_0_340 = """select "1999-09-11", cast(cast("1999-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_340};"""
        exception ""
    }
    def const_sql_0_341 = """select "1999-09-28", cast(cast("1999-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_341};"""
        exception ""
    }
    def const_sql_0_342 = """select "1999-10-01", cast(cast("1999-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_342};"""
        exception ""
    }
    def const_sql_0_343 = """select "1999-10-02", cast(cast("1999-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_343};"""
        exception ""
    }
    def const_sql_0_344 = """select "1999-10-09", cast(cast("1999-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_344};"""
        exception ""
    }
    def const_sql_0_345 = """select "1999-10-10", cast(cast("1999-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_345};"""
        exception ""
    }
    def const_sql_0_346 = """select "1999-10-11", cast(cast("1999-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_346};"""
        exception ""
    }
    def const_sql_0_347 = """select "1999-10-28", cast(cast("1999-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_347};"""
        exception ""
    }
    def const_sql_0_348 = """select "1999-11-01", cast(cast("1999-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_348};"""
        exception ""
    }
    def const_sql_0_349 = """select "1999-11-02", cast(cast("1999-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_349};"""
        exception ""
    }
    def const_sql_0_350 = """select "1999-11-09", cast(cast("1999-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_350};"""
        exception ""
    }
    def const_sql_0_351 = """select "1999-11-10", cast(cast("1999-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_351};"""
        exception ""
    }
    def const_sql_0_352 = """select "1999-11-11", cast(cast("1999-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_352};"""
        exception ""
    }
    def const_sql_0_353 = """select "1999-11-28", cast(cast("1999-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_353};"""
        exception ""
    }
    def const_sql_0_354 = """select "1999-12-01", cast(cast("1999-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_354};"""
        exception ""
    }
    def const_sql_0_355 = """select "1999-12-02", cast(cast("1999-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_355};"""
        exception ""
    }
    def const_sql_0_356 = """select "1999-12-09", cast(cast("1999-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_356};"""
        exception ""
    }
    def const_sql_0_357 = """select "1999-12-10", cast(cast("1999-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_357};"""
        exception ""
    }
    def const_sql_0_358 = """select "1999-12-11", cast(cast("1999-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_358};"""
        exception ""
    }
    def const_sql_0_359 = """select "1999-12-28", cast(cast("1999-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_359};"""
        exception ""
    }
    def const_sql_0_360 = """select "2000-01-01", cast(cast("2000-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_360};"""
        exception ""
    }
    def const_sql_0_361 = """select "2000-01-02", cast(cast("2000-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_361};"""
        exception ""
    }
    def const_sql_0_362 = """select "2000-01-09", cast(cast("2000-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_362};"""
        exception ""
    }
    def const_sql_0_363 = """select "2000-01-10", cast(cast("2000-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_363};"""
        exception ""
    }
    def const_sql_0_364 = """select "2000-01-11", cast(cast("2000-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_364};"""
        exception ""
    }
    def const_sql_0_365 = """select "2000-01-28", cast(cast("2000-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_365};"""
        exception ""
    }
    def const_sql_0_366 = """select "2000-09-01", cast(cast("2000-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_366};"""
        exception ""
    }
    def const_sql_0_367 = """select "2000-09-02", cast(cast("2000-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_367};"""
        exception ""
    }
    def const_sql_0_368 = """select "2000-09-09", cast(cast("2000-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_368};"""
        exception ""
    }
    def const_sql_0_369 = """select "2000-09-10", cast(cast("2000-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_369};"""
        exception ""
    }
    def const_sql_0_370 = """select "2000-09-11", cast(cast("2000-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_370};"""
        exception ""
    }
    def const_sql_0_371 = """select "2000-09-28", cast(cast("2000-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_371};"""
        exception ""
    }
    def const_sql_0_372 = """select "2000-10-01", cast(cast("2000-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_372};"""
        exception ""
    }
    def const_sql_0_373 = """select "2000-10-02", cast(cast("2000-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_373};"""
        exception ""
    }
    def const_sql_0_374 = """select "2000-10-09", cast(cast("2000-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_374};"""
        exception ""
    }
    def const_sql_0_375 = """select "2000-10-10", cast(cast("2000-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_375};"""
        exception ""
    }
    def const_sql_0_376 = """select "2000-10-11", cast(cast("2000-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_376};"""
        exception ""
    }
    def const_sql_0_377 = """select "2000-10-28", cast(cast("2000-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_377};"""
        exception ""
    }
    def const_sql_0_378 = """select "2000-11-01", cast(cast("2000-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_378};"""
        exception ""
    }
    def const_sql_0_379 = """select "2000-11-02", cast(cast("2000-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_379};"""
        exception ""
    }
    def const_sql_0_380 = """select "2000-11-09", cast(cast("2000-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_380};"""
        exception ""
    }
    def const_sql_0_381 = """select "2000-11-10", cast(cast("2000-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_381};"""
        exception ""
    }
    def const_sql_0_382 = """select "2000-11-11", cast(cast("2000-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_382};"""
        exception ""
    }
    def const_sql_0_383 = """select "2000-11-28", cast(cast("2000-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_383};"""
        exception ""
    }
    def const_sql_0_384 = """select "2000-12-01", cast(cast("2000-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_384};"""
        exception ""
    }
    def const_sql_0_385 = """select "2000-12-02", cast(cast("2000-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_385};"""
        exception ""
    }
    def const_sql_0_386 = """select "2000-12-09", cast(cast("2000-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_386};"""
        exception ""
    }
    def const_sql_0_387 = """select "2000-12-10", cast(cast("2000-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_387};"""
        exception ""
    }
    def const_sql_0_388 = """select "2000-12-11", cast(cast("2000-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_388};"""
        exception ""
    }
    def const_sql_0_389 = """select "2000-12-28", cast(cast("2000-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_389};"""
        exception ""
    }
    def const_sql_0_390 = """select "2024-01-01", cast(cast("2024-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_390};"""
        exception ""
    }
    def const_sql_0_391 = """select "2024-01-02", cast(cast("2024-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_391};"""
        exception ""
    }
    def const_sql_0_392 = """select "2024-01-09", cast(cast("2024-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_392};"""
        exception ""
    }
    def const_sql_0_393 = """select "2024-01-10", cast(cast("2024-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_393};"""
        exception ""
    }
    def const_sql_0_394 = """select "2024-01-11", cast(cast("2024-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_394};"""
        exception ""
    }
    def const_sql_0_395 = """select "2024-01-28", cast(cast("2024-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_395};"""
        exception ""
    }
    def const_sql_0_396 = """select "2024-09-01", cast(cast("2024-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_396};"""
        exception ""
    }
    def const_sql_0_397 = """select "2024-09-02", cast(cast("2024-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_397};"""
        exception ""
    }
    def const_sql_0_398 = """select "2024-09-09", cast(cast("2024-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_398};"""
        exception ""
    }
    def const_sql_0_399 = """select "2024-09-10", cast(cast("2024-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_399};"""
        exception ""
    }
    def const_sql_0_400 = """select "2024-09-11", cast(cast("2024-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_400};"""
        exception ""
    }
    def const_sql_0_401 = """select "2024-09-28", cast(cast("2024-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_401};"""
        exception ""
    }
    def const_sql_0_402 = """select "2024-10-01", cast(cast("2024-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_402};"""
        exception ""
    }
    def const_sql_0_403 = """select "2024-10-02", cast(cast("2024-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_403};"""
        exception ""
    }
    def const_sql_0_404 = """select "2024-10-09", cast(cast("2024-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_404};"""
        exception ""
    }
    def const_sql_0_405 = """select "2024-10-10", cast(cast("2024-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_405};"""
        exception ""
    }
    def const_sql_0_406 = """select "2024-10-11", cast(cast("2024-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_406};"""
        exception ""
    }
    def const_sql_0_407 = """select "2024-10-28", cast(cast("2024-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_407};"""
        exception ""
    }
    def const_sql_0_408 = """select "2024-11-01", cast(cast("2024-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_408};"""
        exception ""
    }
    def const_sql_0_409 = """select "2024-11-02", cast(cast("2024-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_409};"""
        exception ""
    }
    def const_sql_0_410 = """select "2024-11-09", cast(cast("2024-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_410};"""
        exception ""
    }
    def const_sql_0_411 = """select "2024-11-10", cast(cast("2024-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_411};"""
        exception ""
    }
    def const_sql_0_412 = """select "2024-11-11", cast(cast("2024-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_412};"""
        exception ""
    }
    def const_sql_0_413 = """select "2024-11-28", cast(cast("2024-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_413};"""
        exception ""
    }
    def const_sql_0_414 = """select "2024-12-01", cast(cast("2024-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_414};"""
        exception ""
    }
    def const_sql_0_415 = """select "2024-12-02", cast(cast("2024-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_415};"""
        exception ""
    }
    def const_sql_0_416 = """select "2024-12-09", cast(cast("2024-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_416};"""
        exception ""
    }
    def const_sql_0_417 = """select "2024-12-10", cast(cast("2024-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_417};"""
        exception ""
    }
    def const_sql_0_418 = """select "2024-12-11", cast(cast("2024-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_418};"""
        exception ""
    }
    def const_sql_0_419 = """select "2024-12-28", cast(cast("2024-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_419};"""
        exception ""
    }
    def const_sql_0_420 = """select "2025-01-01", cast(cast("2025-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_420};"""
        exception ""
    }
    def const_sql_0_421 = """select "2025-01-02", cast(cast("2025-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_421};"""
        exception ""
    }
    def const_sql_0_422 = """select "2025-01-09", cast(cast("2025-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_422};"""
        exception ""
    }
    def const_sql_0_423 = """select "2025-01-10", cast(cast("2025-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_423};"""
        exception ""
    }
    def const_sql_0_424 = """select "2025-01-11", cast(cast("2025-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_424};"""
        exception ""
    }
    def const_sql_0_425 = """select "2025-01-28", cast(cast("2025-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_425};"""
        exception ""
    }
    def const_sql_0_426 = """select "2025-09-01", cast(cast("2025-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_426};"""
        exception ""
    }
    def const_sql_0_427 = """select "2025-09-02", cast(cast("2025-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_427};"""
        exception ""
    }
    def const_sql_0_428 = """select "2025-09-09", cast(cast("2025-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_428};"""
        exception ""
    }
    def const_sql_0_429 = """select "2025-09-10", cast(cast("2025-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_429};"""
        exception ""
    }
    def const_sql_0_430 = """select "2025-09-11", cast(cast("2025-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_430};"""
        exception ""
    }
    def const_sql_0_431 = """select "2025-09-28", cast(cast("2025-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_431};"""
        exception ""
    }
    def const_sql_0_432 = """select "2025-10-01", cast(cast("2025-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_432};"""
        exception ""
    }
    def const_sql_0_433 = """select "2025-10-02", cast(cast("2025-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_433};"""
        exception ""
    }
    def const_sql_0_434 = """select "2025-10-09", cast(cast("2025-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_434};"""
        exception ""
    }
    def const_sql_0_435 = """select "2025-10-10", cast(cast("2025-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_435};"""
        exception ""
    }
    def const_sql_0_436 = """select "2025-10-11", cast(cast("2025-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_436};"""
        exception ""
    }
    def const_sql_0_437 = """select "2025-10-28", cast(cast("2025-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_437};"""
        exception ""
    }
    def const_sql_0_438 = """select "2025-11-01", cast(cast("2025-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_438};"""
        exception ""
    }
    def const_sql_0_439 = """select "2025-11-02", cast(cast("2025-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_439};"""
        exception ""
    }
    def const_sql_0_440 = """select "2025-11-09", cast(cast("2025-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_440};"""
        exception ""
    }
    def const_sql_0_441 = """select "2025-11-10", cast(cast("2025-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_441};"""
        exception ""
    }
    def const_sql_0_442 = """select "2025-11-11", cast(cast("2025-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_442};"""
        exception ""
    }
    def const_sql_0_443 = """select "2025-11-28", cast(cast("2025-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_443};"""
        exception ""
    }
    def const_sql_0_444 = """select "2025-12-01", cast(cast("2025-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_444};"""
        exception ""
    }
    def const_sql_0_445 = """select "2025-12-02", cast(cast("2025-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_445};"""
        exception ""
    }
    def const_sql_0_446 = """select "2025-12-09", cast(cast("2025-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_446};"""
        exception ""
    }
    def const_sql_0_447 = """select "2025-12-10", cast(cast("2025-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_447};"""
        exception ""
    }
    def const_sql_0_448 = """select "2025-12-11", cast(cast("2025-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_448};"""
        exception ""
    }
    def const_sql_0_449 = """select "2025-12-28", cast(cast("2025-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_449};"""
        exception ""
    }
    def const_sql_0_450 = """select "9999-01-01", cast(cast("9999-01-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_450};"""
        exception ""
    }
    def const_sql_0_451 = """select "9999-01-02", cast(cast("9999-01-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_451};"""
        exception ""
    }
    def const_sql_0_452 = """select "9999-01-09", cast(cast("9999-01-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_452};"""
        exception ""
    }
    def const_sql_0_453 = """select "9999-01-10", cast(cast("9999-01-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_453};"""
        exception ""
    }
    def const_sql_0_454 = """select "9999-01-11", cast(cast("9999-01-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_454};"""
        exception ""
    }
    def const_sql_0_455 = """select "9999-01-28", cast(cast("9999-01-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_455};"""
        exception ""
    }
    def const_sql_0_456 = """select "9999-09-01", cast(cast("9999-09-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_456};"""
        exception ""
    }
    def const_sql_0_457 = """select "9999-09-02", cast(cast("9999-09-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_457};"""
        exception ""
    }
    def const_sql_0_458 = """select "9999-09-09", cast(cast("9999-09-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_458};"""
        exception ""
    }
    def const_sql_0_459 = """select "9999-09-10", cast(cast("9999-09-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_459};"""
        exception ""
    }
    def const_sql_0_460 = """select "9999-09-11", cast(cast("9999-09-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_460};"""
        exception ""
    }
    def const_sql_0_461 = """select "9999-09-28", cast(cast("9999-09-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_461};"""
        exception ""
    }
    def const_sql_0_462 = """select "9999-10-01", cast(cast("9999-10-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_462};"""
        exception ""
    }
    def const_sql_0_463 = """select "9999-10-02", cast(cast("9999-10-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_463};"""
        exception ""
    }
    def const_sql_0_464 = """select "9999-10-09", cast(cast("9999-10-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_464};"""
        exception ""
    }
    def const_sql_0_465 = """select "9999-10-10", cast(cast("9999-10-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_465};"""
        exception ""
    }
    def const_sql_0_466 = """select "9999-10-11", cast(cast("9999-10-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_466};"""
        exception ""
    }
    def const_sql_0_467 = """select "9999-10-28", cast(cast("9999-10-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_467};"""
        exception ""
    }
    def const_sql_0_468 = """select "9999-11-01", cast(cast("9999-11-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_468};"""
        exception ""
    }
    def const_sql_0_469 = """select "9999-11-02", cast(cast("9999-11-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_469};"""
        exception ""
    }
    def const_sql_0_470 = """select "9999-11-09", cast(cast("9999-11-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_470};"""
        exception ""
    }
    def const_sql_0_471 = """select "9999-11-10", cast(cast("9999-11-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_471};"""
        exception ""
    }
    def const_sql_0_472 = """select "9999-11-11", cast(cast("9999-11-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_472};"""
        exception ""
    }
    def const_sql_0_473 = """select "9999-11-28", cast(cast("9999-11-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_473};"""
        exception ""
    }
    def const_sql_0_474 = """select "9999-12-01", cast(cast("9999-12-01" as datev2) as double);"""

    test {
        sql """${const_sql_0_474};"""
        exception ""
    }
    def const_sql_0_475 = """select "9999-12-02", cast(cast("9999-12-02" as datev2) as double);"""

    test {
        sql """${const_sql_0_475};"""
        exception ""
    }
    def const_sql_0_476 = """select "9999-12-09", cast(cast("9999-12-09" as datev2) as double);"""

    test {
        sql """${const_sql_0_476};"""
        exception ""
    }
    def const_sql_0_477 = """select "9999-12-10", cast(cast("9999-12-10" as datev2) as double);"""

    test {
        sql """${const_sql_0_477};"""
        exception ""
    }
    def const_sql_0_478 = """select "9999-12-11", cast(cast("9999-12-11" as datev2) as double);"""

    test {
        sql """${const_sql_0_478};"""
        exception ""
    }
    def const_sql_0_479 = """select "9999-12-28", cast(cast("9999-12-28" as datev2) as double);"""

    test {
        sql """${const_sql_0_479};"""
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
    qt_sql_0_160_non_strict "${const_sql_0_160}"
    testFoldConst("${const_sql_0_160}")
    qt_sql_0_161_non_strict "${const_sql_0_161}"
    testFoldConst("${const_sql_0_161}")
    qt_sql_0_162_non_strict "${const_sql_0_162}"
    testFoldConst("${const_sql_0_162}")
    qt_sql_0_163_non_strict "${const_sql_0_163}"
    testFoldConst("${const_sql_0_163}")
    qt_sql_0_164_non_strict "${const_sql_0_164}"
    testFoldConst("${const_sql_0_164}")
    qt_sql_0_165_non_strict "${const_sql_0_165}"
    testFoldConst("${const_sql_0_165}")
    qt_sql_0_166_non_strict "${const_sql_0_166}"
    testFoldConst("${const_sql_0_166}")
    qt_sql_0_167_non_strict "${const_sql_0_167}"
    testFoldConst("${const_sql_0_167}")
    qt_sql_0_168_non_strict "${const_sql_0_168}"
    testFoldConst("${const_sql_0_168}")
    qt_sql_0_169_non_strict "${const_sql_0_169}"
    testFoldConst("${const_sql_0_169}")
    qt_sql_0_170_non_strict "${const_sql_0_170}"
    testFoldConst("${const_sql_0_170}")
    qt_sql_0_171_non_strict "${const_sql_0_171}"
    testFoldConst("${const_sql_0_171}")
    qt_sql_0_172_non_strict "${const_sql_0_172}"
    testFoldConst("${const_sql_0_172}")
    qt_sql_0_173_non_strict "${const_sql_0_173}"
    testFoldConst("${const_sql_0_173}")
    qt_sql_0_174_non_strict "${const_sql_0_174}"
    testFoldConst("${const_sql_0_174}")
    qt_sql_0_175_non_strict "${const_sql_0_175}"
    testFoldConst("${const_sql_0_175}")
    qt_sql_0_176_non_strict "${const_sql_0_176}"
    testFoldConst("${const_sql_0_176}")
    qt_sql_0_177_non_strict "${const_sql_0_177}"
    testFoldConst("${const_sql_0_177}")
    qt_sql_0_178_non_strict "${const_sql_0_178}"
    testFoldConst("${const_sql_0_178}")
    qt_sql_0_179_non_strict "${const_sql_0_179}"
    testFoldConst("${const_sql_0_179}")
    qt_sql_0_180_non_strict "${const_sql_0_180}"
    testFoldConst("${const_sql_0_180}")
    qt_sql_0_181_non_strict "${const_sql_0_181}"
    testFoldConst("${const_sql_0_181}")
    qt_sql_0_182_non_strict "${const_sql_0_182}"
    testFoldConst("${const_sql_0_182}")
    qt_sql_0_183_non_strict "${const_sql_0_183}"
    testFoldConst("${const_sql_0_183}")
    qt_sql_0_184_non_strict "${const_sql_0_184}"
    testFoldConst("${const_sql_0_184}")
    qt_sql_0_185_non_strict "${const_sql_0_185}"
    testFoldConst("${const_sql_0_185}")
    qt_sql_0_186_non_strict "${const_sql_0_186}"
    testFoldConst("${const_sql_0_186}")
    qt_sql_0_187_non_strict "${const_sql_0_187}"
    testFoldConst("${const_sql_0_187}")
    qt_sql_0_188_non_strict "${const_sql_0_188}"
    testFoldConst("${const_sql_0_188}")
    qt_sql_0_189_non_strict "${const_sql_0_189}"
    testFoldConst("${const_sql_0_189}")
    qt_sql_0_190_non_strict "${const_sql_0_190}"
    testFoldConst("${const_sql_0_190}")
    qt_sql_0_191_non_strict "${const_sql_0_191}"
    testFoldConst("${const_sql_0_191}")
    qt_sql_0_192_non_strict "${const_sql_0_192}"
    testFoldConst("${const_sql_0_192}")
    qt_sql_0_193_non_strict "${const_sql_0_193}"
    testFoldConst("${const_sql_0_193}")
    qt_sql_0_194_non_strict "${const_sql_0_194}"
    testFoldConst("${const_sql_0_194}")
    qt_sql_0_195_non_strict "${const_sql_0_195}"
    testFoldConst("${const_sql_0_195}")
    qt_sql_0_196_non_strict "${const_sql_0_196}"
    testFoldConst("${const_sql_0_196}")
    qt_sql_0_197_non_strict "${const_sql_0_197}"
    testFoldConst("${const_sql_0_197}")
    qt_sql_0_198_non_strict "${const_sql_0_198}"
    testFoldConst("${const_sql_0_198}")
    qt_sql_0_199_non_strict "${const_sql_0_199}"
    testFoldConst("${const_sql_0_199}")
    qt_sql_0_200_non_strict "${const_sql_0_200}"
    testFoldConst("${const_sql_0_200}")
    qt_sql_0_201_non_strict "${const_sql_0_201}"
    testFoldConst("${const_sql_0_201}")
    qt_sql_0_202_non_strict "${const_sql_0_202}"
    testFoldConst("${const_sql_0_202}")
    qt_sql_0_203_non_strict "${const_sql_0_203}"
    testFoldConst("${const_sql_0_203}")
    qt_sql_0_204_non_strict "${const_sql_0_204}"
    testFoldConst("${const_sql_0_204}")
    qt_sql_0_205_non_strict "${const_sql_0_205}"
    testFoldConst("${const_sql_0_205}")
    qt_sql_0_206_non_strict "${const_sql_0_206}"
    testFoldConst("${const_sql_0_206}")
    qt_sql_0_207_non_strict "${const_sql_0_207}"
    testFoldConst("${const_sql_0_207}")
    qt_sql_0_208_non_strict "${const_sql_0_208}"
    testFoldConst("${const_sql_0_208}")
    qt_sql_0_209_non_strict "${const_sql_0_209}"
    testFoldConst("${const_sql_0_209}")
    qt_sql_0_210_non_strict "${const_sql_0_210}"
    testFoldConst("${const_sql_0_210}")
    qt_sql_0_211_non_strict "${const_sql_0_211}"
    testFoldConst("${const_sql_0_211}")
    qt_sql_0_212_non_strict "${const_sql_0_212}"
    testFoldConst("${const_sql_0_212}")
    qt_sql_0_213_non_strict "${const_sql_0_213}"
    testFoldConst("${const_sql_0_213}")
    qt_sql_0_214_non_strict "${const_sql_0_214}"
    testFoldConst("${const_sql_0_214}")
    qt_sql_0_215_non_strict "${const_sql_0_215}"
    testFoldConst("${const_sql_0_215}")
    qt_sql_0_216_non_strict "${const_sql_0_216}"
    testFoldConst("${const_sql_0_216}")
    qt_sql_0_217_non_strict "${const_sql_0_217}"
    testFoldConst("${const_sql_0_217}")
    qt_sql_0_218_non_strict "${const_sql_0_218}"
    testFoldConst("${const_sql_0_218}")
    qt_sql_0_219_non_strict "${const_sql_0_219}"
    testFoldConst("${const_sql_0_219}")
    qt_sql_0_220_non_strict "${const_sql_0_220}"
    testFoldConst("${const_sql_0_220}")
    qt_sql_0_221_non_strict "${const_sql_0_221}"
    testFoldConst("${const_sql_0_221}")
    qt_sql_0_222_non_strict "${const_sql_0_222}"
    testFoldConst("${const_sql_0_222}")
    qt_sql_0_223_non_strict "${const_sql_0_223}"
    testFoldConst("${const_sql_0_223}")
    qt_sql_0_224_non_strict "${const_sql_0_224}"
    testFoldConst("${const_sql_0_224}")
    qt_sql_0_225_non_strict "${const_sql_0_225}"
    testFoldConst("${const_sql_0_225}")
    qt_sql_0_226_non_strict "${const_sql_0_226}"
    testFoldConst("${const_sql_0_226}")
    qt_sql_0_227_non_strict "${const_sql_0_227}"
    testFoldConst("${const_sql_0_227}")
    qt_sql_0_228_non_strict "${const_sql_0_228}"
    testFoldConst("${const_sql_0_228}")
    qt_sql_0_229_non_strict "${const_sql_0_229}"
    testFoldConst("${const_sql_0_229}")
    qt_sql_0_230_non_strict "${const_sql_0_230}"
    testFoldConst("${const_sql_0_230}")
    qt_sql_0_231_non_strict "${const_sql_0_231}"
    testFoldConst("${const_sql_0_231}")
    qt_sql_0_232_non_strict "${const_sql_0_232}"
    testFoldConst("${const_sql_0_232}")
    qt_sql_0_233_non_strict "${const_sql_0_233}"
    testFoldConst("${const_sql_0_233}")
    qt_sql_0_234_non_strict "${const_sql_0_234}"
    testFoldConst("${const_sql_0_234}")
    qt_sql_0_235_non_strict "${const_sql_0_235}"
    testFoldConst("${const_sql_0_235}")
    qt_sql_0_236_non_strict "${const_sql_0_236}"
    testFoldConst("${const_sql_0_236}")
    qt_sql_0_237_non_strict "${const_sql_0_237}"
    testFoldConst("${const_sql_0_237}")
    qt_sql_0_238_non_strict "${const_sql_0_238}"
    testFoldConst("${const_sql_0_238}")
    qt_sql_0_239_non_strict "${const_sql_0_239}"
    testFoldConst("${const_sql_0_239}")
    qt_sql_0_240_non_strict "${const_sql_0_240}"
    testFoldConst("${const_sql_0_240}")
    qt_sql_0_241_non_strict "${const_sql_0_241}"
    testFoldConst("${const_sql_0_241}")
    qt_sql_0_242_non_strict "${const_sql_0_242}"
    testFoldConst("${const_sql_0_242}")
    qt_sql_0_243_non_strict "${const_sql_0_243}"
    testFoldConst("${const_sql_0_243}")
    qt_sql_0_244_non_strict "${const_sql_0_244}"
    testFoldConst("${const_sql_0_244}")
    qt_sql_0_245_non_strict "${const_sql_0_245}"
    testFoldConst("${const_sql_0_245}")
    qt_sql_0_246_non_strict "${const_sql_0_246}"
    testFoldConst("${const_sql_0_246}")
    qt_sql_0_247_non_strict "${const_sql_0_247}"
    testFoldConst("${const_sql_0_247}")
    qt_sql_0_248_non_strict "${const_sql_0_248}"
    testFoldConst("${const_sql_0_248}")
    qt_sql_0_249_non_strict "${const_sql_0_249}"
    testFoldConst("${const_sql_0_249}")
    qt_sql_0_250_non_strict "${const_sql_0_250}"
    testFoldConst("${const_sql_0_250}")
    qt_sql_0_251_non_strict "${const_sql_0_251}"
    testFoldConst("${const_sql_0_251}")
    qt_sql_0_252_non_strict "${const_sql_0_252}"
    testFoldConst("${const_sql_0_252}")
    qt_sql_0_253_non_strict "${const_sql_0_253}"
    testFoldConst("${const_sql_0_253}")
    qt_sql_0_254_non_strict "${const_sql_0_254}"
    testFoldConst("${const_sql_0_254}")
    qt_sql_0_255_non_strict "${const_sql_0_255}"
    testFoldConst("${const_sql_0_255}")
    qt_sql_0_256_non_strict "${const_sql_0_256}"
    testFoldConst("${const_sql_0_256}")
    qt_sql_0_257_non_strict "${const_sql_0_257}"
    testFoldConst("${const_sql_0_257}")
    qt_sql_0_258_non_strict "${const_sql_0_258}"
    testFoldConst("${const_sql_0_258}")
    qt_sql_0_259_non_strict "${const_sql_0_259}"
    testFoldConst("${const_sql_0_259}")
    qt_sql_0_260_non_strict "${const_sql_0_260}"
    testFoldConst("${const_sql_0_260}")
    qt_sql_0_261_non_strict "${const_sql_0_261}"
    testFoldConst("${const_sql_0_261}")
    qt_sql_0_262_non_strict "${const_sql_0_262}"
    testFoldConst("${const_sql_0_262}")
    qt_sql_0_263_non_strict "${const_sql_0_263}"
    testFoldConst("${const_sql_0_263}")
    qt_sql_0_264_non_strict "${const_sql_0_264}"
    testFoldConst("${const_sql_0_264}")
    qt_sql_0_265_non_strict "${const_sql_0_265}"
    testFoldConst("${const_sql_0_265}")
    qt_sql_0_266_non_strict "${const_sql_0_266}"
    testFoldConst("${const_sql_0_266}")
    qt_sql_0_267_non_strict "${const_sql_0_267}"
    testFoldConst("${const_sql_0_267}")
    qt_sql_0_268_non_strict "${const_sql_0_268}"
    testFoldConst("${const_sql_0_268}")
    qt_sql_0_269_non_strict "${const_sql_0_269}"
    testFoldConst("${const_sql_0_269}")
    qt_sql_0_270_non_strict "${const_sql_0_270}"
    testFoldConst("${const_sql_0_270}")
    qt_sql_0_271_non_strict "${const_sql_0_271}"
    testFoldConst("${const_sql_0_271}")
    qt_sql_0_272_non_strict "${const_sql_0_272}"
    testFoldConst("${const_sql_0_272}")
    qt_sql_0_273_non_strict "${const_sql_0_273}"
    testFoldConst("${const_sql_0_273}")
    qt_sql_0_274_non_strict "${const_sql_0_274}"
    testFoldConst("${const_sql_0_274}")
    qt_sql_0_275_non_strict "${const_sql_0_275}"
    testFoldConst("${const_sql_0_275}")
    qt_sql_0_276_non_strict "${const_sql_0_276}"
    testFoldConst("${const_sql_0_276}")
    qt_sql_0_277_non_strict "${const_sql_0_277}"
    testFoldConst("${const_sql_0_277}")
    qt_sql_0_278_non_strict "${const_sql_0_278}"
    testFoldConst("${const_sql_0_278}")
    qt_sql_0_279_non_strict "${const_sql_0_279}"
    testFoldConst("${const_sql_0_279}")
    qt_sql_0_280_non_strict "${const_sql_0_280}"
    testFoldConst("${const_sql_0_280}")
    qt_sql_0_281_non_strict "${const_sql_0_281}"
    testFoldConst("${const_sql_0_281}")
    qt_sql_0_282_non_strict "${const_sql_0_282}"
    testFoldConst("${const_sql_0_282}")
    qt_sql_0_283_non_strict "${const_sql_0_283}"
    testFoldConst("${const_sql_0_283}")
    qt_sql_0_284_non_strict "${const_sql_0_284}"
    testFoldConst("${const_sql_0_284}")
    qt_sql_0_285_non_strict "${const_sql_0_285}"
    testFoldConst("${const_sql_0_285}")
    qt_sql_0_286_non_strict "${const_sql_0_286}"
    testFoldConst("${const_sql_0_286}")
    qt_sql_0_287_non_strict "${const_sql_0_287}"
    testFoldConst("${const_sql_0_287}")
    qt_sql_0_288_non_strict "${const_sql_0_288}"
    testFoldConst("${const_sql_0_288}")
    qt_sql_0_289_non_strict "${const_sql_0_289}"
    testFoldConst("${const_sql_0_289}")
    qt_sql_0_290_non_strict "${const_sql_0_290}"
    testFoldConst("${const_sql_0_290}")
    qt_sql_0_291_non_strict "${const_sql_0_291}"
    testFoldConst("${const_sql_0_291}")
    qt_sql_0_292_non_strict "${const_sql_0_292}"
    testFoldConst("${const_sql_0_292}")
    qt_sql_0_293_non_strict "${const_sql_0_293}"
    testFoldConst("${const_sql_0_293}")
    qt_sql_0_294_non_strict "${const_sql_0_294}"
    testFoldConst("${const_sql_0_294}")
    qt_sql_0_295_non_strict "${const_sql_0_295}"
    testFoldConst("${const_sql_0_295}")
    qt_sql_0_296_non_strict "${const_sql_0_296}"
    testFoldConst("${const_sql_0_296}")
    qt_sql_0_297_non_strict "${const_sql_0_297}"
    testFoldConst("${const_sql_0_297}")
    qt_sql_0_298_non_strict "${const_sql_0_298}"
    testFoldConst("${const_sql_0_298}")
    qt_sql_0_299_non_strict "${const_sql_0_299}"
    testFoldConst("${const_sql_0_299}")
    qt_sql_0_300_non_strict "${const_sql_0_300}"
    testFoldConst("${const_sql_0_300}")
    qt_sql_0_301_non_strict "${const_sql_0_301}"
    testFoldConst("${const_sql_0_301}")
    qt_sql_0_302_non_strict "${const_sql_0_302}"
    testFoldConst("${const_sql_0_302}")
    qt_sql_0_303_non_strict "${const_sql_0_303}"
    testFoldConst("${const_sql_0_303}")
    qt_sql_0_304_non_strict "${const_sql_0_304}"
    testFoldConst("${const_sql_0_304}")
    qt_sql_0_305_non_strict "${const_sql_0_305}"
    testFoldConst("${const_sql_0_305}")
    qt_sql_0_306_non_strict "${const_sql_0_306}"
    testFoldConst("${const_sql_0_306}")
    qt_sql_0_307_non_strict "${const_sql_0_307}"
    testFoldConst("${const_sql_0_307}")
    qt_sql_0_308_non_strict "${const_sql_0_308}"
    testFoldConst("${const_sql_0_308}")
    qt_sql_0_309_non_strict "${const_sql_0_309}"
    testFoldConst("${const_sql_0_309}")
    qt_sql_0_310_non_strict "${const_sql_0_310}"
    testFoldConst("${const_sql_0_310}")
    qt_sql_0_311_non_strict "${const_sql_0_311}"
    testFoldConst("${const_sql_0_311}")
    qt_sql_0_312_non_strict "${const_sql_0_312}"
    testFoldConst("${const_sql_0_312}")
    qt_sql_0_313_non_strict "${const_sql_0_313}"
    testFoldConst("${const_sql_0_313}")
    qt_sql_0_314_non_strict "${const_sql_0_314}"
    testFoldConst("${const_sql_0_314}")
    qt_sql_0_315_non_strict "${const_sql_0_315}"
    testFoldConst("${const_sql_0_315}")
    qt_sql_0_316_non_strict "${const_sql_0_316}"
    testFoldConst("${const_sql_0_316}")
    qt_sql_0_317_non_strict "${const_sql_0_317}"
    testFoldConst("${const_sql_0_317}")
    qt_sql_0_318_non_strict "${const_sql_0_318}"
    testFoldConst("${const_sql_0_318}")
    qt_sql_0_319_non_strict "${const_sql_0_319}"
    testFoldConst("${const_sql_0_319}")
    qt_sql_0_320_non_strict "${const_sql_0_320}"
    testFoldConst("${const_sql_0_320}")
    qt_sql_0_321_non_strict "${const_sql_0_321}"
    testFoldConst("${const_sql_0_321}")
    qt_sql_0_322_non_strict "${const_sql_0_322}"
    testFoldConst("${const_sql_0_322}")
    qt_sql_0_323_non_strict "${const_sql_0_323}"
    testFoldConst("${const_sql_0_323}")
    qt_sql_0_324_non_strict "${const_sql_0_324}"
    testFoldConst("${const_sql_0_324}")
    qt_sql_0_325_non_strict "${const_sql_0_325}"
    testFoldConst("${const_sql_0_325}")
    qt_sql_0_326_non_strict "${const_sql_0_326}"
    testFoldConst("${const_sql_0_326}")
    qt_sql_0_327_non_strict "${const_sql_0_327}"
    testFoldConst("${const_sql_0_327}")
    qt_sql_0_328_non_strict "${const_sql_0_328}"
    testFoldConst("${const_sql_0_328}")
    qt_sql_0_329_non_strict "${const_sql_0_329}"
    testFoldConst("${const_sql_0_329}")
    qt_sql_0_330_non_strict "${const_sql_0_330}"
    testFoldConst("${const_sql_0_330}")
    qt_sql_0_331_non_strict "${const_sql_0_331}"
    testFoldConst("${const_sql_0_331}")
    qt_sql_0_332_non_strict "${const_sql_0_332}"
    testFoldConst("${const_sql_0_332}")
    qt_sql_0_333_non_strict "${const_sql_0_333}"
    testFoldConst("${const_sql_0_333}")
    qt_sql_0_334_non_strict "${const_sql_0_334}"
    testFoldConst("${const_sql_0_334}")
    qt_sql_0_335_non_strict "${const_sql_0_335}"
    testFoldConst("${const_sql_0_335}")
    qt_sql_0_336_non_strict "${const_sql_0_336}"
    testFoldConst("${const_sql_0_336}")
    qt_sql_0_337_non_strict "${const_sql_0_337}"
    testFoldConst("${const_sql_0_337}")
    qt_sql_0_338_non_strict "${const_sql_0_338}"
    testFoldConst("${const_sql_0_338}")
    qt_sql_0_339_non_strict "${const_sql_0_339}"
    testFoldConst("${const_sql_0_339}")
    qt_sql_0_340_non_strict "${const_sql_0_340}"
    testFoldConst("${const_sql_0_340}")
    qt_sql_0_341_non_strict "${const_sql_0_341}"
    testFoldConst("${const_sql_0_341}")
    qt_sql_0_342_non_strict "${const_sql_0_342}"
    testFoldConst("${const_sql_0_342}")
    qt_sql_0_343_non_strict "${const_sql_0_343}"
    testFoldConst("${const_sql_0_343}")
    qt_sql_0_344_non_strict "${const_sql_0_344}"
    testFoldConst("${const_sql_0_344}")
    qt_sql_0_345_non_strict "${const_sql_0_345}"
    testFoldConst("${const_sql_0_345}")
    qt_sql_0_346_non_strict "${const_sql_0_346}"
    testFoldConst("${const_sql_0_346}")
    qt_sql_0_347_non_strict "${const_sql_0_347}"
    testFoldConst("${const_sql_0_347}")
    qt_sql_0_348_non_strict "${const_sql_0_348}"
    testFoldConst("${const_sql_0_348}")
    qt_sql_0_349_non_strict "${const_sql_0_349}"
    testFoldConst("${const_sql_0_349}")
    qt_sql_0_350_non_strict "${const_sql_0_350}"
    testFoldConst("${const_sql_0_350}")
    qt_sql_0_351_non_strict "${const_sql_0_351}"
    testFoldConst("${const_sql_0_351}")
    qt_sql_0_352_non_strict "${const_sql_0_352}"
    testFoldConst("${const_sql_0_352}")
    qt_sql_0_353_non_strict "${const_sql_0_353}"
    testFoldConst("${const_sql_0_353}")
    qt_sql_0_354_non_strict "${const_sql_0_354}"
    testFoldConst("${const_sql_0_354}")
    qt_sql_0_355_non_strict "${const_sql_0_355}"
    testFoldConst("${const_sql_0_355}")
    qt_sql_0_356_non_strict "${const_sql_0_356}"
    testFoldConst("${const_sql_0_356}")
    qt_sql_0_357_non_strict "${const_sql_0_357}"
    testFoldConst("${const_sql_0_357}")
    qt_sql_0_358_non_strict "${const_sql_0_358}"
    testFoldConst("${const_sql_0_358}")
    qt_sql_0_359_non_strict "${const_sql_0_359}"
    testFoldConst("${const_sql_0_359}")
    qt_sql_0_360_non_strict "${const_sql_0_360}"
    testFoldConst("${const_sql_0_360}")
    qt_sql_0_361_non_strict "${const_sql_0_361}"
    testFoldConst("${const_sql_0_361}")
    qt_sql_0_362_non_strict "${const_sql_0_362}"
    testFoldConst("${const_sql_0_362}")
    qt_sql_0_363_non_strict "${const_sql_0_363}"
    testFoldConst("${const_sql_0_363}")
    qt_sql_0_364_non_strict "${const_sql_0_364}"
    testFoldConst("${const_sql_0_364}")
    qt_sql_0_365_non_strict "${const_sql_0_365}"
    testFoldConst("${const_sql_0_365}")
    qt_sql_0_366_non_strict "${const_sql_0_366}"
    testFoldConst("${const_sql_0_366}")
    qt_sql_0_367_non_strict "${const_sql_0_367}"
    testFoldConst("${const_sql_0_367}")
    qt_sql_0_368_non_strict "${const_sql_0_368}"
    testFoldConst("${const_sql_0_368}")
    qt_sql_0_369_non_strict "${const_sql_0_369}"
    testFoldConst("${const_sql_0_369}")
    qt_sql_0_370_non_strict "${const_sql_0_370}"
    testFoldConst("${const_sql_0_370}")
    qt_sql_0_371_non_strict "${const_sql_0_371}"
    testFoldConst("${const_sql_0_371}")
    qt_sql_0_372_non_strict "${const_sql_0_372}"
    testFoldConst("${const_sql_0_372}")
    qt_sql_0_373_non_strict "${const_sql_0_373}"
    testFoldConst("${const_sql_0_373}")
    qt_sql_0_374_non_strict "${const_sql_0_374}"
    testFoldConst("${const_sql_0_374}")
    qt_sql_0_375_non_strict "${const_sql_0_375}"
    testFoldConst("${const_sql_0_375}")
    qt_sql_0_376_non_strict "${const_sql_0_376}"
    testFoldConst("${const_sql_0_376}")
    qt_sql_0_377_non_strict "${const_sql_0_377}"
    testFoldConst("${const_sql_0_377}")
    qt_sql_0_378_non_strict "${const_sql_0_378}"
    testFoldConst("${const_sql_0_378}")
    qt_sql_0_379_non_strict "${const_sql_0_379}"
    testFoldConst("${const_sql_0_379}")
    qt_sql_0_380_non_strict "${const_sql_0_380}"
    testFoldConst("${const_sql_0_380}")
    qt_sql_0_381_non_strict "${const_sql_0_381}"
    testFoldConst("${const_sql_0_381}")
    qt_sql_0_382_non_strict "${const_sql_0_382}"
    testFoldConst("${const_sql_0_382}")
    qt_sql_0_383_non_strict "${const_sql_0_383}"
    testFoldConst("${const_sql_0_383}")
    qt_sql_0_384_non_strict "${const_sql_0_384}"
    testFoldConst("${const_sql_0_384}")
    qt_sql_0_385_non_strict "${const_sql_0_385}"
    testFoldConst("${const_sql_0_385}")
    qt_sql_0_386_non_strict "${const_sql_0_386}"
    testFoldConst("${const_sql_0_386}")
    qt_sql_0_387_non_strict "${const_sql_0_387}"
    testFoldConst("${const_sql_0_387}")
    qt_sql_0_388_non_strict "${const_sql_0_388}"
    testFoldConst("${const_sql_0_388}")
    qt_sql_0_389_non_strict "${const_sql_0_389}"
    testFoldConst("${const_sql_0_389}")
    qt_sql_0_390_non_strict "${const_sql_0_390}"
    testFoldConst("${const_sql_0_390}")
    qt_sql_0_391_non_strict "${const_sql_0_391}"
    testFoldConst("${const_sql_0_391}")
    qt_sql_0_392_non_strict "${const_sql_0_392}"
    testFoldConst("${const_sql_0_392}")
    qt_sql_0_393_non_strict "${const_sql_0_393}"
    testFoldConst("${const_sql_0_393}")
    qt_sql_0_394_non_strict "${const_sql_0_394}"
    testFoldConst("${const_sql_0_394}")
    qt_sql_0_395_non_strict "${const_sql_0_395}"
    testFoldConst("${const_sql_0_395}")
    qt_sql_0_396_non_strict "${const_sql_0_396}"
    testFoldConst("${const_sql_0_396}")
    qt_sql_0_397_non_strict "${const_sql_0_397}"
    testFoldConst("${const_sql_0_397}")
    qt_sql_0_398_non_strict "${const_sql_0_398}"
    testFoldConst("${const_sql_0_398}")
    qt_sql_0_399_non_strict "${const_sql_0_399}"
    testFoldConst("${const_sql_0_399}")
    qt_sql_0_400_non_strict "${const_sql_0_400}"
    testFoldConst("${const_sql_0_400}")
    qt_sql_0_401_non_strict "${const_sql_0_401}"
    testFoldConst("${const_sql_0_401}")
    qt_sql_0_402_non_strict "${const_sql_0_402}"
    testFoldConst("${const_sql_0_402}")
    qt_sql_0_403_non_strict "${const_sql_0_403}"
    testFoldConst("${const_sql_0_403}")
    qt_sql_0_404_non_strict "${const_sql_0_404}"
    testFoldConst("${const_sql_0_404}")
    qt_sql_0_405_non_strict "${const_sql_0_405}"
    testFoldConst("${const_sql_0_405}")
    qt_sql_0_406_non_strict "${const_sql_0_406}"
    testFoldConst("${const_sql_0_406}")
    qt_sql_0_407_non_strict "${const_sql_0_407}"
    testFoldConst("${const_sql_0_407}")
    qt_sql_0_408_non_strict "${const_sql_0_408}"
    testFoldConst("${const_sql_0_408}")
    qt_sql_0_409_non_strict "${const_sql_0_409}"
    testFoldConst("${const_sql_0_409}")
    qt_sql_0_410_non_strict "${const_sql_0_410}"
    testFoldConst("${const_sql_0_410}")
    qt_sql_0_411_non_strict "${const_sql_0_411}"
    testFoldConst("${const_sql_0_411}")
    qt_sql_0_412_non_strict "${const_sql_0_412}"
    testFoldConst("${const_sql_0_412}")
    qt_sql_0_413_non_strict "${const_sql_0_413}"
    testFoldConst("${const_sql_0_413}")
    qt_sql_0_414_non_strict "${const_sql_0_414}"
    testFoldConst("${const_sql_0_414}")
    qt_sql_0_415_non_strict "${const_sql_0_415}"
    testFoldConst("${const_sql_0_415}")
    qt_sql_0_416_non_strict "${const_sql_0_416}"
    testFoldConst("${const_sql_0_416}")
    qt_sql_0_417_non_strict "${const_sql_0_417}"
    testFoldConst("${const_sql_0_417}")
    qt_sql_0_418_non_strict "${const_sql_0_418}"
    testFoldConst("${const_sql_0_418}")
    qt_sql_0_419_non_strict "${const_sql_0_419}"
    testFoldConst("${const_sql_0_419}")
    qt_sql_0_420_non_strict "${const_sql_0_420}"
    testFoldConst("${const_sql_0_420}")
    qt_sql_0_421_non_strict "${const_sql_0_421}"
    testFoldConst("${const_sql_0_421}")
    qt_sql_0_422_non_strict "${const_sql_0_422}"
    testFoldConst("${const_sql_0_422}")
    qt_sql_0_423_non_strict "${const_sql_0_423}"
    testFoldConst("${const_sql_0_423}")
    qt_sql_0_424_non_strict "${const_sql_0_424}"
    testFoldConst("${const_sql_0_424}")
    qt_sql_0_425_non_strict "${const_sql_0_425}"
    testFoldConst("${const_sql_0_425}")
    qt_sql_0_426_non_strict "${const_sql_0_426}"
    testFoldConst("${const_sql_0_426}")
    qt_sql_0_427_non_strict "${const_sql_0_427}"
    testFoldConst("${const_sql_0_427}")
    qt_sql_0_428_non_strict "${const_sql_0_428}"
    testFoldConst("${const_sql_0_428}")
    qt_sql_0_429_non_strict "${const_sql_0_429}"
    testFoldConst("${const_sql_0_429}")
    qt_sql_0_430_non_strict "${const_sql_0_430}"
    testFoldConst("${const_sql_0_430}")
    qt_sql_0_431_non_strict "${const_sql_0_431}"
    testFoldConst("${const_sql_0_431}")
    qt_sql_0_432_non_strict "${const_sql_0_432}"
    testFoldConst("${const_sql_0_432}")
    qt_sql_0_433_non_strict "${const_sql_0_433}"
    testFoldConst("${const_sql_0_433}")
    qt_sql_0_434_non_strict "${const_sql_0_434}"
    testFoldConst("${const_sql_0_434}")
    qt_sql_0_435_non_strict "${const_sql_0_435}"
    testFoldConst("${const_sql_0_435}")
    qt_sql_0_436_non_strict "${const_sql_0_436}"
    testFoldConst("${const_sql_0_436}")
    qt_sql_0_437_non_strict "${const_sql_0_437}"
    testFoldConst("${const_sql_0_437}")
    qt_sql_0_438_non_strict "${const_sql_0_438}"
    testFoldConst("${const_sql_0_438}")
    qt_sql_0_439_non_strict "${const_sql_0_439}"
    testFoldConst("${const_sql_0_439}")
    qt_sql_0_440_non_strict "${const_sql_0_440}"
    testFoldConst("${const_sql_0_440}")
    qt_sql_0_441_non_strict "${const_sql_0_441}"
    testFoldConst("${const_sql_0_441}")
    qt_sql_0_442_non_strict "${const_sql_0_442}"
    testFoldConst("${const_sql_0_442}")
    qt_sql_0_443_non_strict "${const_sql_0_443}"
    testFoldConst("${const_sql_0_443}")
    qt_sql_0_444_non_strict "${const_sql_0_444}"
    testFoldConst("${const_sql_0_444}")
    qt_sql_0_445_non_strict "${const_sql_0_445}"
    testFoldConst("${const_sql_0_445}")
    qt_sql_0_446_non_strict "${const_sql_0_446}"
    testFoldConst("${const_sql_0_446}")
    qt_sql_0_447_non_strict "${const_sql_0_447}"
    testFoldConst("${const_sql_0_447}")
    qt_sql_0_448_non_strict "${const_sql_0_448}"
    testFoldConst("${const_sql_0_448}")
    qt_sql_0_449_non_strict "${const_sql_0_449}"
    testFoldConst("${const_sql_0_449}")
    qt_sql_0_450_non_strict "${const_sql_0_450}"
    testFoldConst("${const_sql_0_450}")
    qt_sql_0_451_non_strict "${const_sql_0_451}"
    testFoldConst("${const_sql_0_451}")
    qt_sql_0_452_non_strict "${const_sql_0_452}"
    testFoldConst("${const_sql_0_452}")
    qt_sql_0_453_non_strict "${const_sql_0_453}"
    testFoldConst("${const_sql_0_453}")
    qt_sql_0_454_non_strict "${const_sql_0_454}"
    testFoldConst("${const_sql_0_454}")
    qt_sql_0_455_non_strict "${const_sql_0_455}"
    testFoldConst("${const_sql_0_455}")
    qt_sql_0_456_non_strict "${const_sql_0_456}"
    testFoldConst("${const_sql_0_456}")
    qt_sql_0_457_non_strict "${const_sql_0_457}"
    testFoldConst("${const_sql_0_457}")
    qt_sql_0_458_non_strict "${const_sql_0_458}"
    testFoldConst("${const_sql_0_458}")
    qt_sql_0_459_non_strict "${const_sql_0_459}"
    testFoldConst("${const_sql_0_459}")
    qt_sql_0_460_non_strict "${const_sql_0_460}"
    testFoldConst("${const_sql_0_460}")
    qt_sql_0_461_non_strict "${const_sql_0_461}"
    testFoldConst("${const_sql_0_461}")
    qt_sql_0_462_non_strict "${const_sql_0_462}"
    testFoldConst("${const_sql_0_462}")
    qt_sql_0_463_non_strict "${const_sql_0_463}"
    testFoldConst("${const_sql_0_463}")
    qt_sql_0_464_non_strict "${const_sql_0_464}"
    testFoldConst("${const_sql_0_464}")
    qt_sql_0_465_non_strict "${const_sql_0_465}"
    testFoldConst("${const_sql_0_465}")
    qt_sql_0_466_non_strict "${const_sql_0_466}"
    testFoldConst("${const_sql_0_466}")
    qt_sql_0_467_non_strict "${const_sql_0_467}"
    testFoldConst("${const_sql_0_467}")
    qt_sql_0_468_non_strict "${const_sql_0_468}"
    testFoldConst("${const_sql_0_468}")
    qt_sql_0_469_non_strict "${const_sql_0_469}"
    testFoldConst("${const_sql_0_469}")
    qt_sql_0_470_non_strict "${const_sql_0_470}"
    testFoldConst("${const_sql_0_470}")
    qt_sql_0_471_non_strict "${const_sql_0_471}"
    testFoldConst("${const_sql_0_471}")
    qt_sql_0_472_non_strict "${const_sql_0_472}"
    testFoldConst("${const_sql_0_472}")
    qt_sql_0_473_non_strict "${const_sql_0_473}"
    testFoldConst("${const_sql_0_473}")
    qt_sql_0_474_non_strict "${const_sql_0_474}"
    testFoldConst("${const_sql_0_474}")
    qt_sql_0_475_non_strict "${const_sql_0_475}"
    testFoldConst("${const_sql_0_475}")
    qt_sql_0_476_non_strict "${const_sql_0_476}"
    testFoldConst("${const_sql_0_476}")
    qt_sql_0_477_non_strict "${const_sql_0_477}"
    testFoldConst("${const_sql_0_477}")
    qt_sql_0_478_non_strict "${const_sql_0_478}"
    testFoldConst("${const_sql_0_478}")
    qt_sql_0_479_non_strict "${const_sql_0_479}"
    testFoldConst("${const_sql_0_479}")
}