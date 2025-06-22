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


suite("test_cast_to_float32_from_decimal32") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_float32_from_decimal32_0;"
    sql "create table test_cast_to_float32_from_decimal32_0(f1 int, f2 decimalv3(1, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_0 values (0, "0"),(1, "0"),(2, "1"),(3, "-1"),(4, "8"),(5, "-8"),(6, "9"),(7, "-9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_0 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_0 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_1;"
    sql "create table test_cast_to_float32_from_decimal32_1(f1 int, f2 decimalv3(1, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_1 values (0, "0.0"),(1, "0.0"),(2, "0.1"),(3, "-0.1"),(4, "0.8"),(5, "-0.8"),(6, "0.9"),(7, "-0.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_1_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_1 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_1_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_1 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_2;"
    sql "create table test_cast_to_float32_from_decimal32_2(f1 int, f2 decimalv3(9, 0)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_2 values (0, "0"),(1, "0"),(2, "1"),(3, "-1"),(4, "9"),(5, "-9"),(6, "99999999"),(7, "-99999999"),(8, "900000000"),(9, "-900000000"),(10, "900000001"),(11, "-900000001"),(12, "999999998"),(13, "-999999998"),(14, "999999999"),(15, "-999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_2_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_2 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_2_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_2 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_3;"
    sql "create table test_cast_to_float32_from_decimal32_3(f1 int, f2 decimalv3(9, 1)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_3 values (0, "0.0"),(1, "0.0"),(2, "0.1"),(3, "-0.1"),(4, "0.8"),(5, "-0.8"),(6, "0.9"),(7, "-0.9"),(8, "1.0"),(9, "-1.0"),(10, "1.1"),(11, "-1.1"),(12, "1.8"),(13, "-1.8"),(14, "1.9"),(15, "-1.9"),(16, "9.0"),(17, "-9.0"),(18, "9.1"),(19, "-9.1"),
      (20, "9.8"),(21, "-9.8"),(22, "9.9"),(23, "-9.9"),(24, "9999999.0"),(25, "-9999999.0"),(26, "9999999.1"),(27, "-9999999.1"),(28, "9999999.8"),(29, "-9999999.8"),(30, "9999999.9"),(31, "-9999999.9"),(32, "90000000.0"),(33, "-90000000.0"),(34, "90000000.1"),(35, "-90000000.1"),(36, "90000000.8"),(37, "-90000000.8"),(38, "90000000.9"),(39, "-90000000.9"),
      (40, "90000001.0"),(41, "-90000001.0"),(42, "90000001.1"),(43, "-90000001.1"),(44, "90000001.8"),(45, "-90000001.8"),(46, "90000001.9"),(47, "-90000001.9"),(48, "99999998.0"),(49, "-99999998.0"),(50, "99999998.1"),(51, "-99999998.1"),(52, "99999998.8"),(53, "-99999998.8"),(54, "99999998.9"),(55, "-99999998.9"),(56, "99999999.0"),(57, "-99999999.0"),(58, "99999999.1"),(59, "-99999999.1"),
      (60, "99999999.8"),(61, "-99999999.8"),(62, "99999999.9"),(63, "-99999999.9");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_3_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_3 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_3 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_4;"
    sql "create table test_cast_to_float32_from_decimal32_4(f1 int, f2 decimalv3(9, 4)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_4 values (0, "0.0000"),(1, "0.0000"),(2, "0.0001"),(3, "-0.0001"),(4, "0.0009"),(5, "-0.0009"),(6, "0.0999"),(7, "-0.0999"),(8, "0.9000"),(9, "-0.9000"),(10, "0.9001"),(11, "-0.9001"),(12, "0.9998"),(13, "-0.9998"),(14, "0.9999"),(15, "-0.9999"),(16, "1.0000"),(17, "-1.0000"),(18, "1.0001"),(19, "-1.0001"),
      (20, "1.0009"),(21, "-1.0009"),(22, "1.0999"),(23, "-1.0999"),(24, "1.9000"),(25, "-1.9000"),(26, "1.9001"),(27, "-1.9001"),(28, "1.9998"),(29, "-1.9998"),(30, "1.9999"),(31, "-1.9999"),(32, "9.0000"),(33, "-9.0000"),(34, "9.0001"),(35, "-9.0001"),(36, "9.0009"),(37, "-9.0009"),(38, "9.0999"),(39, "-9.0999"),
      (40, "9.9000"),(41, "-9.9000"),(42, "9.9001"),(43, "-9.9001"),(44, "9.9998"),(45, "-9.9998"),(46, "9.9999"),(47, "-9.9999"),(48, "9999.0000"),(49, "-9999.0000"),(50, "9999.0001"),(51, "-9999.0001"),(52, "9999.0009"),(53, "-9999.0009"),(54, "9999.0999"),(55, "-9999.0999"),(56, "9999.9000"),(57, "-9999.9000"),(58, "9999.9001"),(59, "-9999.9001"),
      (60, "9999.9998"),(61, "-9999.9998"),(62, "9999.9999"),(63, "-9999.9999"),(64, "90000.0000"),(65, "-90000.0000"),(66, "90000.0001"),(67, "-90000.0001"),(68, "90000.0009"),(69, "-90000.0009"),(70, "90000.0999"),(71, "-90000.0999"),(72, "90000.9000"),(73, "-90000.9000"),(74, "90000.9001"),(75, "-90000.9001"),(76, "90000.9998"),(77, "-90000.9998"),(78, "90000.9999"),(79, "-90000.9999"),
      (80, "90001.0000"),(81, "-90001.0000"),(82, "90001.0001"),(83, "-90001.0001"),(84, "90001.0009"),(85, "-90001.0009"),(86, "90001.0999"),(87, "-90001.0999"),(88, "90001.9000"),(89, "-90001.9000"),(90, "90001.9001"),(91, "-90001.9001"),(92, "90001.9998"),(93, "-90001.9998"),(94, "90001.9999"),(95, "-90001.9999"),(96, "99998.0000"),(97, "-99998.0000"),(98, "99998.0001"),(99, "-99998.0001"),
      (100, "99998.0009"),(101, "-99998.0009"),(102, "99998.0999"),(103, "-99998.0999"),(104, "99998.9000"),(105, "-99998.9000"),(106, "99998.9001"),(107, "-99998.9001"),(108, "99998.9998"),(109, "-99998.9998"),(110, "99998.9999"),(111, "-99998.9999"),(112, "99999.0000"),(113, "-99999.0000"),(114, "99999.0001"),(115, "-99999.0001"),(116, "99999.0009"),(117, "-99999.0009"),(118, "99999.0999"),(119, "-99999.0999"),
      (120, "99999.9000"),(121, "-99999.9000"),(122, "99999.9001"),(123, "-99999.9001"),(124, "99999.9998"),(125, "-99999.9998"),(126, "99999.9999"),(127, "-99999.9999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_4_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_4 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_4_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_4 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_5;"
    sql "create table test_cast_to_float32_from_decimal32_5(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_5 values (0, "0.00000000"),(1, "0.00000000"),(2, "0.00000001"),(3, "-0.00000001"),(4, "0.00000009"),(5, "-0.00000009"),(6, "0.09999999"),(7, "-0.09999999"),(8, "0.90000000"),(9, "-0.90000000"),(10, "0.90000001"),(11, "-0.90000001"),(12, "0.99999998"),(13, "-0.99999998"),(14, "0.99999999"),(15, "-0.99999999"),(16, "1.00000000"),(17, "-1.00000000"),(18, "1.00000001"),(19, "-1.00000001"),
      (20, "1.00000009"),(21, "-1.00000009"),(22, "1.09999999"),(23, "-1.09999999"),(24, "1.90000000"),(25, "-1.90000000"),(26, "1.90000001"),(27, "-1.90000001"),(28, "1.99999998"),(29, "-1.99999998"),(30, "1.99999999"),(31, "-1.99999999"),(32, "8.00000000"),(33, "-8.00000000"),(34, "8.00000001"),(35, "-8.00000001"),(36, "8.00000009"),(37, "-8.00000009"),(38, "8.09999999"),(39, "-8.09999999"),
      (40, "8.90000000"),(41, "-8.90000000"),(42, "8.90000001"),(43, "-8.90000001"),(44, "8.99999998"),(45, "-8.99999998"),(46, "8.99999999"),(47, "-8.99999999"),(48, "9.00000000"),(49, "-9.00000000"),(50, "9.00000001"),(51, "-9.00000001"),(52, "9.00000009"),(53, "-9.00000009"),(54, "9.09999999"),(55, "-9.09999999"),(56, "9.90000000"),(57, "-9.90000000"),(58, "9.90000001"),(59, "-9.90000001"),
      (60, "9.99999998"),(61, "-9.99999998"),(62, "9.99999999"),(63, "-9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_5_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_5 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_5_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_5 order by 1;'

    sql "drop table if exists test_cast_to_float32_from_decimal32_6;"
    sql "create table test_cast_to_float32_from_decimal32_6(f1 int, f2 decimalv3(9, 9)) properties('replication_num'='1');"
    sql """insert into test_cast_to_float32_from_decimal32_6 values (0, "0.000000000"),(1, "0.000000000"),(2, "0.000000001"),(3, "-0.000000001"),(4, "0.000000009"),(5, "-0.000000009"),(6, "0.099999999"),(7, "-0.099999999"),(8, "0.900000000"),(9, "-0.900000000"),(10, "0.900000001"),(11, "-0.900000001"),(12, "0.999999998"),(13, "-0.999999998"),(14, "0.999999999"),(15, "-0.999999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_6_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_6 order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_6_non_strict 'select f1, cast(f2 as float) from test_cast_to_float32_from_decimal32_6 order by 1;'

}