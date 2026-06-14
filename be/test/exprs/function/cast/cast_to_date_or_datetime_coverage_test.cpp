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

// Coverage-oriented tests for the datev1/datetimev1 cast implementation in
// `be/src/exprs/function/cast/cast_to_date_or_datetime_impl.hpp` (struct
// `CastToDateOrDatetime`). These tests intentionally target the v1 types
// `DataTypeDate` (TYPE_DATE) and `DataTypeDateTime` (TYPE_DATETIME), which route
// to `CastToDateOrDatetime`, as opposed to the v2 tests that target
// `DataTypeDateV2` / `DataTypeDateTimeV2`. Expected values mirror the proven v2
// ground-truth tests, adjusted for v1 second-level precision (no microseconds).

#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "exprs/function/cast/cast_test.h"

namespace doris {
using namespace ut_type;

// ===========================================================================
// Cluster D: from_string manual parser -> DATE (strict mode, valid)
// Non-canonical formats exercise the manual parser: 14-digit YYYYMMDDHHMMSS,
// no-delimiter date (6/8-digit), 2/4-digit year with separators, colon/compact
// time, fractional part, timezone offsets and timezone names, year cutoff.
// ===========================================================================
TEST_F(FunctionCastTest, v1_string_to_date_valid_case_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // ISO 8601 with timezone (tz parsed/validated, not applied for DATE)
            {{std::string("2023-07-16T19:20:30.123+08:00")}, std::string("2023-07-16")},
            {{std::string("2023-07-16T19+08:00")}, std::string("2023-07-16")},
            {{std::string("2023-07-16T1920+08:00")}, std::string("2023-07-16")},
            {{std::string("2023-07-16T1920+00:00")}, std::string("2023-07-16")},
            {{std::string("70-1-1T00:00:00-0000")}, std::string("1970-01-01")},

            // Timezone names
            {{std::string("2024-02-29 12:00:00 Europe/Paris")}, std::string("2024-02-29")},
            {{std::string("2024-05-01T00:00Asia/Shanghai")}, std::string("2024-05-01")},
            {{std::string("20231005T081530Europe/London")}, std::string("2023-10-05")},
            {{std::string("85-12-25T0000gMt")}, std::string("1985-12-25")},

            // Simple / delimiter date formats
            {{std::string("2024-05-01")}, std::string("2024-05-01")},
            {{std::string("24-5-1")}, std::string("2024-05-01")},
            {{std::string("2024-05-01 0:1:2.333")}, std::string("2024-05-01")},
            {{std::string("2024-05-01 0:1:2.")}, std::string("2024-05-01")},

            // Compact formats
            {{std::string("20240501 01")}, std::string("2024-05-01")},
            {{std::string("20230716 1920Z")}, std::string("2023-07-16")},
            {{std::string("20240501T0000")}, std::string("2024-05-01")},

            // High precision fractional (ignored for DATE)
            {{std::string("2024-12-31 23:59:59.9999999999999999999")}, std::string("2024-12-31")},

            // Timezone offsets
            {{std::string("2020-12-12 13:12:12-03:00")}, std::string("2020-12-12")},
            {{std::string("0023-01-01T00:00Z")}, std::string("0023-01-01")},

            // Year cutoff cases
            {{std::string("69-12-31")}, std::string("2069-12-31")},
            {{std::string("70-01-01")}, std::string("1970-01-01")},
            {{std::string("68-01-01")}, std::string("2068-01-01")},
            {{std::string("69-01-01")}, std::string("2069-01-01")},
            {{std::string("71-01-01")}, std::string("1971-01-01")},
            {{std::string("99-12-31")}, std::string("1999-12-31")},
            {{std::string("00-01-01")}, std::string("2000-01-01")},
            {{std::string("20-1-1")}, std::string("2020-01-01")},

            // Compact numeric (no-delimiter) date formats
            {{std::string("230102")}, std::string("2023-01-02")},
            {{std::string("19230101")}, std::string("1923-01-01")},
            {{std::string("20120102030405")}, std::string("2012-01-02")},
            {{std::string("0123-12-12")}, std::string("0123-12-12")},
            {{std::string("01231212")}, std::string("0123-12-12")},
            {{std::string("12010203040506.999")}, std::string("1201-02-03")},
            {{std::string("12010203040506.")}, std::string("1201-02-03")},

            // '/' separators and mixed
            {{std::string("2024/05/01")}, std::string("2024-05-01")},
            {{std::string("2024/05-01T12:30:45")}, std::string("2024-05-01")},

            // tz offset hour=14, fractional carry path (ignored for DATE)
            {{std::string("2024-02-29T23:59:59.999999 UTC")}, std::string("2024-02-29")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1970-01-01")},
            {{std::string("0023-1-1T1:2:3. -00:00")}, std::string("0023-01-01")},
            {{std::string("2025/06/15T00:00:00.99999999999999")}, std::string("2025-06-15")},
    };
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
}

// Cluster D + overflow/range error paths: from_string -> DATE (strict, invalid)
TEST_F(FunctionCastTest, v1_string_to_date_invalid_case_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("abc")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("2023-07-16T19.123+08:00")}, Null()},
            {{std::string("24012")}, Null()},
            {{std::string("2411 123")}, Null()},
            {{std::string("2024-05-01 01:030:02")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-0131T12:00")}, Null()},
            {{std::string("2024-05-01@00:00")}, Null()},
            {{std::string("20120212051")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024-5-1T24:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T12:60")}, Null()},
            {{std::string("2012-06-30T23:59:60")}, Null()},
            {{std::string("2024-05-01T00:00+14:30")}, Null()},
            {{std::string("2024-05-01T00:00+08:25")}, Null()},
            {{std::string("2020-12-12   12:12:12")}, Null()},
            {{std::string("2020-12-12T 12:12:12")}, Null()},
            {{std::string("2020-12-12 +12:12:12")}, Null()},
            {{std::string("2011")}, Null()},
            {{std::string("123-12-12")}, Null()},
            {{std::string("1-12-12")}, Null()},
            {{std::string("00123-12-12")}, Null()},
            {{std::string("1231212")}, Null()},
            {{std::string("")}, Null()},
            {{std::string("   ")}, Null()},
            {{std::string("2024.05.01")}, Null()},
            {{std::string("2024.05.01 12.30.45")}, Null()},
            {{std::string("2024-05/01 12.30.45")}, Null()},
            {{std::string("-1")}, Null()},
            {{std::string("-1234")}, Null()},
    };
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set, "to Date failed");
}

// ===========================================================================
// Cluster E: from_string_non_strict_mode_impl -> DATE
// Flexible separators ('.', '|', '^', '~', '#'), whitespace, non-colon time
// separators, 2-digit-year completion. Also includes valid strict cases and
// invalid (NULL) cases routed through non-strict.
// ===========================================================================
TEST_F(FunctionCastTest, v1_string_to_date_non_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            // canonical / strict-parsable still work via non-strict
            {{std::string("2024-05-01")}, std::string("2024-05-01")},
            {{std::string("24-5-1")}, std::string("2024-05-01")},
            {{std::string("2024-05-01:12:12:12")}, std::string("2024-05-01")},
            {{std::string("2024-05-01:12:12:12.1230")}, std::string("2024-05-01")},
            {{std::string("200104")}, std::string("2020-01-04")},
            {{std::string("050505")}, std::string("2005-05-05")},

            // flexible separators / whitespace -> non_strict_mode_impl
            {{std::string("2023-7-4T9-5-3.1Z")}, std::string("2023-07-04")},
            {{std::string("99.12.31 23.59.59+05:30")}, std::string("1999-12-31")},
            {{std::string("2000/01/01T00/00/00-230")}, std::string("2000-01-01")},
            {{std::string("85 1 1T0 0 0. CST")}, std::string("1985-01-01")},
            {{std::string("2025/06/15T00:00:00.0-0")}, std::string("2025-06-15")},
            {{std::string("2025/06/15T00:00:00.99999999999")}, std::string("2025-06-15")},
            {{std::string("  2024-05-01T12:00:00  ")}, std::string("2024-05-01")},
            {{std::string("2024.05.01")}, std::string("2024-05-01")},
            {{std::string("2024.05.01 12.30.45")}, std::string("2024-05-01")},
            {{std::string("2024/05-01T12:30:45")}, std::string("2024-05-01")},
            {{std::string("2024-05/01 12.30.45")}, std::string("2024-05-01")},
            {{std::string(" 2024-05-01 ")}, std::string("2024-05-01")},
            {{std::string("2024|05|01")}, std::string("2024-05-01")},
            {{std::string("2024^05^01")}, std::string("2024-05-01")},
            {{std::string("2024~05~01 12~00~00")}, std::string("2024-05-01")},
            {{std::string("2024#05#01T12#00#00")}, std::string("2024-05-01")},

            // invalid -> NULL
            {{std::string("19991231T235960.5UTC")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024年05月01日")}, Null()},
            {{std::string("2024//05//01")}, Null()},
            {{std::string("2024- 05- 01")}, Null()},
            {{std::string("123.123")}, Null()},
            {{std::string("")}, Null()},
            {{std::string("   ")}, Null()},
    };
    check_function_for_cast<DataTypeDate>(input_types, data_set);
}

// ===========================================================================
// Cluster D: from_string manual parser -> DATETIME (strict mode, valid)
// Reuses the v2 datetime ground-truth inputs; expected values drop microseconds
// and keep second-level carry. Timezone results assume Asia/Shanghai (test tz).
// ===========================================================================
TEST_F(FunctionCastTest, v1_string_to_datetime_valid_case_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2023-07-16T19:20:30.123+08:00")}, std::string("2023-07-16 19:20:30")},
            {{std::string("2023-07-16T19+08:00")}, std::string("2023-07-16 19:00:00")},
            {{std::string("2023-07-16T1920+08:00")}, std::string("2023-07-16 19:20:00")},
            {{std::string("70-1-1T00:00:00-0000")}, std::string("1970-01-01 08:00:00")},

            {{std::string("2024-02-29 12:00:00 Europe/Paris")}, std::string("2024-02-29 19:00:00")},
            {{std::string("2024-05-01T00:00Asia/Shanghai")}, std::string("2024-05-01 00:00:00")},
            {{std::string("20231005T081530Europe/London")}, std::string("2023-10-05 15:15:30")},
            {{std::string("85-12-25T0000gMt")}, std::string("1985-12-25 08:00:00")},

            {{std::string("2024-05-01")}, std::string("2024-05-01 00:00:00")},
            {{std::string("24-5-1")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024-05-01 0:1:2.333")}, std::string("2024-05-01 00:01:02")},
            {{std::string("2024-05-01 0:1:2.")}, std::string("2024-05-01 00:01:02")},

            {{std::string("20240501 01")}, std::string("2024-05-01 01:00:00")},
            {{std::string("20230716 1920Z")}, std::string("2023-07-17 03:20:00")},
            {{std::string("20240501T0000")}, std::string("2024-05-01 00:00:00")},

            // fractional carry rolls seconds (and the whole calendar) over
            {{std::string("2024-12-31 23:59:59.9999999999999999999")},
             std::string("2025-01-01 00:00:00")},

            {{std::string("2020-12-12 13:12:12-03:00")}, std::string("2020-12-13 00:12:12")},
            // historical Shanghai LMT before 1900 is +08:05:43, not +08:00:00
            {{std::string("0023-01-01T00:00Z")}, std::string("0023-01-01 08:05:43")},

            {{std::string("69-12-31")}, std::string("2069-12-31 00:00:00")},
            {{std::string("70-01-01")}, std::string("1970-01-01 00:00:00")},
            {{std::string("68-01-01")}, std::string("2068-01-01 00:00:00")},
            {{std::string("69-01-01")}, std::string("2069-01-01 00:00:00")},
            {{std::string("71-01-01")}, std::string("1971-01-01 00:00:00")},
            {{std::string("99-12-31")}, std::string("1999-12-31 00:00:00")},
            {{std::string("00-01-01")}, std::string("2000-01-01 00:00:00")},
            {{std::string("20-1-1")}, std::string("2020-01-01 00:00:00")},

            {{std::string("230102")}, std::string("2023-01-02 00:00:00")},
            {{std::string("19230101")}, std::string("1923-01-01 00:00:00")},
            {{std::string("20120102030405")}, std::string("2012-01-02 03:04:05")},
            {{std::string("0123-12-12")}, std::string("0123-12-12 00:00:00")},
            {{std::string("01231212")}, std::string("0123-12-12 00:00:00")},
            {{std::string("12010203040506.999")}, std::string("1201-02-03 04:05:06")},
            {{std::string("12010203040506.")}, std::string("1201-02-03 04:05:06")},

            // exactly 6 fractional digits -> no carry
            {{std::string("2024-02-29T23:59:59.999999 UTC")}, std::string("2024-03-01 07:59:59")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1969-12-31 18:00:00")},
            {{std::string("0023-1-1T1:2:3. -00:00")}, std::string("0023-01-01 09:07:46")},

            {{std::string("2024/05/01")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024/05-01T12:30:45")}, std::string("2024-05-01 12:30:45")},
            // >6 fractional digits, digit7>=5 -> carry seconds
            {{std::string("2025/06/15T00:00:00.99999999999999")},
             std::string("2025-06-15 00:00:01")},
    };
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
}

// Cluster D + range/overflow error paths: from_string -> DATETIME (strict, invalid)
TEST_F(FunctionCastTest, v1_string_to_datetime_invalid_case_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("abc")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("2023-07-16T19.123+08:00")}, Null()},
            {{std::string("24012")}, Null()},
            {{std::string("2411 123")}, Null()},
            {{std::string("2024-05-01 01:030:02")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-0131T12:00")}, Null()},
            {{std::string("2024-05-01@00:00")}, Null()},
            {{std::string("20120212051")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024-5-1T24:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T12:60")}, Null()},
            {{std::string("2012-06-30T23:59:60")}, Null()},
            {{std::string("2024-05-01T00:00+14:30")}, Null()},
            {{std::string("2024-05-01T00:00+08:25")}, Null()},
            {{std::string("2020-12-12   12:12:12")}, Null()},
            {{std::string("2020-12-12T 12:12:12")}, Null()},
            {{std::string("2011")}, Null()},
            {{std::string("123-12-12")}, Null()},
            {{std::string("1-12-12")}, Null()},
            {{std::string("00123-12-12")}, Null()},
            {{std::string("1231212")}, Null()},
            {{std::string("")}, Null()},
            {{std::string("   ")}, Null()},
            {{std::string("2024.05.01")}, Null()},
            {{std::string("2024.05.01 12.30.45")}, Null()},
            {{std::string("-1")}, Null()},
            {{std::string("-1234")}, Null()},
    };
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set,
                                                          "to DateTime failed");
}

// ===========================================================================
// Cluster E: from_string_non_strict_mode_impl -> DATETIME
// ===========================================================================
TEST_F(FunctionCastTest, v1_string_to_datetime_non_strict_mode) {
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR};
    DataSet data_set = {
            {{std::string("2023-7-4T9-5-3.1Z")}, std::string("2023-07-04 17:05:03")},
            {{std::string("99.12.31 23.59.59+05:30")}, std::string("2000-01-01 02:29:59")},
            {{std::string("2000/01/01T00/00/00")}, std::string("2000-01-01 00:00:00")},
            {{std::string("85 1 1T0 0 0. CST")}, std::string("1985-01-01 00:00:00")},
            {{std::string("2024-02-29T23:59:59.999999Z")}, std::string("2024-03-01 07:59:59")},
            {{std::string("70-01-01T00:00:00+14")}, std::string("1969-12-31 18:00:00")},
            {{std::string("2025/06/15T00:00:00")}, std::string("2025-06-15 00:00:00")},
            {{std::string("2025/06/15T00:00:00.99999999999")}, std::string("2025-06-15 00:00:01")},
            {{std::string("2025/06/15T00:00:00.0-0")}, std::string("2025-06-15 08:00:00")},
            {{std::string("  2024-05-01T12:00:00  ")}, std::string("2024-05-01 12:00:00")},
            {{std::string("2024.05.01")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024.05.01 12.30.45")}, std::string("2024-05-01 12:30:45")},
            {{std::string("2024/05-01T12:30:45")}, std::string("2024-05-01 12:30:45")},
            {{std::string("2024-05/01 12.30.45")}, std::string("2024-05-01 12:30:45")},
            {{std::string(" 2024-05-01 ")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024|05|01")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024^05^01")}, std::string("2024-05-01 00:00:00")},
            {{std::string("2024~05~01 12~00~00")}, std::string("2024-05-01 12:00:00")},
            {{std::string("2024#05#01T12#00#00")}, std::string("2024-05-01 12:00:00")},

            // invalid minute offset (05 not in {0,30,45}) -> NULL
            {{std::string("0023-1-1T1:2:3. -08:05:43")}, Null()},
            // invalid -> NULL
            {{std::string("19991231T235960.5UTC")}, Null()},
            {{std::string("2020-05-05 12:30:60")}, Null()},
            {{std::string("10000-01-01 00:00:00")}, Null()},
            {{std::string("2024-02-30")}, Null()},
            {{std::string("2024-05-01T00:00XYZ")}, Null()},
            {{std::string("2024//05//01")}, Null()},
            {{std::string("")}, Null()},
    };
    check_function_for_cast<DataTypeDateTime>(input_types, data_set);
}

// ===========================================================================
// Cluster A: from_integer length branches -> DATE / DATETIME (valid)
// Covers length 3, 4, 5, 6 (year<70 and year>=70), 8, 14.
// ===========================================================================
TEST_F(FunctionCastTest, v1_from_int_to_date_valid) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    DataSet data_set = {
            {{int64_t(123)}, std::string("2000-01-23")},            // length 3
            {{int64_t(1231)}, std::string("2000-12-31")},           // length 4
            {{int64_t(91231)}, std::string("2009-12-31")},          // length 5
            {{int64_t(230102)}, std::string("2023-01-02")},         // length 6, year<70 -> 20xx
            {{int64_t(991231)}, std::string("1999-12-31")},         // length 6, year>=70 -> 19xx
            {{int64_t(19230101)}, std::string("1923-01-01")},       // length 8
            {{int64_t(20150102)}, std::string("2015-01-02")},       // length 8
            {{int64_t(20150102030405)}, std::string("2015-01-02")}, // length 14
    };
    // non-strict path (from_integer<NON_STRICT>)
    check_function_for_cast<DataTypeDate>(input_types, data_set);
    // strict path (from_integer<STRICT>) must also succeed
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
}

TEST_F(FunctionCastTest, v1_from_int_to_datetime_valid) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    DataSet data_set = {
            {{int64_t(123)}, std::string("2000-01-23 00:00:00")},
            {{int64_t(1231)}, std::string("2000-12-31 00:00:00")},
            {{int64_t(91231)}, std::string("2009-12-31 00:00:00")},
            {{int64_t(230102)}, std::string("2023-01-02 00:00:00")},
            {{int64_t(991231)}, std::string("1999-12-31 00:00:00")},
            {{int64_t(19230101)}, std::string("1923-01-01 00:00:00")},
            {{int64_t(20150102)}, std::string("2015-01-02 00:00:00")},
            {{int64_t(20150102030405)}, std::string("2015-01-02 03:04:05")},
    };
    check_function_for_cast<DataTypeDateTime>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
}

// Cluster A: from_integer error paths (invalid length and range branches).
// All entries are invalid for both DATE and DATETIME targets.
TEST_F(FunctionCastTest, v1_from_int_to_date_invalid) {
    InputTypeSet input_types = {PrimitiveType::TYPE_BIGINT};
    DataSet data_set = {
            {{int64_t(1)}, Null()},                // length 1 -> invalid digits
            {{int64_t(22)}, Null()},               // length 2 -> invalid digits
            {{int64_t(-222)}, Null()},             // negative -> invalid int value
            {{int64_t(7777777)}, Null()},          // length 7 -> invalid digits
            {{int64_t(2015010203040516)}, Null()}, // length 16 -> invalid digits
            {{int64_t(100)}, Null()},              // length 3 -> day 0
            {{int64_t(1300)}, Null()},             // length 4 -> month 13
            {{int64_t(1240)}, Null()},             // length 4 -> day 40
            {{int64_t(99999)}, Null()},            // length 5 -> month 99
            {{int64_t(90230)}, Null()},            // length 5 -> Feb 30
            {{int64_t(130230)}, Null()},           // length 6 -> Feb 30
            {{int64_t(20151301)}, Null()},         // length 8 -> month 13
            {{int64_t(20150230)}, Null()},         // length 8 -> Feb 30
            {{int64_t(20150102250000)}, Null()},   // length 14 -> hour 25
            {{int64_t(20150102236000)}, Null()},   // length 14 -> minute 60
            {{int64_t(20150102235960)}, Null()},   // length 14 -> second 60
    };
    check_function_for_cast<DataTypeDate>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set, "to Date failed");
    check_function_for_cast<DataTypeDateTime>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set,
                                                          "to DateTime failed");
}

// ===========================================================================
// Cluster B: from_float -> DATE / DATETIME (valid + invalid)
// ===========================================================================
TEST_F(FunctionCastTest, v1_from_float_to_date) {
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{123.123}, std::string("2000-01-23")},
            {{20150102030405.0}, std::string("2015-01-02")},
            {{20150102030405.123456}, std::string("2015-01-02")},
    };
    check_function_for_cast<DataTypeDate>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
}

TEST_F(FunctionCastTest, v1_from_float_to_datetime) {
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{123.123}, std::string("2000-01-23 00:00:00")},
            {{20150102030405.0}, std::string("2015-01-02 03:04:05")},
            // fractional part rounds at microsecond level only; seconds unchanged
            {{20150102030405.123456}, std::string("2015-01-02 03:04:05")},
    };
    check_function_for_cast<DataTypeDateTime>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
}

TEST_F(FunctionCastTest, v1_from_float_invalid) {
    InputTypeSet input_types = {PrimitiveType::TYPE_DOUBLE};
    DataSet data_set = {
            {{1.}, Null()},                // length 1
            {{22.223}, Null()},            // length 2
            {{-222.}, Null()},             // non-positive
            {{7777777.}, Null()},          // length 7
            {{2015010203040516.}, Null()}, // length 16
            {{1000.0}, Null()},            // length 4 -> day 0
    };
    check_function_for_cast<DataTypeDate>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set, "to Date failed");
    check_function_for_cast<DataTypeDateTime>(input_types, data_set);
    check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set,
                                                          "to DateTime failed");
}

// ===========================================================================
// Cluster C: from_decimal -> DATE / DATETIME and microsecond_carry_on
// ===========================================================================
TEST_F(FunctionCastTest, v1_from_decimal_to_date) {
    // Decimal(10,3)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 10}};
        DataSet data_set = {
                {{DECIMAL64(123, 123, 3)}, std::string("2000-01-23")},
                {{DECIMAL64(20150102, 123, 3)}, std::string("2015-01-02")},
                {{DECIMAL64(20151231, 999, 3)}, std::string("2015-12-31")},
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
    }
    // Decimal(18,6)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
        DataSet data_set = {
                {{DECIMAL64(20150102, 123456, 6)}, std::string("2015-01-02")},
                {{DECIMAL64(20151231, 999999, 6)}, std::string("2015-12-31")},
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
    }
    // Decimal(17,3) -> max datetime; fractional ignored for DATE
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 17}};
        DataSet data_set = {
                {{DECIMAL64(99991231235959, 999, 3)}, std::string("9999-12-31")},
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
    }
    // Decimal(21,7) -> fraction ignored for DATE (no carry observable)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL128I, 7, 21}};
        DataSet data_set = {
                {{DECIMAL128V3(20150102030405, 9999999, 7)}, std::string("2015-01-02")},
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set);
    }
}

TEST_F(FunctionCastTest, v1_from_decimal_to_datetime) {
    // Decimal(10,3)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 10}};
        DataSet data_set = {
                {{DECIMAL64(123, 123, 3)}, std::string("2000-01-23 00:00:00")},
                {{DECIMAL64(20150102, 123, 3)}, std::string("2015-01-02 00:00:00")},
                {{DECIMAL64(20151231, 999, 3)}, std::string("2015-12-31 00:00:00")},
        };
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
    }
    // Decimal(18,6)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
        DataSet data_set = {
                {{DECIMAL64(20150102, 123456, 6)}, std::string("2015-01-02 00:00:00")},
                {{DECIMAL64(20151231, 999999, 6)}, std::string("2015-12-31 00:00:00")},
        };
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
    }
    // Decimal(17,3) -> max datetime, no rounding (scale<=6)
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 17}};
        DataSet data_set = {
                {{DECIMAL64(99991231235959, 999, 3)}, std::string("9999-12-31 23:59:59")},
        };
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
    }
    // Decimal(21,7) -> exercises microsecond_carry_on rounding paths:
    //   1000000 -> digit7=0 (<5), no increment
    //   1234565 -> digit7=5 (>=5), increment without overflow
    //   9999999 -> digit7=9 (>=5), ms overflow -> carry +1 second
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL128I, 7, 21}};
        DataSet data_set = {
                {{DECIMAL128V3(20150102030405, 1000000, 7)}, std::string("2015-01-02 03:04:05")},
                {{DECIMAL128V3(20150102030405, 1234565, 7)}, std::string("2015-01-02 03:04:05")},
                {{DECIMAL128V3(20150102030405, 9999999, 7)}, std::string("2015-01-02 03:04:06")},
        };
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set);
    }
}

// Cluster C: from_decimal error paths (invalid month/day -> NULL / error)
TEST_F(FunctionCastTest, v1_from_decimal_invalid) {
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 3, 10}};
        DataSet data_set = {
                {{DECIMAL64(1000, 0, 3)}, Null()}, // length 4 -> day 0
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set, "to Date failed");
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set,
                                                              "to DateTime failed");
    }
    {
        InputTypeSet input_types = {{PrimitiveType::TYPE_DECIMAL64, 6, 18}};
        DataSet data_set = {
                {{DECIMAL64(123123, 123456, 6)}, Null()}, // length 6 -> month 31
        };
        check_function_for_cast<DataTypeDate>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDate>(input_types, data_set, "to Date failed");
        check_function_for_cast<DataTypeDateTime>(input_types, data_set);
        check_function_for_cast_strict_mode<DataTypeDateTime>(input_types, data_set,
                                                              "to DateTime failed");
    }
}

} // namespace doris
