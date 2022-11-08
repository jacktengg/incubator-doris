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

#include "vec/functions/function_date_or_datetime_computation.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
using FunctionDatetimeV2SubSeconds = FunctionDateOrDateTimeComputation<
        SubtractSecondsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMinutes = FunctionDateOrDateTimeComputation<
        SubtractMinutesImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubHours = FunctionDateOrDateTimeComputation<
        SubtractHoursImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubDays = FunctionDateOrDateTimeComputation<
        SubtractDaysImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubWeeks = FunctionDateOrDateTimeComputation<
        SubtractWeeksImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubMonths = FunctionDateOrDateTimeComputation<
        SubtractMonthsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubQuarters = FunctionDateOrDateTimeComputation<
        SubtractQuartersImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;
using FunctionDatetimeV2SubYears = FunctionDateOrDateTimeComputation<
        SubtractYearsImpl<DataTypeDateTimeV2, UInt64, DataTypeDateTimeV2>>;

void register_function_date_time_computation_v2_sub(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionDatetimeV2SubSeconds>();
    factory.register_function<FunctionDatetimeV2SubMinutes>();
    factory.register_function<FunctionDatetimeV2SubHours>();
    factory.register_function<FunctionDatetimeV2SubDays>();
    factory.register_function<FunctionDatetimeV2SubMonths>();
    factory.register_function<FunctionDatetimeV2SubYears>();
    factory.register_function<FunctionDatetimeV2SubQuarters>();
    factory.register_function<FunctionDatetimeV2SubWeeks>();
}

} // namespace doris::vectorized