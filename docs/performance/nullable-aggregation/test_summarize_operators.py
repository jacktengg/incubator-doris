#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Duration parsing checks for the archived RuntimeProfile summaries."""
import unittest

from summarize_operators import duration_ms, operator_times


class ProfileDurationTest(unittest.TestCase):
    def test_complete_duration(self):
        for value, expected in [
            ('0', 0), ('300ns', 0.0003), ('0.125us', 0.000125),
            ('29.680ms', 29.680), ('1sec153ms', 1153), ('1s153ms', 1153),
            ('2min3sec', 123000), ('1h2m', 3720000),
            ('1hour2min3sec4ms5us6ns', 3723004.005006),
        ]:
            with self.subTest(value=value):
                self.assertAlmostEqual(duration_ms(value), expected, places=8)

    def test_operator_fields(self):
        self.assertEqual(
            operator_times('- ExecTime: avg 1sec128ms, max 1sec182ms, min 1sec37ms'),
            {'avg': 1128, 'max': 1182, 'min': 1037})

    def test_fe_nanosecond_remainders(self):
        # One recorded counter is sufficient. FE writes the remainder without
        # zero padding: 12 ms + 75 us becomes "12.75ms", not "12.075ms".
        # Decimal parsing would put the minimum above the reported maximum.
        self.assertEqual(
            operator_times('- ExecTime: avg 12.212ms, max 12.297ms, min 12.75ms'),
            {'avg': 12.212, 'max': 12.297, 'min': 12.075})
        for token, expected in [('121.93us', 0.121093), ('1.1ms', 1.001), ('1.750ms', 1.750)]:
            with self.subTest(token=token):
                values = operator_times(f'- ExecTime: avg {token}, max {token}, min {token}')
                self.assertAlmostEqual(values['avg'], expected, places=8)
        # BE PrettyPrinter text still uses ordinary decimal fractions.
        self.assertEqual(duration_ms('12.75ms'), 12.75)

    def test_reject_truncated_or_incomplete_values(self):
        for value in ['', '1sec153msjunk', '1sec 153ms', '1', '-1ms']:
            with self.subTest(value=value), self.assertRaises(ValueError):
                duration_ms(value)
        with self.assertRaises(ValueError):
            operator_times('- ExecTime: avg 1ms, max 2ms')


if __name__ == '__main__':
    unittest.main()
