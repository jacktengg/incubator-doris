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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.generator.Stack;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;

class FunctionParameterRangeTest {
    @Test
    void checkFullWidthIntegerRange() {
        // These values become 1 and 0 if validation first calls longValue().
        LargeIntLiteral wrapsToOne = new LargeIntLiteral(new BigInteger("-18446744073709551615"));
        LargeIntLiteral wrapsToZero = new LargeIntLiteral(new BigInteger("-18446744073709551616"));
        Assertions.assertFalse(wrapsToOne.isValueInRange(1, Long.MAX_VALUE));
        Assertions.assertFalse(wrapsToZero.isValueInRange(0, 9));
        // The post-rewrite check must also compare the original literal.
        Assertions.assertThrows(AnalysisException.class, new Ntile(wrapsToOne)::checkLegalityAfterRewrite);
        Assertions.assertTrue(new BigIntLiteral(Long.MIN_VALUE).isValueInRange(Long.MIN_VALUE, Long.MAX_VALUE));
        Assertions.assertTrue(new BigIntLiteral(Long.MAX_VALUE).isValueInRange(0, Long.MAX_VALUE));
        Assertions.assertTrue(new IntegerLiteral(0).isValueInRange(0, 0));
        Assertions.assertFalse(new LargeIntLiteral(new BigInteger("9223372036854775808"))
                .isValueInRange(0, Long.MAX_VALUE));
    }

    @Test
    void stackRejectsNegativeLargeIntBeforeComputingColumns() {
        // A single output value is enough to expose a row count that wraps to 1.
        Stack stack = new Stack(new LargeIntLiteral(new BigInteger("-18446744073709551615")),
                new IntegerLiteral(7));
        Assertions.assertThrows(AnalysisException.class, stack::getOutputColumnCount);
        Assertions.assertEquals(1, new Stack(new IntegerLiteral(1), new IntegerLiteral(7)).getOutputColumnCount());
        // Only derive the output schema. No rows or large arrays are created.
        Stack aboveMaximum = new Stack(new BigIntLiteral(2147483648L), new IntegerLiteral(7));
        Assertions.assertThrows(AnalysisException.class, aboveMaximum::getOutputColumnCount);
    }
}
