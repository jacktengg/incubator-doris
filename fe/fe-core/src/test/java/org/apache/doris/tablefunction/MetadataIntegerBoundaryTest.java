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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

class MetadataIntegerBoundaryTest {
    @Test
    void partitionCellMustFitDeclaredType() {
        // One partition string is enough. The wire uses intVal for all three SQL widths.
        Assertions.assertThrows(NumberFormatException.class, () -> rows(Type.TINYINT, "128"));
        Assertions.assertThrows(NumberFormatException.class, () -> rows(Type.TINYINT, "-129"));
        Assertions.assertThrows(NumberFormatException.class, () -> rows(Type.SMALLINT, "32768"));
        Assertions.assertThrows(NumberFormatException.class, () -> rows(Type.SMALLINT, "-32769"));
        Assertions.assertEquals(127, rows(Type.TINYINT, "127").get(0).column_value.get(0).intVal);
        Assertions.assertEquals(-32768, rows(Type.SMALLINT, "-32768").get(0).column_value.get(0).intVal);
    }

    private List<TRow> rows(Type type, String value) {
        return Deencapsulation.invoke(MetadataGenerator.class, "partitionValuesRows",
                List.of(new Column("p", type)), List.of("p"), Map.of("p=" + value, List.of(value)), "t");
    }
}
