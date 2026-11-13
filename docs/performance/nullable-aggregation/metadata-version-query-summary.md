<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

| Case | V1 ms | V2 ms | Migrated ms [min,max] | vs V2 | Peak MiB (V2 → migrated) |
| --- | ---: | ---: | ---: | ---: | ---: |
| numeric_low_null0 | 36.95 | 36.36 | 38.84 [32.30,49.12] | +6.8% | 244.0 → 233.0 |
| numeric_high_null0 | 187.57 | 175.71 | 167.09 [159.46,171.96] | -4.9% | 812.1 → 772.7 |
| variadic_null0 | 82.68 | 84.81 | 65.43 [63.12,67.39] | -22.8% | 308.2 → 276.9 |
| numeric_low_null1 | 34.87 | 36.96 | 38.80 [35.10,42.37] | +5.0% | 213.4 → 244.6 |
| numeric_high_null1 | 185.39 | 182.17 | 175.26 [163.41,196.76] | -3.8% | 764.3 → 772.3 |
| variadic_null1 | 83.20 | 88.33 | 74.46 [70.23,90.63] | -15.7% | 276.9 → 279.0 |
| numeric_low_null50 | 40.58 | 38.52 | 41.44 [38.92,43.09] | +7.6% | 234.6 → 240.1 |
| numeric_high_null50 | 200.03 | 195.93 | 172.86 [167.06,182.32] | -11.8% | 794.5 → 762.0 |
| variadic_null50 | 87.60 | 82.57 | 66.83 [64.32,75.72] | -19.1% | 285.3 → 285.5 |
| numeric_low_null100 | 33.32 | 33.27 | 36.15 [34.43,38.34] | +8.6% | 223.1 → 233.2 |
| numeric_high_null100 | 106.95 | 172.07 | 117.06 [108.18,132.39] | -32.0% | 781.3 → 811.4 |
| variadic_null100 | 64.58 | 65.10 | 51.91 [47.83,61.05] | -20.3% | 284.6 → 295.6 |
| wide_string_set | 1219.02 | 1243.99 | 1273.92 [1222.40,1367.05] | +2.4% | 2679.6 → 2673.9 |
| wide_string_streaming | 351.55 | 354.63 | 346.76 [324.79,372.60] | -2.2% | 1020.8 → 1039.9 |
| wide_topn | 537.22 | 549.17 | 620.76 [514.35,990.85] | +13.0% | 348.9 → 341.7 |
| stored_merge | 62.42 | 63.17 | 64.08 [58.13,74.55] | +1.4% | 188.3 → 188.2 |
| stored_union_finalize | 15.84 | 15.93 | 15.71 [13.11,18.78] | -1.3% | 218.8 → 218.8 |
| stored_bulk_merge | 45.58 | 44.68 | 47.04 [43.04,51.64] | +5.3% | 262.1 → 246.3 |
| stored_bulk_union_finalize | 29.45 | 29.96 | 29.81 [27.96,31.42] | -0.5% | 175.1 → 173.6 |
| stored_bulk_wide | 735.57 | 740.64 | 740.71 [715.75,754.39] | +0.0% | 1066.6 → 1036.5 |
| tpcds_sf1_q78 | 134.46 | 135.66 | 143.86 [121.53,170.14] | +6.0% | 1668.9 → 1668.6 |
