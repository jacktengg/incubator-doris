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

| Kernel/rows/NULL%/mode/groups | V1 us | V2 us | Migrated us [min,max] | vs V2 | Heap requests/op V2 → migrated | Heap bytes/op V2 → migrated |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| streaming/nullable_sum/4096/0/1 | 43.523 | 6.421 | 1.708 [1.618,1.908] | -73.4% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/1/1 | 43.631 | 7.241 | 1.680 [1.626,1.764] | -76.8% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/50/1 | 37.210 | 6.554 | 1.669 [1.635,1.700] | -74.5% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_sum/4096/100/1 | 26.667 | 5.225 | 1.000 [0.901,1.712] | -80.9% | 7 → 5 | 139392 → 73856 |
| streaming/nullable_avg/4096/0/1 | 64.156 | 17.765 | 9.279 [9.178,9.355] | -47.8% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/1/1 | 64.315 | 18.272 | 9.135 [9.108,9.165] | -50.0% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/50/1 | 46.768 | 16.621 | 6.459 [6.446,6.469] | -61.1% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_avg/4096/100/1 | 27.434 | 14.189 | 3.461 [3.448,3.472] | -75.6% | 7 → 5 | 434304 → 270464 |
| merge/nullable_sum/4096/0/1/64 | 6.703 | 4.359 | 3.915 [3.740,4.139] | -10.2% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/0/1/4096 | 13.675 | 11.795 | 10.845 [10.780,10.905] | -8.1% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/1/1/64 | 6.785 | 4.406 | 3.600 [3.551,3.674] | -18.3% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/1/1/4096 | 13.710 | 12.083 | 10.997 [10.851,11.170] | -9.0% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/50/1/64 | 5.814 | 4.467 | 2.804 [2.570,3.271] | -37.2% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/50/1/4096 | 12.961 | 12.051 | 9.862 [9.704,10.042] | -18.2% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/100/1/64 | 3.544 | 4.417 | 1.552 [1.498,1.676] | -64.9% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_sum/4096/100/1/4096 | 10.333 | 11.936 | 8.290 [8.231,8.462] | -30.5% | 0.0625 → 0 | 4096 → 0 |
| merge/nullable_avg/4096/0/1/64 | 12.253 | 7.474 | 7.134 [7.063,7.278] | -4.5% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/0/1/4096 | 23.402 | 20.389 | 17.325 [17.104,17.573] | -15.0% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/1/1/64 | 12.185 | 7.746 | 7.036 [6.926,7.094] | -9.2% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/1/1/4096 | 23.071 | 20.342 | 17.152 [16.983,17.216] | -15.7% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/50/1/64 | 9.630 | 7.560 | 4.358 [4.303,4.433] | -42.4% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/50/1/4096 | 20.202 | 20.392 | 14.766 [14.706,14.858] | -27.6% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/100/1/64 | 6.746 | 7.559 | 2.720 [1.899,4.730] | -64.0% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_avg/4096/100/1/4096 | 17.280 | 20.430 | 11.820 [11.745,11.911] | -42.1% | 0.0625 → 0 | 12288 → 0 |
| streaming/nullable_decimal_sum/4096/0/1 | 50.787 | 10.318 | 3.393 [3.365,3.422] | -67.1% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/1/1 | 50.750 | 11.084 | 3.399 [3.349,3.463] | -69.3% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/50/1 | 41.226 | 9.396 | 3.617 [3.604,3.635] | -61.5% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_sum/4096/100/1 | 26.936 | 7.597 | 1.980 [1.975,1.989] | -73.9% | 7 → 5 | 237696 → 139392 |
| streaming/nullable_decimal_avg/4096/0/1 | 64.765 | 17.978 | 9.113 [9.086,9.134] | -49.3% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/1/1 | 64.393 | 19.287 | 9.091 [9.065,9.125] | -52.9% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/50/1 | 48.450 | 17.003 | 6.377 [6.313,6.426] | -62.5% | 7 → 5 | 434304 → 270464 |
| streaming/nullable_decimal_avg/4096/100/1 | 27.671 | 15.484 | 3.516 [3.463,3.590] | -77.3% | 7 → 5 | 434304 → 270464 |
| merge/nullable_decimal_sum/4096/0/1/64 | 9.389 | 5.900 | 5.828 [5.615,6.100] | -1.2% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/0/1/4096 | 17.213 | 14.502 | 13.247 [12.977,13.524] | -8.7% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/1/1/64 | 9.237 | 5.960 | 5.952 [5.644,6.174] | -0.1% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/1/1/4096 | 17.522 | 14.514 | 13.008 [12.975,13.040] | -10.4% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/50/1/64 | 6.867 | 5.938 | 3.711 [3.581,3.814] | -37.5% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/50/1/4096 | 14.763 | 14.669 | 10.872 [10.760,10.973] | -25.9% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/100/1/64 | 4.809 | 6.006 | 1.514 [1.503,1.533] | -74.8% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_sum/4096/100/1/4096 | 11.852 | 14.538 | 8.398 [8.317,8.446] | -42.2% | 0.0625 → 0 | 8192 → 0 |
| merge/nullable_decimal_avg/4096/0/1/64 | 12.260 | 7.309 | 7.121 [7.029,7.233] | -2.6% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/0/1/4096 | 23.407 | 20.300 | 20.138 [17.575,22.927] | -0.8% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/1/1/64 | 12.252 | 7.422 | 7.947 [6.854,13.530] | +7.1% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/1/1/4096 | 23.673 | 20.425 | 17.397 [17.299,17.468] | -14.8% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/50/1/64 | 9.529 | 7.422 | 4.307 [4.255,4.415] | -42.0% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/50/1/4096 | 20.231 | 20.293 | 14.933 [14.798,15.058] | -26.4% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/100/1/64 | 6.797 | 7.390 | 1.532 [1.507,1.560] | -79.3% | 0.0625 → 0 | 12288 → 0 |
| merge/nullable_decimal_avg/4096/100/1/4096 | 16.880 | 20.436 | 11.556 [11.470,11.658] | -43.5% | 0.0625 → 0 | 12288 → 0 |
