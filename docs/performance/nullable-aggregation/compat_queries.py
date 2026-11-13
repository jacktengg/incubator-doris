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
"""Read-only checks at each explicitly selected FE/BE upgrade stage."""
import argparse
import json
import math
from pathlib import Path
import pymysql

parser = argparse.ArgumentParser()
parser.add_argument('label')
parser.add_argument('--port', type=int, default=22901)
parser.add_argument('--output', type=Path, default=Path(__file__).parent)
args = parser.parse_args()
label = args.label
connection = pymysql.connect(host='127.0.0.1', port=args.port, user='root', database='nullable_migration')
s = connection.cursor()
s.execute('set enable_agg_state=true')
s.execute('set parallel_pipeline_task_num=4')
s.execute('set agg_phase=2')
# Each switch value must execute on the selected backend. A cached FE result
# would not prove that the old and new wire formats both work.
s.execute('set enable_sql_cache=false')
queries = {
 'unary_variadic': "select id%7 g,sum(if(id%5=0,null,v)),avg(if(id%5=0,null,v)),avg_weighted(if(id%5=0,null,cast(v as double)),if(id%11=0,null,w)) from perf_input where id<16000 group by g order by g",
 'stored': "select g,sum_merge(s),avg_merge(a),avg_weighted_merge(w),array_size(array_agg_merge(ar)) from old_states group by g order by g",
 'window': "select id,sum(if(id<2,null,v)) over(order by id rows between 1 preceding and current row),avg(if(id<2,null,v)) over(order by id rows between 1 preceding and current row) from perf_input where id<12 order by id"
}
records = {'label': label, 'queries': queries, 'results': {}, 'sql_cache': False}
s.execute("admin show frontend config like 'be_exec_version'")
records['frontend_version_config'] = s.fetchall()
s.execute('show backends')
records['backend_columns'] = [d[0] for d in s.description]
records['backends'] = s.fetchall()
for flag in ['false', 'true']:
    s.execute('set enable_aggregate_function_null_v2='+flag)
    for name, sql in queries.items():
        s.execute(sql)
        records['results'][flag+'_'+name] = s.fetchall()
def equivalent(a, b):
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(equivalent(x,y) for x,y in zip(a,b))
    if isinstance(a, int) and isinstance(b, int):
        return a == b
    if isinstance(a, (int,float)) and isinstance(b, (int,float)):
        return math.isclose(a,b,rel_tol=1e-9,abs_tol=1e-9)
    return a == b

records = json.loads(json.dumps(records,default=str))
for name in queries:
    assert equivalent(records['results']['false_'+name],records['results']['true_'+name]), name
baseline = args.output/'old-fe-old-be-compatibility.json'
if label != 'old-fe-old-be':
    expected = json.loads(baseline.read_text())['results']
    for name,value in records['results'].items():
        assert equivalent(expected[name],value), (name,expected[name],value)
args.output.mkdir(parents=True, exist_ok=True)
path = args.output/(label+'-compatibility.json')
path.write_text(json.dumps(records, default=str, indent=2)+'\n')
print(label, 'PASS', path)
