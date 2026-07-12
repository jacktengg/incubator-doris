#!/bin/bash

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

set -euo pipefail

ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe
ORACLE_SID=XE
ORACLE_CPU_COUNT=${ORACLE_CPU_COUNT:-2}
PFILE=/tmp/doris-initXE.ora
SPFILE=/tmp/doris-spfileXE.ora

export ORACLE_HOME ORACLE_SID

cleanup() {
    rm -f "${PFILE}" "${SPFILE}"
}
trap cleanup EXIT

rm -f "${PFILE}" "${SPFILE}"
printf "CREATE PFILE='%s' FROM SPFILE;\nEXIT;\n" "${PFILE}" \
    | su -s /bin/bash oracle -c \
        "ORACLE_HOME=${ORACLE_HOME} ORACLE_SID=${ORACLE_SID} ${ORACLE_HOME}/bin/sqlplus -L -s / AS SYSDBA"

su -s /bin/bash oracle -c "sed -i '/^\\*\\.cpu_count=/d' ${PFILE}"
su -s /bin/bash oracle -c "printf '\n*.cpu_count=%s\n' '${ORACLE_CPU_COUNT}' >> ${PFILE}"

printf "CREATE SPFILE='%s' FROM PFILE='%s';\nEXIT;\n" "${SPFILE}" "${PFILE}" \
    | su -s /bin/bash oracle -c \
        "ORACLE_HOME=${ORACLE_HOME} ORACLE_SID=${ORACLE_SID} ${ORACLE_HOME}/bin/sqlplus -L -s / AS SYSDBA"
install -o oracle -g dba -m 640 "${SPFILE}" "${ORACLE_HOME}/dbs/spfileXE.ora"

exec /usr/sbin/startup.sh
