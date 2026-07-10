#!/usr/bin/env bash
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

set -eo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." &>/dev/null && pwd)"
. "${ROOT}/docker-network-helpers.sh"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_eq() {
    local expected="$1"
    local actual="$2"
    [[ "${actual}" == "${expected}" ]] || fail "expected '${expected}', got '${actual}'"
}

assert_eq "172.16.32.0/19" "$(docker_network_find_available_block "172.16.0.1/32")"
assert_eq "172.16.32.0/19" "$(docker_network_find_available_block "172.16.16.0/20")"
assert_eq "10.128.0.0/19" "$(docker_network_find_available_block \
    "172.16.0.0/12" "192.168.0.0/16" "10.0.0.0/9")"

configure_thirdparty_docker_subnets "172.16.32.0/19"
assert_eq "172.16.0.0/19" "${THIRDPARTY_DOCKER_SUBNET_BLOCK}"
assert_eq "172.16.0.0/24" "${DOCKER_HUDI_SUBNET}"
assert_eq "172.16.15.0/24" "${DOCKER_KAFKA_SUBNET}"
assert_eq "172.16.16.0/24" "${DOCKER_LAKESOUL_SUBNET}"
subnet_count="$(compgen -A variable DOCKER_ | grep '_SUBNET$' | while read -r variable; do
    echo "${!variable}"
done | sort -u | wc -l)"
assert_eq "17" "${subnet_count}"

if docker_network_cidr_bounds "172.16.0.0/33" >/dev/null; then
    fail "expected an invalid prefix to fail"
fi
if docker_network_block_is_available "172.16.0.0/19" "172.16.31.255/32"; then
    fail "expected an overlapping host route to make the block unavailable"
fi

echo "PASS"
