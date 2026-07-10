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

# A /19 provides one non-overlapping /24 for every bridge-based compose stack.
THIRDPARTY_DOCKER_SUBNET_VARIABLES=(
    DOCKER_HUDI_SUBNET
    DOCKER_ICEBERG_REST_SUBNET
    DOCKER_POLARIS_SUBNET
    DOCKER_OCEANBASE_SUBNET
    DOCKER_MYSQL_57_SUBNET
    DOCKER_CLICKHOUSE_SUBNET
    DOCKER_ES_SUBNET
    DOCKER_ICEBERG_SUBNET
    DOCKER_MARIADB_SUBNET
    DOCKER_ORACLE_SUBNET
    DOCKER_POSTGRESQL_SUBNET
    DOCKER_SQLSERVER_SUBNET
    DOCKER_MINIO_SUBNET
    DOCKER_RANGER_SUBNET
    DOCKER_DB2_SUBNET
    DOCKER_KAFKA_SUBNET
    DOCKER_LAKESOUL_SUBNET
)

docker_network_ipv4_to_int() {
    local ipv4="$1"
    local first
    local second
    local third
    local fourth
    local extra

    IFS=. read -r first second third fourth extra <<<"${ipv4}"
    if [[ -n "${extra}" || ! "${first}" =~ ^[0-9]+$ || ! "${second}" =~ ^[0-9]+$ ||
            ! "${third}" =~ ^[0-9]+$ || ! "${fourth}" =~ ^[0-9]+$ ]]; then
        return 1
    fi
    first=$((10#${first}))
    second=$((10#${second}))
    third=$((10#${third}))
    fourth=$((10#${fourth}))
    if ((first > 255 || second > 255 || third > 255 || fourth > 255)); then
        return 1
    fi

    echo $(((first << 24) + (second << 16) + (third << 8) + fourth))
}

docker_network_int_to_ipv4() {
    local value="$1"
    echo "$(((value >> 24) & 255)).$(((value >> 16) & 255)).$(((value >> 8) & 255)).$((value & 255))"
}

docker_network_cidr_bounds() {
    local cidr="$1"
    local ipv4="${cidr%/*}"
    local prefix=32
    local ipv4_int
    local size
    local start

    if [[ "${cidr}" == */* ]]; then
        prefix="${cidr#*/}"
    fi
    if [[ ! "${prefix}" =~ ^[0-9]+$ ]]; then
        return 1
    fi
    prefix=$((10#${prefix}))
    if ((prefix > 32)); then
        return 1
    fi
    ipv4_int="$(docker_network_ipv4_to_int "${ipv4}")" || return 1
    size=$((1 << (32 - prefix)))
    start=$((ipv4_int / size * size))
    echo "${start} $((start + size - 1))"
}

docker_network_block_is_available() {
    local candidate="$1"
    shift
    local candidate_start
    local candidate_end
    local occupied
    local occupied_start
    local occupied_end

    read -r candidate_start candidate_end < <(docker_network_cidr_bounds "${candidate}") || return 1
    for occupied in "$@"; do
        [[ -n "${occupied}" ]] || continue
        read -r occupied_start occupied_end < <(docker_network_cidr_bounds "${occupied}") || {
            echo "Invalid occupied IPv4 CIDR: ${occupied}" >&2
            return 1
        }
        if ((candidate_start <= occupied_end && occupied_start <= candidate_end)); then
            return 1
        fi
    done
}

docker_network_find_available_block() {
    local -a occupied_cidrs=("$@")
    local -a occupied_starts=()
    local -a occupied_ends=()
    local occupied
    local occupied_start
    local occupied_end
    local -a private_pools=("172.16.0.0/12" "192.168.0.0/16" "10.0.0.0/8")
    local private_pool
    local pool_start
    local pool_end
    local candidate_start
    local candidate_end
    local index
    local available

    for occupied in "${occupied_cidrs[@]}"; do
        [[ -n "${occupied}" ]] || continue
        read -r occupied_start occupied_end < <(docker_network_cidr_bounds "${occupied}") || {
            echo "Invalid occupied IPv4 CIDR: ${occupied}" >&2
            return 1
        }
        occupied_starts+=("${occupied_start}")
        occupied_ends+=("${occupied_end}")
    done

    for private_pool in "${private_pools[@]}"; do
        read -r pool_start pool_end < <(docker_network_cidr_bounds "${private_pool}")
        for ((candidate_start = pool_start; candidate_start <= pool_end; candidate_start += 8192)); do
            candidate_end=$((candidate_start + 8191))
            available=1
            for index in "${!occupied_starts[@]}"; do
                if ((candidate_start <= occupied_ends[index] && occupied_starts[index] <= candidate_end)); then
                    available=0
                    break
                fi
            done
            if ((available)); then
                echo "$(docker_network_int_to_ipv4 "${candidate_start}")/19"
                return 0
            fi
        done
    done

    echo "No available /19 block in the RFC1918 address space" >&2
    return 1
}

docker_network_subnet_at() {
    local block="$1"
    local index="$2"
    local block_start
    local subnet_start

    read -r block_start _ < <(docker_network_cidr_bounds "${block}") || return 1
    if [[ "${block#*/}" != "19" ]] || ((index < 0 || index >= 32)); then
        return 1
    fi
    subnet_start=$((block_start + index * 256))
    echo "$(docker_network_int_to_ipv4 "${subnet_start}")/24"
}

docker_network_collect_occupied_cidrs() {
    local network_ids
    local network_subnets
    local routes

    command -v ip >/dev/null 2>&1 || {
        echo "The ip command is required to detect host and VPN routes" >&2
        return 1
    }

    routes="$(ip -4 route show table all)" || {
        echo "Failed to inspect host IPv4 routes" >&2
        return 1
    }
    awk '{
        destination = $1
        if ($1 == "local" || $1 == "broadcast" || $1 == "unreachable" ||
                $1 == "blackhole" || $1 == "prohibit" || $1 == "throw") {
            destination = $2
        }
        if (destination ~ /^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+(\/[0-9]+)?$/) {
            print destination
        }
    }' <<<"${routes}"

    network_ids="$(sudo docker network ls -q)" || {
        echo "Failed to list Docker networks" >&2
        return 1
    }
    if [[ -n "${network_ids}" ]]; then
        # shellcheck disable=SC2086
        network_subnets="$(sudo docker network inspect ${network_ids} \
            --format '{{range .IPAM.Config}}{{if .Subnet}}{{println .Subnet}}{{end}}{{end}}')" || {
            echo "Failed to inspect Docker network subnets" >&2
            return 1
        }
        awk '/^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+\/[0-9]+$/' <<<"${network_subnets}"
    fi
}

configure_thirdparty_docker_subnets() {
    local -a occupied_cidrs=("$@")
    local block
    local index
    local variable
    local subnet

    block="$(docker_network_find_available_block "${occupied_cidrs[@]}")" || return 1
    for index in "${!THIRDPARTY_DOCKER_SUBNET_VARIABLES[@]}"; do
        variable="${THIRDPARTY_DOCKER_SUBNET_VARIABLES[$index]}"
        subnet="$(docker_network_subnet_at "${block}" "${index}")" || return 1
        printf -v "${variable}" '%s' "${subnet}"
        export "${variable}"
    done
    export THIRDPARTY_DOCKER_SUBNET_BLOCK="${block}"
}
