#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
OUTFILE=machine.json

echo "Making ${OUTFILE}"
echo "** NOTE: This command fails on everything but OSX right now.      **"
echo "*  also, the intent is to make this script not suck, just not now. *"
echo "Please type GPU details here (or manually modify ${OUTFILE} later)."
read -p "GPU information string (or <enter>): " gpu_information
read -p "GPU part number (or <enter>): " gpu_part_number
read -p "GPU product name (or <enter>): " gpu_product_name


cat <<MACHINE_JSON > ${OUTFILE}
{
  "mac_address": "$(ifconfig en1 | awk '/ether/{print $2}')",
  "machine_name": "$(uname -n)",
  "memory_bytes": $(sysctl -n hw.memsize),
  "cpu_actual_frequency_hz": $(sysctl -n hw.cpufrequency),
  "os_name": "$(uname -s)",
  "architecture_name": "$(uname -m)",
  "kernel_name": "$(uname -r)",
  "cpu_model_name": "$(sysctl -n machdep.cpu.brand_string)",
  "cpu_core_count": $(sysctl -n hw.physicalcpu),
  "cpu_thread_count": $(sysctl -n hw.logicalcpu),
  "cpu_frequency_max_hz": $(sysctl -n hw.cpufrequency_max),
  "cpu_frequency_min_hz": $(sysctl -n hw.cpufrequency_min),
  "cpu_l1d_cache_bytes": $(sysctl -n hw.l1dcachesize),
  "cpu_l1i_cache_bytes": $(sysctl -n hw.l1icachesize),
  "cpu_l2_cache_bytes": $(sysctl -n hw.l2cachesize),
  "cpu_l3_cache_bytes": $(sysctl -n hw.l3cachesize),
  "gpu_information": "${gpu_information}",
  "gpu_part_number": "${gpu_part_number}",
  "gpu_product_name": "${gpu_product_name}"
}
MACHINE_JSON

echo "Machine details saved in ${OUTFILE}"
