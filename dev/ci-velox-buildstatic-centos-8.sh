#!/bin/bash

set -e

source /opt/rh/gcc-toolset-11/enable
export NUM_THREADS=4
if [ "$(uname -m)" = "aarch64" ]; then
    export CPU_TARGET="aarch64";
    export VCPKG_FORCE_SYSTEM_BINARIES=1;
fi

./dev/builddeps-veloxbe.sh --enable_vcpkg=ON --build_arrow=OFF --build_tests=OFF --build_benchmarks=OFF \
                           --build_examples=OFF --enable_s3=ON --enable_gcs=ON --enable_hdfs=ON --enable_abfs=ON
