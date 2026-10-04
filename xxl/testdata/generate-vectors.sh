#!/usr/bin/env bash
set -euo pipefail

m2_root="${HOME}/.m2/repository"
classpath="${m2_root}/com/caucho/hessian/4.0.63/hessian-4.0.63.jar:${m2_root}/com/xuxueli/xxl-rpc-core/1.5.0/xxl-rpc-core-1.5.0.jar:${m2_root}/com/xuxueli/xxl-job-core/2.1.2/xxl-job-core-2.1.2.jar"
script_dir="$(cd "$(dirname "$0")" && pwd)"

javac -cp "$classpath" -d "$script_dir" "$script_dir/XxlVector.java"
java -cp "$script_dir:$classpath" XxlVector > "$script_dir/java-vectors.txt"
rm -f "$script_dir/XxlVector.class"
