#!/usr/bin/env bash
# ENV-01: 测试入口（与生产入口 python -m baostock_runner 严格区分）。
# - 强制 BAOSTOCK_OFFLINE=true，保证测试绝不登录真实 BaoStock / 不访问生产库。
# - 默认在 Python 3.12 隔离容器（Dockerfile.test-env）内运行；也可 -l 用本机 Python。
# 用法:
#   scripts/run_tests.sh            # Docker python:3.12 隔离测试
#   scripts/run_tests.sh -l         # 本机 Python（要求依赖已安装，推荐 venv）
#   scripts/run_tests.sh -l -k xxx  # 本机运行单个测试模式
set -euo pipefail
cd "$(dirname "$0")/.."

LOCAL=0
ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -l|--local) LOCAL=1; shift ;;
    *) ARGS+=("$1"); shift ;;
  esac
done

export BAOSTOCK_OFFLINE=true

if [[ $LOCAL -eq 1 ]]; then
  echo "[test] local python $(python3 --version 2>&1), offline forced"
  exec python3 -m unittest discover -s tests -v "${ARGS[@]}"
fi

echo "[test] building/using python:3.12 isolated image (Dockerfile.test-env)..."
docker build -q -f Dockerfile.test-env -t baostock-runner-test:env .
exec docker run --rm baostock-runner-test:env
