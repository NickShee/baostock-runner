#!/usr/bin/env bash
# ENV-01: 在 Python 3.12 隔离环境中生成精确依赖锁定文件 requirements.lock。
# 与生产容器同基镜像（python:3.12-slim），保证锁定版本与实际运行环境一致。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[lock] generating requirements.lock inside python:3.12-slim ..."
docker run --rm -v "$PWD":/src -w /src python:3.12-slim \
  bash -lc "pip install --quiet --no-cache-dir -r requirements.txt && pip freeze"
