FROM node:22-alpine AS dashboard-builder

WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json* ./
RUN npm ci --no-audit --no-fund
COPY dashboard/ ./
RUN npm run build

FROM python:3.12-slim

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt requirements.lock* ./
# 优先使用精确锁定文件（ENV-01：固定已验证依赖版本），缺失时退回范围约束。
RUN if [ -f requirements.lock ]; then \
        http_proxy="${HTTP_PROXY}" https_proxy="${HTTPS_PROXY}" \
        HTTP_PROXY="${HTTP_PROXY}" HTTPS_PROXY="${HTTPS_PROXY}" NO_PROXY="${NO_PROXY}" \
        pip install --no-cache-dir -r requirements.lock; \
    else \
        http_proxy="${HTTP_PROXY}" https_proxy="${HTTPS_PROXY}" \
        HTTP_PROXY="${HTTP_PROXY}" HTTPS_PROXY="${HTTPS_PROXY}" NO_PROXY="${NO_PROXY}" \
        pip install --no-cache-dir -r requirements.txt; \
    fi

COPY baostock_runner ./baostock_runner
COPY --from=dashboard-builder /app/dashboard/dist ./baostock_runner/dashboard_dist
COPY README.md .

RUN useradd --create-home --uid 10001 runner \
    && mkdir -p /data \
    && chown -R runner:runner /app /data
USER runner

VOLUME ["/data"]
ENTRYPOINT ["python", "-m", "baostock_runner"]
