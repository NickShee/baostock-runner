FROM python:3.12-slim

ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN http_proxy="${HTTP_PROXY}" \
    https_proxy="${HTTPS_PROXY}" \
    HTTP_PROXY="${HTTP_PROXY}" \
    HTTPS_PROXY="${HTTPS_PROXY}" \
    NO_PROXY="${NO_PROXY}" \
    pip install --no-cache-dir -r requirements.txt

COPY baostock_runner ./baostock_runner
COPY README.md .

RUN useradd --create-home --uid 10001 runner \
    && mkdir -p /data \
    && chown -R runner:runner /app /data
USER runner

VOLUME ["/data"]
ENTRYPOINT ["python", "-m", "baostock_runner"]
