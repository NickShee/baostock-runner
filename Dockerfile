FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY baostock_runner ./baostock_runner
COPY README.md .

RUN useradd --create-home --uid 10001 runner \
    && mkdir -p /data \
    && chown -R runner:runner /app /data
USER runner

VOLUME ["/data"]
ENTRYPOINT ["python", "-m", "baostock_runner"]

