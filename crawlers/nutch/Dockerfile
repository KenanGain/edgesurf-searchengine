FROM eclipse-temurin:17-jdk-jammy

# ─────────────────────────────────────────
#  Apache Nutch 1.20 — Batch Web Crawler
#  Wrapped with a Python REST API for management
# ─────────────────────────────────────────

ENV NUTCH_VERSION=1.20
ENV NUTCH_HOME=/opt/nutch
ENV NUTCH_DATA=/opt/nutch/data

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget python3 python3-pip python3-venv && \
    rm -rf /var/lib/apt/lists/*

# Download and install Apache Nutch
RUN wget -q "https://archive.apache.org/dist/nutch/${NUTCH_VERSION}/apache-nutch-${NUTCH_VERSION}-bin.tar.gz" \
        -O /tmp/nutch.tar.gz && \
    tar -xzf /tmp/nutch.tar.gz -C /opt/ && \
    mv /opt/apache-nutch-${NUTCH_VERSION} ${NUTCH_HOME} && \
    rm /tmp/nutch.tar.gz && \
    mkdir -p ${NUTCH_DATA}

# Configure Nutch
COPY nutch-site.xml ${NUTCH_HOME}/conf/nutch-site.xml

# Install Python wrapper dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY wrapper.py .

EXPOSE 8084
CMD ["python3", "-m", "uvicorn", "wrapper:app", "--host", "0.0.0.0", "--port", "8084"]
