FROM eclipse-temurin:17-jre-jammy

# ─────────────────────────────────────────
#  Heritrix 3 — Internet Archive Web Crawler
#  REST API available at https://localhost:8443
# ─────────────────────────────────────────

ENV HERITRIX_VERSION=3.4.0-20230222
ENV HERITRIX_HOME=/opt/heritrix

RUN apt-get update && \
    apt-get install -y --no-install-recommends wget unzip && \
    wget -q "https://repo1.maven.org/maven2/org/archive/heritrix/heritrix/${HERITRIX_VERSION}/heritrix-${HERITRIX_VERSION}-dist.zip" \
        -O /tmp/heritrix.zip && \
    mkdir -p /opt && \
    unzip /tmp/heritrix.zip -d /opt/ && \
    ln -s /opt/heritrix-${HERITRIX_VERSION} ${HERITRIX_HOME} && \
    chmod +x ${HERITRIX_HOME}/bin/heritrix && \
    rm /tmp/heritrix.zip && \
    apt-get purge -y wget unzip && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

WORKDIR ${HERITRIX_HOME}

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8443

ENTRYPOINT ["/entrypoint.sh"]
