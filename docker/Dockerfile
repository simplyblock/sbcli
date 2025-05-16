# syntax=docker/dockerfile:1
FROM python:3.9-slim

RUN apt-get update

RUN apt-get install -y wget nano

RUN rm -rf /var/lib/apt/lists/*

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" = "linux/arm64" ]; then \
        echo "ARM64 version"; \
        mkdir -p /usr/lib64/; \
        wget https://github.com/apple/foundationdb/releases/download/7.3.56/libfdb_c.aarch64.so -O /usr/lib64/libfdb_c.so ; \
    else \
        echo "AMD64 version"; \
    	wget https://github.com/apple/foundationdb/releases/download/7.3.56/libfdb_c.x86_64.so -O /usr/lib64/libfdb_c.so ; \
    fi

RUN pip install sbcli-dev

