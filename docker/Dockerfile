# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

# Create a non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

# Fix ownership to non-root user
RUN chown -R appuser:appgroup /app && \
    python setup.py install

USER appuser
