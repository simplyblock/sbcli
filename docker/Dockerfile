# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

WORKDIR /app

COPY requirements.txt .
COPY simplyblock_web/requirements.txt .

RUN --mount=type=cache,target=/root/.cache/pip pip3 install -r requirements.txt \
    && pip3 install -r simplyblock_web/requirements.txt

COPY . /app

RUN python setup.py install
