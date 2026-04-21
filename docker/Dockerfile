# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_ubi10

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

RUN python3 setup.py install

USER simplyblock
