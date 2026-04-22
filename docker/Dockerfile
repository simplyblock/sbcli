# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_ubi10

USER root

RUN dnf install -y iproute

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

RUN pip3 install -r requirements.txt --user

COPY . /app

RUN rm -rf /app/tests

RUN python3 setup.py install

RUN python3 setup.py install --user

USER simplyblock
