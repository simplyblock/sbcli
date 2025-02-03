# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base-image

RUN apt-get update -y ;  apt-get install -y iptables

WORKDIR /app

COPY . /app/
COPY ./docker/motd /etc/

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

RUN python setup.py install
