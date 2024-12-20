# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base-image

RUN apt-get update; apt-get install -y ssh systemctl; apt-get reinstall util-linux -y ; rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app/
COPY ./docker/motd /etc/

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

RUN python setup.py install
