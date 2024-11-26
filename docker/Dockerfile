# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base-image

WORKDIR /app

# pip install
COPY requirements.txt requirements.txt
COPY simplyblock_web/requirements.txt simplyblock_web/requirements.txt

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

# copy other artifacts
COPY simplyblock_core simplyblock_core
COPY simplyblock_web simplyblock_web
COPY simplyblock_cli simplyblock_cli
COPY env_var env_var
COPY setup.py setup.py
COPY README.md README.md
COPY ./docker/motd /etc/motd

# generate sbcli-dev
RUN python setup.py install
