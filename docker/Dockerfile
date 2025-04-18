# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

WORKDIR /app

COPY . /app/
COPY ./docker/motd /etc/

RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt

RUN python setup.py install

RUN rm -rf ".git"
