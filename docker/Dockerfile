# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_313

LABEL name="simplyblock"
LABEL vendor="Simplyblock"
LABEL version="1.0.0"
LABEL release="1"
LABEL summary="Simplyblock controlplane plane component"
LABEL description="Simplyblock controlplane plane container"
LABEL maintainer="developers@simplyblock.io"

COPY LICENSE /licenses/LICENSE
USER simplyblock
WORKDIR /app

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt


COPY . /app

RUN sudo python setup.py install

RUN if [ -d /usr/share/terminfo ]; then \
       find /usr/share/terminfo -lname '*ncr260vt300wpp*' -exec rm -f {} + ; \
       sudo rm -f /usr/share/terminfo/n/ncr260vt300wpp || true ; \
    fi

