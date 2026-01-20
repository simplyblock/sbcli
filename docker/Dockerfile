# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_313

USER simplyblock
WORKDIR /app
COPY . /app

RUN sudo pip3 install -r requirements.txt

RUN sudo python setup.py install

RUN if [ -d /usr/share/terminfo ]; then \
       sudo find /usr/share/terminfo -lname '*ncr260vt300wpp*' -exec rm -f {} + ; \
       sudo rm -f /usr/share/terminfo/n/ncr260vt300wpp || true ; \
    fi

