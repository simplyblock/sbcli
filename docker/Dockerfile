# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_313

USER root
WORKDIR /app
COPY . /app

RUN pip3 install -r requirements.txt

RUN python setup.py install

RUN if [ -d /usr/share/terminfo ]; then \
       find /usr/share/terminfo -lname '*ncr260vt300wpp*' -exec rm -f {} + ; \
       rm -f /usr/share/terminfo/n/ncr260vt300wpp || true ; \
    fi

USER simplyblock
