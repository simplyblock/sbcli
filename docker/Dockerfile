# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image_ubi10

WORKDIR /app

COPY requirements.txt .

RUN sudo pip3 install -r requirements.txt

COPY . /app

RUN sudo python3 setup.py install
