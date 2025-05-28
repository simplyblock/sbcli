# syntax=docker/dockerfile:1
FROM simplyblock/simplyblock:base_image

WORKDIR /app

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY . /app

RUN python setup.py install
