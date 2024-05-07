# syntax=docker/dockerfile:1
FROM fedora:37

USER root

WORKDIR /root

COPY simplyblock_core simplyblock_core
COPY simplyblock_web simplyblock_web
COPY simplyblock_cli simplyblock_cli
COPY env_var env_var
COPY setup.py setup.py
COPY README.md README.md
COPY requirements.txt requirements.txt
COPY ./docker/motd /etc/motd


RUN dnf update -y ; dnf install -y python3-pip kmod parted gdisk pkg-config wget vim dmidecode nvme-cli \
    iproute pciutils openssh-server iputils hostname

RUN wget https://github.com/apple/foundationdb/releases/download/7.3.3/foundationdb-clients-7.3.3-1.el7.x86_64.rpm
RUN rpm -Uvh foundationdb-clients-7.3.3-1.el7.x86_64.rpm
RUN rm -f foundationdb-clients-7.3.3-1.el7.x86_64.rpm


RUN pip3 install -r requirements.txt
RUN pip3 install -r simplyblock_web/requirements.txt
RUN ln -s /usr/bin/python3 /usr/bin/python
RUN python setup.py install

# Configure the SSH server
RUN mkdir /var/run/sshd && \
    echo 'root:123456' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
