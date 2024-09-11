FROM registry.sh.nextgenwaterprediction.com/ngwpc/hydrofabric/hydrofabric:develop

RUN dnf upgrade -y

# I'd much rather compile my own or know it is in the base, but this is quick for now...
RUN dnf install -y python311 python3.11-pip python3.11-devel git \
    && git config --global --add safe.directory /workspace

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3.11 1

COPY requirements.txt /tmp/pip/

RUN pip install -r /tmp/pip/requirements.txt \
    && rm -rf /tmp/pip
