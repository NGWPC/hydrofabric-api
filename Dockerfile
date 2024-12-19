FROM registry.sh.nextgenwaterprediction.com/ngwpc/hydrofabric/hydrofabric:development

# Update packages
RUN dnf upgrade -y

# I'd much rather compile my own or know it is in the base, but this is quick for now...
RUN dnf install -y \
    python311 python3.11-pip python3.11-devel \
    git sudo shadow-utils \
    && dnf clean all \
    && git config --global --add safe.directory /workspace

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
    && dnf remove pip -y \    
    && update-alternatives --install /usr/bin/pip pip /usr/bin/pip3.11 1

# Install Python Dependencies and test tools
COPY requirements.txt /tmp/pip/
RUN pip install --no-cache-dir -r /tmp/pip/requirements.txt \
    && pip install pytest flake8 \
    && rm -rf /tmp/pip

# Just to make it a little easier for people to get running in dev / test
ENV DB_PORT=5432
ENV DB_NAME=hydrofabric_db
ENV DB_ENGINE=django.db.backends.postgresql_psycopg2
ENV LD_LIBRARY_PATH=/usr/local/lib64

EXPOSE 8000
WORKDIR /workspace/djangoApps/

# Start the application
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]
