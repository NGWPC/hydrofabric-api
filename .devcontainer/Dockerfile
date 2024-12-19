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

# Create a non-root user
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && echo $USERNAME ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

# Install Python Dependencies and test tools
COPY requirements.txt /tmp/pip/
RUN pip install --no-cache-dir -r /tmp/pip/requirements.txt \
    && pip install pytest flake8 \
    && rm -rf /tmp/pip

# Set library path
ENV LD_LIBRARY_PATH=/usr/local/lib64

USER $USERNAME

# Default command to run when the container starts (can be overridden)
CMD ["sleep", "infinity"]
