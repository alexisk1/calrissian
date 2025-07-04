FROM rockylinux:9.3-minimal

# Install Python, Node.js, pip, and Hatch
RUN microdnf install -y --nodocs \
      python3 \
      python3-pip \
      nodejs \
  && pip3 install hatch \
  && microdnf clean all

# Set up a default user and home directory
ENV HOME=/home/calrissian

RUN useradd -u 1001 -r -g 0 -m -d ${HOME} -s /sbin/nologin \
    -c "Default Calrissian User" calrissian && \
    mkdir -p /app /prod && \
    chown -R 1001:0 /app && \
    chmod g+rwx ${HOME} /app

USER calrissian

# Copy the app into /app
COPY --chown=1001:0 . /app
WORKDIR /app

# Set up virtual environment paths
ENV VIRTUAL_ENV=/app/envs/calrissian
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Build production environment and verify CLI
RUN hatch env prune && \
    hatch env create prod && \
    hatch run prod:calrissian --help && \
    rm -rf /app/.git /app/.pytest_cache

WORKDIR /app

# Default command
CMD ["calrissian"]
