FROM --platform=linux/amd64 fedora:latest

ARG dir="."

COPY ${dir}/entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

RUN dnf update -y && \
    bash -x /opt/entrypoint.sh init && \
    dnf clean all

ENTRYPOINT ["/opt/entrypoint.sh"]
CMD ["env", "bash"]
