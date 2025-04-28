FROM --platform=linux/amd64 fedora:latest

ARG dir="."
COPY ${dir}/init.sh /tmp
RUN dnf update -y
RUN bash -x /tmp/init.sh

CMD ["env", "bash"]
