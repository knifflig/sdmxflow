# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/devcontainers/python:2-3.14-trixie

# The base image currently includes a Yarn APT repo entry that can break `apt-get update`
# if the signing key changes/rotates. This repo is not required for this workspace and
# it prevents Dev Container features (e.g. docker-in-docker) from installing.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    /etc/apt/sources.list.d/yarn*.list \
    /etc/apt/trusted.gpg.d/yarn.gpg \
    /etc/apt/trusted.gpg.d/yarn*.gpg \
    /usr/share/keyrings/yarn.gpg \
    /usr/share/keyrings/yarnkey.gpg \
    || true
