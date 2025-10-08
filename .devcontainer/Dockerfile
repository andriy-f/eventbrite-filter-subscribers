FROM mcr.microsoft.com/devcontainers/javascript-node:1-22-bookworm

USER root

RUN npm uninstall -g yarn pnpm \
	&& npm install -g corepack@latest

USER node

ENV MY_BIN_DIR=/home/node/bin
RUN mkdir -p $MY_BIN_DIR
ENV PATH=$PATH:$MY_BIN_DIR

RUN corepack enable --install-directory $MY_BIN_DIR pnpm \
	&& corepack install -g pnpm@latest-10