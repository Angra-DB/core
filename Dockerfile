FROM erlang:21.2-alpine as builder

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

ENV RELX_REPLACE_OS_VARS=true

# Install building deps
RUN apk add --no-cache --virtual .build-deps \
        dpkg-dev dpkg \
        gcc \
        g++ \
        libc-dev \
        linux-headers \
        make \
        autoconf \
        ncurses-dev \
        openssl-dev \
        unixodbc-dev \
        lksctp-tools-dev \
        tar \
        git \
    && rebar3 release \
    && apk del .build-deps

FROM alpine:latest

# Copy deployed application
COPY --from=builder /app/_build/default/rel/adb_core /angra

ENV ANGRA_NAME="a@127.0.0.1"
ENV ANGRA_COOKIE="teste"

RUN apk add ncurses openssl

# Expose ports used by EPMD process
EXPOSE 4369
EXPOSE 9100-9200

# Make port 1234 evailable to the world outside this container
EXPOSE 1234-1236

CMD ["/angra/bin/adb_core", "foreground"]

