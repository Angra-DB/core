FROM erlang:21.2-alpine as builder

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install building deps
RUN set -xe \
    && apk add --no-cache --virtual .build-deps \
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
    && rebar3 release tar \
    && apk del .build-deps

FROM alpine:latest

# Copy deployed application
WORKDIR /app
COPY --from=builder /app/_build/default/rel .

# Expose ports used by EPMD process
EXPOSE 4369
EXPOSE 9100-9200

# Make port 80 evailable to the world outside this container
EXPOSE 1234

RUN set -xe \
    && ANGRA_VERSION=0.1.0 \
    && tar -zxvf adb_core-$ANGRA_VERSION.tar.gz \
    && rm adb_core-$ANGRA_VERSION.tar.gz

CMD ["./adb_core/bin/adb_core console"]

