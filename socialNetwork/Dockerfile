FROM yg397/thrift-microservice-deps:xenial

ARG NUM_CPUS=40

COPY ./ /social-network-microservices
RUN apt-get install -y net-tools
RUN cd /social-network-microservices \
    && mkdir -p build \
    && cd build \
    && cmake .. \
    && make -j${NUM_CPUS} \
    && make install \
    && cd ../tcp_latency \
    && mkdir -p build \
    && cd build \
    && cmake .. \
    && make -j${NUM_CPUS} \
    && make install


WORKDIR /social-network-microservices