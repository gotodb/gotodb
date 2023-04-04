# docker run --rm -it -p 50051:50051 -p 50052:50052  --name=gotodb-worker  zhenorzz/gotodb-worker -rpc-port=50052 -tcp-port=50051 --etcd-endpoint=http://192.168.5.18:2379
FROM alpine

LABEL maintainer="zhenorzz"

ADD coordinator /opt/gotodb/

RUN chmod a+x /opt/gotodb/coordinator

WORKDIR /opt/gotodb/

EXPOSE 80

EXPOSE 8080

ENTRYPOINT ["/opt/gotodb/coordinator"]
