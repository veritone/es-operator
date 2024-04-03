FROM alpine:latest

ADD build/linux/es-operator /

ENTRYPOINT ["/es-operator"]
