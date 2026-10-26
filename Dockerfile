FROM golang:alpine AS builder

ARG RELEASE=false
ARG COMPRESS=false
# Build with the PingMessenger source directory as context:
# docker build -f open-im-server/Dockerfile .
# The sibling protocol fork is required for token-scoped gateway kick events.
WORKDIR /src

RUN apk add --no-cache upx

RUN go install github.com/magefile/mage@latest

COPY openim-protocol ./openim-protocol
COPY open-im-server ./openim-server
WORKDIR /src/openim-server
RUN go mod download
RUN RELEASE=${RELEASE} COMPRESS=${COMPRESS} mage build
RUN mage -compile ./mage -ldflags "-s -w"

FROM alpine:latest

WORKDIR /openim-server

COPY --from=builder /src/openim-server/_output ./_output
COPY --from=builder /src/openim-server/config ./config
COPY --from=builder /src/openim-server/start-config.yml ./start-config.yml
COPY --from=builder /src/openim-server/mage ./mage

ENTRYPOINT ["sh", "-c", "./mage start && sleep infinity"]
