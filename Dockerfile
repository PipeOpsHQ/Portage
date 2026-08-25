# Copyright 2026 PipeOps and the Portage Authors.
# SPDX-License-Identifier: Apache-2.0

FROM golang:1.24-bookworm AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/controller ./cmd/controller \
 && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags "-s -w" -o /out/portage ./cmd/portage

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /out/controller /controller
COPY --from=builder /out/portage /portage
USER 65532:65532
ENTRYPOINT ["/controller"]
