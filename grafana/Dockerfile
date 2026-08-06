# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang AS supervisor
ENV GOTOOLCHAIN=auto
ENV CGO_ENABLED=0
WORKDIR /src
RUN --mount=type=bind,source=go.mod,target=go.mod --mount=type=bind,source=go.sum,target=go.sum \
	go mod download
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build,sharing=locked --mount=type=bind,source=.,target=. \
	GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /litestream-supervisor .

FROM grafana/grafana:13.1.2 AS grafana-plugin-installer
RUN grafana cli plugins install grafana-llm-app

FROM grafana/grafana:13.1.2-distroless
COPY --from=supervisor /litestream-supervisor /usr/local/bin/litestream-supervisor
COPY --from=grafana-plugin-installer --chown=472:0 /var/lib/grafana/plugins /var/lib/grafana/plugins
COPY grafana.ini /etc/grafana/grafana.ini
COPY provisioning /etc/grafana/provisioning

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/litestream-supervisor"]
CMD ["--database", "/var/lib/grafana/grafana.db", "--replica-url", "ltxws://replica.worker/rpc", "--sync-interval", "10s", "--close-timeout", "60s", "--monitor-port", "3000", "--", "/usr/share/grafana/bin/grafana", "server", "--homepath=/usr/share/grafana", "--config=/etc/grafana/grafana.ini", "--packaging=docker"]
