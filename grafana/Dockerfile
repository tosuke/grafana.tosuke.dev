# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:alpine AS webdav-proxy
WORKDIR /src
ARG TARGETOS TARGETARCH
RUN --mount=type=bind,source=webdav-proxy,target=. \
	GOTOOLCHAIN=auto CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
	go build -trimpath -ldflags="-s -w" -o /webdav-proxy .

FROM litestream/litestream:0.5.16-scratch AS litestream

FROM grafana/grafana:13.2.0 AS grafana-plugin-installer
RUN grafana cli plugins install grafana-llm-app
RUN grafana cli plugins install grafana-metricsdrilldown-app 2.5.0
RUN grafana cli plugins install grafana-lokiexplore-app 2.5.1
RUN grafana cli plugins install grafana-exploretraces-app

FROM grafana/grafana:13.2.0-distroless
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
COPY --from=webdav-proxy /webdav-proxy /usr/local/bin/webdav-proxy
COPY --from=grafana-plugin-installer --chown=472:0 /var/lib/grafana/plugins /var/lib/grafana/plugins
COPY grafana.ini /etc/grafana/grafana.ini
COPY litestream.yaml /etc/litestream.yaml
COPY provisioning /etc/grafana/provisioning

EXPOSE 3000
ENTRYPOINT ["/usr/local/bin/webdav-proxy"]
CMD ["/usr/local/bin/litestream", "replicate", "-config", "/etc/litestream.yaml", "-exec", "/usr/share/grafana/bin/grafana server --homepath=/usr/share/grafana --config=/etc/grafana/grafana.ini --packaging=docker"]
