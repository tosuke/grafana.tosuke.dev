# syntax=docker/dockerfile:1

FROM litestream/litestream:0.5.14-scratch AS litestream

FROM grafana/grafana:13.1.2-distroless
COPY --from=litestream /usr/local/bin/litestream /usr/local/bin/litestream
COPY <<-EOF /etc/grafana/grafana.ini
	[server]
	enable_gzip = false

	[database]
	type = sqlite3
	path = /var/lib/grafana/grafana.db
	wal = true
	cache_mode = shared
EOF
COPY <<-EOF /etc/litestream.yml
	dbs:
		- path: /var/lib/grafana/grafana.db
		  replicas:
				- url: webdav://r2dav.worker/
					sync-interval: 10s
EOF

EXPOSE 3000
ENTRYPOINT [ \
	"/usr/local/bin/litestream", \
	"replicate", \
	"-restore-if-db-not-exists", \
	"-exec", \
	"/usr/share/grafana/bin/grafana server --homepath=/usr/share/grafana --config=/etc/grafana/grafana.ini --packaging=docker", \
	"/var/lib/grafana/grafana.db", \
	"webdav://r2dav.worker/" \
]
