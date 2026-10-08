# Conduit plugin module (optional)

Custom Conduit binary that registers the `voi_archive` importer.

Fast Follower remains the synchronization layer. This module only translates and
pulls durable blocks into Conduit.

```bash
go mod tidy
go test ./...
go build -o ../../bin/conduit ./cmd/conduit
../../bin/conduit list
```

Configure `importer.name: voi_archive` — see `examples/conduit.yml` and
`../../docs/phase7-conduit-adapter.md`.
