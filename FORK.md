# Fork provenance and maintenance

This repository is based on `github.com/ravendb/ravendb-go-client` commit `2b87f37fe4272af1cd41bd0dfe586f747f24b92e` (2024-07-23). The fork provides a directly importable module at `github.com/eqr/ravendb-go-client`, correct TLS peer verification, lossless JSON handling for exact integer values across entity, query, and compare-exchange paths, and additive per-operation caller-context cancellation.

## Intentional differences

1. HTTP, WebSocket, and TCP TLS connections use Go's normal certificate-chain and hostname verification. A configured `TrustStore` is used as the trust root; when absent, Go uses system roots. The client certificate is optional and is attached only when configured. Whether a client is authorized remains a server-side policy.
2. The Go module path and all internal imports use `github.com/eqr/ravendb-go-client`. This is required for downstream modules to import the fork without inheriting a `replace` directive.
3. `tls_verification_test.go` contains synthetic local HTTP/TCP regression tests for valid and invalid server certificates, client-certificate authentication, optional client certificates, and independent client trust/certificate configurations.
4. Integer JSON values in the `int64`/`uint64` range that exceed float64's exact integer precision remain exact through entity, query, and compare-exchange conversion. Fractional JSON numbers retain float64 behavior; `json_numeric_test.go` covers boundaries, nested values, query, and compare-exchange paths.

5. Additive `Context` methods propagate caller cancellation and deadlines through ordinary requests, session load/save/read operations, query/stream execution, compare-exchange and other operation executors, maintenance/index deployment, and operation polling. Existing methods retain background-context behavior. Automatic response bodies close on every return; streaming bodies transfer only on successful return, and abandoned fastest-node responses are closed. A canceled request is not blindly retried; cancellation after dispatch does not establish whether the server committed a write.
6. Cluster-wide automatic-guard requests advertise `Raven-Client-Version: 5.2.0` only when automatic guards are enabled or left at their default; explicitly disabled and single-node batches retain the base `4.0.0`. Cluster-wide document loads send `txMode=ClusterWide`; new sessions snapshot the per-database maximum known Raft transaction index and successful cluster transactions advance it. Entity puts and tracked deletes preserve `OriginalChangeVector` separately from the optional optimistic-concurrency `ChangeVector`; this prevents a stale cluster-wide delete from removing a newer document while keeping the public `NewDeleteCommandData` signature and ordinary session behavior. Synthetic request/session tests cover the wire fields and index tracking; the PocketQueue RF1 smoke covers guard creation, update, stale-update and stale-delete conflicts, cluster-wide delete/recreate, and actual orphan-guard include decoding/recreation. This is not multi-node acceptance.

The delete request distinction matches the official RavenDB 7.2 [`DeleteCommandData`](https://github.com/ravendb/ravendb/blob/b84416401c6cd51a7d9a17f6d2fc7604b739fdf7/src/Raven.Client/Documents/Commands/Batches/DeleteCommandData.cs) wire contract: `ChangeVector` and `OriginalChangeVector` are separate fields. Official [`atomic guard` documentation](https://docs.ravendb.net/7.2/compare-exchange/atomic-guards) says cluster-wide deletion removes the guard, while ordinary deletion can leave an orphan guard that a cluster-wide missing-document load includes for safe recreation.

The protocol changes are limited to documented cluster-transaction fields needed for automatic atomic guards and stale-delete safety. No topology or failover algorithms are changed; these fork tests do not establish full RavenDB server/client qualification.

## Updating from upstream

1. Fetch upstream and select a reviewed upstream commit. Keep the upstream base revision explicit in the update commit or release notes.
2. Rebase or merge only after reviewing upstream changes to `tcp_utils.go`, HTTP client construction in `request_executor.go`, WebSocket setup in `database_changes.go`, TCP negotiation, module imports, and test/CI commands.
3. Preserve normal TLS verification and the behavioral tests. Do not restore `InsecureSkipVerify`, add a process-global transport override, or weaken a failing certificate test.
4. Resolve module-path conflicts in favor of `github.com/eqr/ravendb-go-client` for all Go source, tests, examples, and coverage commands. Keep upstream links only for upstream issue history and provenance.
5. Run `gofmt` on changed Go files and the local TLS tests (`go test -run 'TestHTTP|TestTCP' .`). Run the full client suite only with the explicit RavenDB test environment it requires. Record test outcomes and the new upstream base in the update PR.

The TLS-only patch can be proposed upstream independently of the fork module-path migration. Do not include `go.mod`, module self-import changes, fork documentation, or fork-specific CI path changes in that upstream patch.
