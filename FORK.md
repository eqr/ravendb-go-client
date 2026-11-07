# Fork provenance and maintenance

This repository is based on `github.com/ravendb/ravendb-go-client` commit `2b87f37fe4272af1cd41bd0dfe586f747f24b92e` (2024-07-23). The fork provides a directly importable module at `github.com/eqr/ravendb-go-client`, correct TLS peer verification, lossless JSON handling for exact integer values across entity, query, and compare-exchange paths, and additive per-operation caller-context cancellation.

## Intentional differences

1. HTTP, WebSocket, and TCP TLS connections use Go's normal certificate-chain and hostname verification. A configured `TrustStore` is used as the trust root; when absent, Go uses system roots. The client certificate is optional and is attached only when configured. Whether a client is authorized remains a server-side policy.
2. The Go module path and all internal imports use `github.com/eqr/ravendb-go-client`. This is required for downstream modules to import the fork without inheriting a `replace` directive.
3. `tls_verification_test.go` contains synthetic local HTTP/TCP regression tests for valid and invalid server certificates, client-certificate authentication, optional client certificates, and independent client trust/certificate configurations.
4. Integer JSON values in the `int64`/`uint64` range that exceed float64's exact integer precision remain exact through entity, query, and compare-exchange conversion. Fractional JSON numbers retain float64 behavior; `json_numeric_test.go` covers boundaries, nested values, query, and compare-exchange paths.

5. Additive `Context` methods propagate caller cancellation and deadlines through ordinary requests, session load/save/read operations, query/stream execution, compare-exchange and other operation executors, maintenance/index deployment, and operation polling. Existing methods retain background-context behavior. Automatic response bodies close on every return; streaming bodies transfer only on successful return, and abandoned fastest-node responses are closed. A canceled request is not blindly retried; cancellation after dispatch does not establish whether the server committed a write.

No topology algorithms, protocol behavior, or broader SDK modernization changes are included in this update. The upstream client remains beta; these fork tests do not establish full RavenDB server/client qualification.

## Updating from upstream

1. Fetch upstream and select a reviewed upstream commit. Keep the upstream base revision explicit in the update commit or release notes.
2. Rebase or merge only after reviewing upstream changes to `tcp_utils.go`, HTTP client construction in `request_executor.go`, WebSocket setup in `database_changes.go`, TCP negotiation, module imports, and test/CI commands.
3. Preserve normal TLS verification and the behavioral tests. Do not restore `InsecureSkipVerify`, add a process-global transport override, or weaken a failing certificate test.
4. Resolve module-path conflicts in favor of `github.com/eqr/ravendb-go-client` for all Go source, tests, examples, and coverage commands. Keep upstream links only for upstream issue history and provenance.
5. Run `gofmt` on changed Go files and the local TLS tests (`go test -run 'TestHTTP|TestTCP' .`). Run the full client suite only with the explicit RavenDB test environment it requires. Record test outcomes and the new upstream base in the update PR.

The TLS-only patch can be proposed upstream independently of the fork module-path migration. Do not include `go.mod`, module self-import changes, fork documentation, or fork-specific CI path changes in that upstream patch.
