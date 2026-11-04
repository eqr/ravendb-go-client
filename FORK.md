# Fork provenance and maintenance

This repository is based on `github.com/ravendb/ravendb-go-client` commit `2b87f37fe4272af1cd41bd0dfe586f747f24b92e` (2024-07-23). The fork exists to provide a directly importable module at `github.com/eqr/ravendb-go-client` and to correct TLS peer verification without changing RavenDB client behavior otherwise.

## Intentional differences

1. HTTP, WebSocket, and TCP TLS connections use Go's normal certificate-chain and hostname verification. A configured `TrustStore` is used as the trust root; when absent, Go uses system roots. The client certificate is optional and is attached only when configured. Whether a client is authorized remains a server-side policy.
2. The Go module path and all internal imports use `github.com/eqr/ravendb-go-client`. This is required for downstream modules to import the fork without inheriting a `replace` directive.
3. `tls_verification_test.go` contains synthetic local HTTP/TCP regression tests for valid and invalid server certificates, client-certificate authentication, optional client certificates, and independent client trust/certificate configurations.

No cancellation, topology, retry, protocol, or broader SDK modernization changes are intended. The upstream client remains beta; this fork's TLS tests do not establish full RavenDB server/client qualification.

## Updating from upstream

1. Fetch upstream and select a reviewed upstream commit. Keep the upstream base revision explicit in the update commit or release notes.
2. Rebase or merge only after reviewing upstream changes to `tcp_utils.go`, HTTP client construction in `request_executor.go`, WebSocket setup in `database_changes.go`, TCP negotiation, module imports, and test/CI commands.
3. Preserve normal TLS verification and the behavioral tests. Do not restore `InsecureSkipVerify`, add a process-global transport override, or weaken a failing certificate test.
4. Resolve module-path conflicts in favor of `github.com/eqr/ravendb-go-client` for all Go source, tests, examples, and coverage commands. Keep upstream links only for upstream issue history and provenance.
5. Run `gofmt` on changed Go files and the local TLS tests (`go test -run 'TestHTTP|TestTCP' .`). Run the full client suite only with the explicit RavenDB test environment it requires. Record test outcomes and the new upstream base in the update PR.

The TLS-only patch can be proposed upstream independently of the fork module-path migration. Do not include `go.mod`, module self-import changes, fork documentation, or fork-specific CI path changes in that upstream patch.
