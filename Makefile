.PHONY: test lint

test:
	go test -vet=off . ./cmd/... ./dive-into-raven ./examples/... ./serverwide/...

lint:
	@test -z "$$(gofmt -l counters_session_operations.go direct_counters_timeseries.go time_series_session_operations.go counters_ts_integration_test.go command_type.go compare_exchange_session_value.go)" || (echo "gofmt issues:"; gofmt -l counters_session_operations.go direct_counters_timeseries.go time_series_session_operations.go counters_ts_integration_test.go command_type.go compare_exchange_session_value.go; exit 1)