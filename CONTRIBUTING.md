# Contributing

Thanks for your interest in contributing to `consistency-checker`.

This project ensures blockchain data consistency across distributed replica nodes. It consumes block change events, validates synchronization, detects forks, and coordinates state via external systems.

Contributions should prioritize correctness, determinism, and reliability under distributed conditions.

---

## Getting Started

### Requirements

* Go 1.22+
* git
* golangci-lint

Install dependencies:

```bash
go mod download
```

Run local checks:

```bash
make ci
```

---

## Development Workflow

1. Fork the repository
2. Create a branch from `main`
3. Make changes
4. Run local checks
5. Open a pull request

Keep PRs small and focused.

---

## Local Checks (must pass)

```bash
gofmt -l .
go vet ./...
golangci-lint run --timeout=5m
go test ./...
```

---

## Code Guidelines

### General

* Prefer simple and explicit logic
* Avoid unnecessary abstractions
* Keep dependencies minimal
* Make behavior observable (logs, metrics)

---

### Distributed Consistency (Critical)

Changes affecting core logic must ensure:

* Correct block ordering
* Idempotent processing (Kafka re-consumption safe)
* Deterministic fork detection
* No data loss under retries / restarts
* Safe coordination via etcd (avoid race conditions)

---

### External Dependencies

The system interacts with:

* Kafka (event ingestion)
* etcd (state coordination)
* S3 (data / snapshot storage)

Requirements:

* Handle transient failures (retry, backoff)
* Avoid tight coupling to external availability
* Fail gracefully with clear error signals

---

### Concurrency

* Use `context.Context` consistently
* Avoid goroutine leaks
* Ensure proper shutdown handling
* Be explicit about locking and shared state

---

### Errors

* Return errors explicitly
* Wrap errors with context (`fmt.Errorf("...: %w", err)`)
* Avoid panics in service code

---

## Testing

All changes must include tests.

### Run tests

```bash
go test ./...
```

---

### Test Types

* **Unit tests**: default, no external dependencies
* **Integration tests**: require Kafka / etcd / S3

Mark integration tests with build tags:

```go
//go:build integration
```

Run integration tests manually:

```bash
go test -tags=integration ./...
```

---

### What to Cover

* Block ordering and gap detection
* Fork detection logic
* Retry and idempotency behavior
* Failure scenarios (partial data, node lag)
* Concurrency and race conditions

---

## Formatting & Lint

```bash
gofmt -w .
golangci-lint run --timeout=5m
```

---

## Pull Requests

Before submitting:

* CI must pass
* Tests added or updated
* Behavior changes clearly explained

PRs should include:

* Summary
* Motivation
* Testing details
* Impact on consistency / correctness

---

## Compatibility Policy

* Avoid breaking external interfaces
* Changes to data formats or coordination logic must be documented
* Backward compatibility should be preserved where possible

---

## Commit Guidelines

Use clear, descriptive messages.

Example:

```text
checker: fix fork detection under delayed block propagation
```

---

## Reporting Issues

Please include:

* Go version
* Environment details
* Reproduction steps
* Expected vs actual behavior
* Relevant logs

---

## Security

Do not disclose vulnerabilities publicly.

See `SECURITY.md` for reporting instructions.

---

## License

By contributing, you agree that your contributions are licensed under the project's license.
