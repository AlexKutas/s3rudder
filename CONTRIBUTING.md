# Contributing to S3Rudder

Thank you for your interest in contributing to S3Rudder! We welcome contributions from developers, testers, and the community.

## AI Co-Pilot Contribution

S3Rudder is co-developed with **Antigravity**, a powerful agentic AI coding assistant designed by Google DeepMind. Both humans and AI agents collaborate on this repository to design, implement, and optimize features. 

If you are using AI assistants to contribute, feel free to do so! We encourage pairing with AI helpers to write high-quality Go code and verify configurations.

## How to Contribute

### 1. Reporting Bugs
* Check the existing issues to see if the bug has already been reported.
* If not, open a new issue using the **Bug Report** template.
* Provide a clear description, steps to reproduce, and any relevant logs or configuration snippets (make sure to redact sensitive keys!).

### 2. Suggesting Features
* Open an issue using the **Feature Request** template.
* Explain the use case, why this feature would be useful for S3Rudder, and how it should behave.

### 3. Submitting Pull Requests
* Fork the repository and create your branch from `main`.
* Write clean Go code, and ensure it is formatted using `gofmt`.
* Add unit tests where applicable to cover your changes.
* Ensure all local tests and Docker environments build and run successfully:
  ```bash
  docker compose up -d --build
  ```
* Open a PR with a description of the changes, referencing the issue it resolves.

## Development Setup

* **Language**: Go 1.25+
* **Dependencies**: Managed via Go modules (`go.mod`)
* **Local environment**: Managed via Docker Compose (MinIO instances for testing S3 failover and replication)

We appreciate your support in making S3Rudder better!
