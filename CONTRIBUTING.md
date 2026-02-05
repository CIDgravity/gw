# Contributing to Filecoin Gateway

Thank you for your interest in contributing to Filecoin Gateway! This project provides an S3-compatible gateway for the Filecoin network, and we welcome contributions from the community.

## Table of Contents

- [How to Report Bugs](#how-to-report-bugs)
- [How to Suggest Features](#how-to-suggest-features)
- [Development Setup](#development-setup)
- [Code Style Guidelines](#code-style-guidelines)
- [Pull Request Process](#pull-request-process)
- [Community Channels](#community-channels)

## How to Report Bugs

If you find a bug, please open an issue using the [Bug Report template](https://github.com/CIDgravity/filecoin-gateway/issues/new?template=bug_report.md). Include as much detail as possible:

- A clear description of the bug
- Steps to reproduce the issue
- Expected vs. actual behavior
- Your environment (OS, Gateway version, deployment method)
- Relevant logs or error messages

Before submitting, please search existing issues to avoid duplicates.

## How to Suggest Features

We love hearing ideas for new features! Please open an issue using the [Feature Request template](https://github.com/CIDgravity/filecoin-gateway/issues/new?template=feature_request.md). Include:

- The problem you're trying to solve
- Your proposed solution
- Any alternatives you've considered

Feature requests help us understand how the community uses Filecoin Gateway and what improvements would be most valuable.

## Development Setup

### Prerequisites

- Go 1.21 or later
- YugabyteDB instance (for testing with a database)
- Docker and Docker Compose (optional, for containerized development)
- Rclone (optional, for S3 testing)

### Building from Source

```bash
# Clone the repository
git clone git@github.com:CIDgravity/filecoin-gateway.git
cd filecoin-gateway

# Build the main binaries
go build -o filecoin-gw ./integrations/kuri/cmd/kuri
go build -o gwcfg ./integrations/gwcfg

# Run configuration wizard
./gwcfg

# Start the gateway
source settings.env
./filecoin-gw daemon
```

### Running Tests

```bash
# Run unit tests
go test ./...

# Run tests with verbose output
go test -v ./...

# Run tests for a specific package
go test ./path/to/package
```

### Using Docker for Development

```bash
# Build the Docker image
docker build . -t fgw:local

# Run with Docker Compose
docker-compose up
```

## Code Style Guidelines

- Follow standard Go conventions and idioms
- Use `gofmt` to format your code before committing
- Write clear, descriptive commit messages
- Add comments for exported functions and complex logic
- Keep functions focused and reasonably sized
- Write tests for new functionality

### Commit Messages

- Use the present tense ("Add feature" not "Added feature")
- Use the imperative mood ("Move cursor to..." not "Moves cursor to...")
- Keep the first line under 72 characters
- Reference issues and pull requests where appropriate

## Pull Request Process

1. **Fork the repository** and create a new branch from `main`
2. **Make your changes** following the code style guidelines
3. **Add or update tests** as needed
4. **Update documentation** if your changes affect user-facing features
5. **Run tests locally** to ensure everything passes
6. **Push your branch** and open a pull request
7. **Fill out the PR template** completely
8. **Respond to feedback** from maintainers during code review

### PR Guidelines

- Keep PRs focused on a single change when possible
- Link related issues in the PR description
- Ensure CI checks pass before requesting review
- Be responsive to review feedback

## Community Channels

### GitHub Issues

For bugs, feature requests, and technical questions:
- [Open an issue](https://github.com/CIDgravity/filecoin-gateway/issues)

### Filecoin Slack

Join the Filecoin community on Slack for real-time discussion:
- Channel: **#filecoin-gateway**
- [Join Filecoin Slack](https://filecoin.io/slack)

### CIDGravity Support

For service-related issues, enterprise support, or questions about CIDGravity integration:
- [CIDGravity Website](https://www.cidgravity.com)
- [Contact CIDGravity Support](https://www.cidgravity.com/contact)

---

Thank you for contributing to Filecoin Gateway! Your contributions help make decentralized storage more accessible to everyone.
