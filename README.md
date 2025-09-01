# CosmosDB Isolation Utilities

A unified command-line interface for managing Azure CosmosDB databases in isolation environments. This tool consolidates multiple utilities into a single CLI with consistent parameter handling and a clean separation between the interface and core implementation.

## Features

- **Unified CLI**: Single command-line tool with subcommands for all operations
- **Connection Testing**: Test and validate CosmosDB connections
- **Container Management**: View status, statistics, and manage containers
- **Data Export/Import**: Dump containers to JSON and restore from JSON files
- **Database Operations**: List and manage databases
- **Rich Output**: Beautiful terminal output with progress bars and tables
- **Safety Features**: Confirmation prompts and dry-run modes for destructive operations

## Installation

### From Source

```bash
git clone <repository-url>
cd cosmos-isolation-utils
pip install -e .
```

### Dependencies

The tool requires the following Python packages:
- `azure-cosmos>=4.0.0` - Azure CosmosDB client
- `click>=8.0.0` - CLI framework
- `rich>=13.0.0` - Rich terminal output
- `urllib3>=1.26.0` - HTTP client

## Usage

The unified CLI tool provides several subcommands, all sharing common connection parameters:

```bash
cosmos-isolation-utils <subcommand> -e <endpoint> -k <key> -d <database> [options]
```

### Common Parameters

- `-e, --endpoint`: CosmosDB endpoint URL (required)
- `-k, --key`: CosmosDB primary key (required)
- `-d, --database`: CosmosDB database name (required)
- `-a, --allow-insecure`: Allow insecure HTTPS requests (suppress warnings)

### Subcommands

#### 1. Test Connection

Test the connection to a CosmosDB database and list available containers:

```bash
cosmos-isolation-utils test -e <endpoint> -k <key> -d <database> [options]
```

**Options:**
- `--create-database`: Create database if it doesn't exist
- `-f, --force`: Skip confirmation prompts

**Example:**
```bash
cosmos-isolation-utils test -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      --create-database
```

#### 2. Container Status

View the status and statistics of all containers in a database:

```bash
cosmos-isolation-utils status -e <endpoint> -k <key> -d <database> [options]
```

**Options:**
- `--detailed`: Show detailed information for each container

**Example:**
```bash
cosmos-isolation-utils status -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      --detailed
```

#### 3. Dump Containers

Export all entries from containers to a JSON file:

```bash
cosmos-isolation-utils dump -e <endpoint> -k <key> -d <database> [options]
```

**Options:**
- `-c, --containers`: Comma-separated list of container names or "all"
- `-o, --output`: Output JSON file path (required)
- `-b, --batch-size`: Batch size for processing (default: 100)
- `-p, --pretty`: Pretty print JSON output

**Examples:**
```bash
# Dump all containers
cosmos-isolation-utils dump -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -c all -o all_containers.json

# Dump specific containers
cosmos-isolation-utils dump -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -c "users,orders" -o selected_containers.json

# Dump with pretty formatting
cosmos-isolation-utils dump -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -c all -o all_containers.json -p
```

#### 4. Upload Entries

Restore containers from a JSON dump file:

```bash
cosmos-isolation-utils upload -e <endpoint> -k <key> -d <database> [options]
```

**Options:**
- `-i, --input`: Input JSON file path (required)
- `-b, --batch-size`: Batch size for processing (default: 100)
- `-u, --upsert`: Use upsert instead of create (overwrites existing items)
- `-r, --dry-run`: Show what would be uploaded without actually uploading
- `-f, --force`: Skip confirmation prompts
- `--create-containers`: Automatically create containers if they don't exist
- `-c, --containers`: Comma-separated list of specific containers to upload

**Examples:**
```bash
# Upload all containers from dump
cosmos-isolation-utils upload -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -i all_containers.json --create-containers

# Upload specific containers with dry-run
cosmos-isolation-utils upload -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -i all_containers.json -c "users,orders" --dry-run
```

#### 5. Database Management

List and manage databases:

```bash
cosmos-isolation-utils delete-db -e <endpoint> -k <key> -d <database> [options]
```

**Options:**
- `-l, --list-databases`: List all existing databases
- `-f, --force`: Skip confirmation prompts for deletion

**Examples:**
```bash
# List all databases
cosmos-isolation-utils delete-db -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -l

# Delete a database (with confirmation)
cosmos-isolation-utils delete-db -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb"

# Force delete a database (skip confirmation)
cosmos-isolation-utils delete-db -e "https://your-cosmosdb.documents.azure.com:443/" \
                      -k "your-primary-key" \
                      -d "testdb" \
                      -f
```

## Project Structure

```
cosmos-isolation-utils/
├── cosmos_isolation_utils/
│   ├── __init__.py
│   ├── __main__.py          # Entry point for python -m
│   ├── __main__.py          # CLI interface and subcommands
│   ├── cosmos_client.py     # CosmosDB client wrapper
│   └── core/                # Core implementation logic
│       ├── __init__.py
│       ├── connection.py    # Connection testing
│       ├── status.py        # Container status
│       ├── dump.py          # Container export
│       ├── upload.py        # Container import
│       └── delete.py        # Database deletion
├── tests/                   # Test suite
├── pyproject.toml          # Project configuration
└── README.md               # This file
```

## Architecture

The tool follows a clean separation of concerns:

- **CLI Layer** (`__main__.py`): Handles command-line interface, parameter parsing, and user interaction
- **Core Layer** (`core/`): Contains the actual business logic for each operation
- **Client Layer** (`cosmos_client.py`): Provides a high-level interface to CosmosDB operations

This separation makes the code more maintainable and testable, while providing a consistent user experience across all operations.

## Development

### Setup Development Environment

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Using unittest discover (recommended)
python -m unittest discover tests/ -v

# Using make
make test

# Run specific test file
python -m unittest tests.test_cli -v

# Run test file directly
python tests/test_cli.py -v

# Run tests with coverage
make test-cov
```

### Code Quality

The project uses:
- `pylint` for code quality checks
- `coverage` for test coverage
- `black` for code formatting

## Release Workflow

This project uses GitHub Actions with environment protection for secure package publishing to PyPI.

### Release Process

1. **Create Release Branch**: Push to a branch named `release/X.Y.Z` (e.g., `release/1.2.3`)
2. **Automated Checks**: The workflow runs tests, builds the package, and publishes to Test PyPI
3. **Test Publication**: Package is published to Test PyPI for validation
4. **Production Publication**: After successful test publication, package is published to Production PyPI
5. **Release Tag**: A Git tag is created for the release

### Environment Setup

The workflow requires two GitHub environments to be configured:

- **`test-pypi`**: For publishing to Test PyPI
- **`production-pypi`**: For publishing to Production PyPI

See [Environment Setup Guide](docs/environment-setup.md) for detailed configuration instructions.

### Security Features

- **Environment Protection**: Each environment requires approval from designated reviewers
- **Sequential Publishing**: Test publication must succeed before production publication
- **Secret Isolation**: PyPI credentials are scoped to specific environments
- **Approval Process**: Production releases require explicit approval with optional wait timers

## Command Reference

### Quick Command Overview

```bash
# Test connection and list containers
cosmos-isolation-utils test -e <endpoint> -k <key> -d <database> [--create-database] [-f]

# Show container status and statistics
cosmos-isolation-utils status -e <endpoint> -k <key> -d <database> [--detailed]

# Dump containers to JSON file
cosmos-isolation-utils dump -e <endpoint> -k <key> -d <database> -c <containers> -o <output> [-b <batch-size>] [-p]

# Upload containers from JSON file
cosmos-isolation-utils upload -e <endpoint> -k <key> -d <database> -i <input> [-c <containers>] [-b <batch-size>] [-u] [-r] [-f] [--create-containers]

# Manage databases
cosmos-isolation-utils delete-db -e <endpoint> -k <key> -d <database> [-l] [-f]
```

### Environment Variables

You can also set connection parameters via environment variables:

```bash
export COSMOS_ENDPOINT="https://your-cosmosdb.documents.azure.com:443/"
export COSMOS_KEY="your-primary-key"
export COSMOS_DATABASE="your-database"

# Then run commands without -e, -k, -d flags
cosmos-isolation-utils test
cosmos-isolation-utils status --detailed

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions, please use the GitHub issue tracker or contact the maintainers. 
