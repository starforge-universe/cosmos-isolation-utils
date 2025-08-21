# CosmosDB Isolation Utils

A Python library providing utilities for Azure CosmosDB isolation and testing.

## Features

- Database isolation utilities for CosmosDB containers and databases
- Testing helpers for CosmosDB applications
- Container and database management tools
- Query isolation and performance testing utilities

## Installation

### From PyPI

```bash
pip install cosmos-isolation-utils
```

### From Source

```bash
git clone https://github.com/starforge-universe/cosmos-isolation-utils.git
cd cosmos-isolation-utils
pip install -e .
```

## Quick Start

### Basic Usage

```python
from cosmos_isolation_utils.hello_world import HelloWorld

# Create a HelloWorld instance
hello = HelloWorld()

# Get a greeting
message = hello.greet()
print(message)  # Output: Hello
```

### CosmosDB Isolation Example

```python
from cosmos_isolation_utils.hello_world import HelloWorld
from examples.cosmosdb_isolation_example import CosmosDBIsolationExample

# Create an isolation example instance
isolation_example = CosmosDBIsolationExample()

# Create isolated containers for testing
container1 = isolation_example.create_isolated_container("users")
container2 = isolation_example.create_isolated_container("orders")

# Run isolation tests
test_results = isolation_example.run_isolation_test("basic_isolation")

# Cleanup
isolation_example.cleanup_isolated_containers()
```

## Development Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/starforge-universe/cosmos-isolation-utils.git
   cd cosmos-isolation-utils
   ```

2. Create a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

4. Verify installation:
   ```bash
   python -c "import cosmos_isolation_utils; print('Installation successful!')"
   ```

## Development Tools

This project uses several development tools to maintain code quality:

- **Pylint**: Linting and code analysis
- **unittest**: Testing framework (built into Python)

### Running Quality Checks

```bash
# Lint code
pylint cosmos_isolation_utils/

# Run tests
python -m unittest discover tests

# Run tests with coverage
make test-cov

# Run specific test file
python -m unittest tests.test_hello_world

## Testing

Run the test suite:

```bash
python -m unittest discover tests
```

Run specific test file:

```bash
python -m unittest tests.test_hello_world
```

Run tests with verbose output:

```bash
python -m unittest discover tests -v
```

Run tests with coverage:

```bash
# Using Makefile
make test-cov

# Or manually
coverage run -m unittest discover tests
coverage report
coverage html
```

The coverage report will be generated in the `htmlcov/` directory. Open `htmlcov/index.html` in your browser to view the detailed coverage report.

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass (`python -m unittest discover tests`)
6. Run code quality checks (`pylint cosmos_isolation_utils/`)
7. Run tests (`python -m unittest discover tests`)
8. Commit your changes (`git commit -m 'Add amazing feature'`)
9. Push to the branch (`git push origin feature/amazing-feature`)
10. Open a Pull Request

### Code Style

- Follow PEP 8 guidelines
- Write docstrings for all public functions and classes
- Keep functions small and focused
- Write comprehensive tests

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- **Documentation**: [https://cosmos-isolation-utils.readthedocs.io/](https://cosmos-isolation-utils.readthedocs.io/)
- **Issues**: [https://github.com/starforge-universe/cosmos-isolation-utils/issues](https://github.com/starforge-universe/cosmos-isolation-utils/issues)
- **Discussions**: [https://github.com/starforge-universe/cosmos-isolation-utils/discussions](https://github.com/starforge-universe/cosmos-isolation-utils/discussions)

## Related Projects

- [Azure CosmosDB Python SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/cosmos/azure-cosmos)
- [Azure Identity Python SDK](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/identity/azure-identity)

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for a list of changes between versions.
