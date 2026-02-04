# Contributing to CosmosDB Isolation Utils

Thank you for your interest in contributing to CosmosDB Isolation Utils! This document provides guidelines and information for contributors.

## Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Create a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```
4. **Install development dependencies**:
   ```bash
   pip install -e ".[dev]"
   ```
5. **Verify installation**:
   ```bash
   python -c "import cosmos_isolation_utils; print('Installation successful!')"
   ```

## Development Workflow

### 1. Create a Feature Branch

Always create a new branch for your changes:

```bash
git checkout -b feature/your-feature-name
```

### 2. Make Your Changes

- Write clear, well-documented code
- Add type hints to all functions
- Write docstrings for all public functions and classes
- Follow PEP 8 style guidelines
- Keep functions small and focused

### 3. Write Tests

- Add tests for all new functionality
- Ensure existing tests still pass
- Aim for high test coverage
- Use descriptive test names

### 4. Run Quality Checks

Before committing, run these checks:

```bash
# Run linting
make lint

# Run tests
make test

# Run tests with coverage
make test-cov
```

### 5. Commit Your Changes

Use conventional commit messages:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Test changes
- `chore`: Build process or auxiliary tool changes

### 6. Push and Create a Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a pull request on GitHub.

## Code Style Guidelines

### Python Code

- Follow PEP 8 style guidelines
- Use Pylint for linting
- Maximum line length: 88 characters

### Documentation

- Use Google-style docstrings
- Include type hints in docstrings
- Provide examples for complex functions
- Keep documentation up to date

### Testing

- Use unittest for testing (built into Python)
- Write unit tests for all functions
- Use descriptive test names
- Mock external dependencies using unittest.mock
- Aim for comprehensive test coverage

## Pull Request Guidelines

### Before Submitting

1. **Ensure all tests pass**
2. **Run quality checks**:
   ```bash
   make lint
   make test
   make test-cov
   ```
3. **Update documentation** if needed
4. **Add changelog entry** if applicable

### Pull Request Template

Use this template when creating a pull request:

```markdown
## Description

Brief description of the changes.

## Type of Change

- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Testing

- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] All existing tests pass

## Checklist

- [ ] My code follows the style guidelines of this project
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes
```

## Reporting Issues

When reporting issues, please include:

1. **Operating system** and Python version
2. **Steps to reproduce** the issue
3. **Expected behavior** vs actual behavior
4. **Error messages** or stack traces
5. **Minimal example** that reproduces the issue

## Getting Help

- **Documentation**: Check the [documentation](https://cosmos-isolation-utils.readthedocs.io/)
- **Issues**: Search existing [issues](https://github.com/starforge-universe/cosmos-isolation-utils/issues)
- **Discussions**: Use [GitHub Discussions](https://github.com/starforge-universe/cosmos-isolation-utils/discussions)
- **Azure CosmosDB Documentation**: [https://docs.microsoft.com/en-us/azure/cosmos-db/](https://docs.microsoft.com/en-us/azure/cosmos-db/)

## License

By contributing to this project, you agree that your contributions will be licensed under the Apache License 2.0.
