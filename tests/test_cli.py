"""
Tests for the unified CLI interface.
"""

import pytest
from click.testing import CliRunner
from cosmos_isolation_utils.__main__ import main


def test_cli_help():
    """Test that the CLI help command works."""
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert 'CosmosDB Isolation Utilities' in result.output


def test_cli_version():
    """Test that the CLI version command works."""
    runner = CliRunner()
    result = runner.invoke(main, ['--version'])
    assert result.exit_code == 0
    assert '0.1.0' in result.output


def test_cli_missing_required_params():
    """Test that the CLI requires all required parameters."""
    runner = CliRunner()
    result = runner.invoke(main, ['test'])
    assert result.exit_code != 0
    assert 'Missing option' in result.output


def test_cli_subcommands():
    """Test that all subcommands are available."""
    runner = CliRunner()
    
    # Test each subcommand help
    subcommands = ['test', 'status', 'dump', 'upload', 'delete-db']
    
    for subcommand in subcommands:
        result = runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert subcommand in result.output


def test_cli_common_params():
    """Test that common parameters are available for all subcommands."""
    runner = CliRunner()
    
    # Test that endpoint, key, and database are required
    result = runner.invoke(main, ['--help'])
    assert result.exit_code == 0
    assert '--endpoint' in result.output
    assert '--key' in result.output
    assert '--database' in result.output
    
    # Test that subcommand help shows the common parameters
    result = runner.invoke(main, ['-e', 'dummy', '-k', 'dummy', '-d', 'dummy', 'test', '--help'])
    assert result.exit_code == 0
