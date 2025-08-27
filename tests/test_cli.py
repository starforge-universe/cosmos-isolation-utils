"""
Tests for the unified CLI interface.
"""

import unittest
from click.testing import CliRunner
from cosmos_isolation_utils.__main__ import main


class TestCLI(unittest.TestCase):
    """Test cases for the CLI interface."""

    def setUp(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    def test_cli_help(self):
        """Test that the CLI help command works."""
        result = self.runner.invoke(main, ['--help'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('CosmosDB Isolation Utilities', result.output)

    def test_cli_version(self):
        """Test that the CLI version command works."""
        result = self.runner.invoke(main, ['--version'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('0.1.0', result.output)

    def test_cli_missing_required_params(self):
        """Test that the CLI requires all required parameters."""
        result = self.runner.invoke(main, ['test'])
        self.assertNotEqual(result.exit_code, 0)
        self.assertIn('Missing required connection parameters', result.output)

    def test_cli_subcommands(self):
        """Test that all subcommands are available."""
        # Test each subcommand help
        subcommands = ['test', 'status', 'dump', 'upload', 'delete-db']
        
        result = self.runner.invoke(main, ['--help'])
        self.assertEqual(result.exit_code, 0)
        
        for subcommand in subcommands:
            self.assertIn(subcommand, result.output)

    def test_cli_common_params(self):
        """Test that common parameters are available for all subcommands."""
        # Test that endpoint, key, and database are required
        result = self.runner.invoke(main, ['--help'])
        self.assertEqual(result.exit_code, 0)
        self.assertIn('--endpoint', result.output)
        self.assertIn('--key', result.output)
        self.assertIn('--database', result.output)
        
        # Test that subcommand help shows the common parameters
        result = self.runner.invoke(main, ['-e', 'dummy', '-k', 'dummy', '-d', 'dummy', 'test', '--help'])
        self.assertEqual(result.exit_code, 0)


if __name__ == '__main__':
    unittest.main()
