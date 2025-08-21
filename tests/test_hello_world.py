"""Tests for the HelloWorld module."""

import unittest

from cosmos_isolation_utils.hello_world import HelloWorld


class TestHelloWorld(unittest.TestCase):
    """Test cases for the HelloWorld class."""

    def test_greeting_response(self):
        """Test that the greet method returns the expected greeting."""
        hello_world = HelloWorld()
        result = hello_world.greet()
        self.assertEqual(result, 'Hello')

    def test_greeting_type(self):
        """Test that the greet method returns a string."""
        hello_world = HelloWorld()
        result = hello_world.greet()
        self.assertIsInstance(result, str)

    def test_multiple_instances(self):
        """Test that multiple instances work correctly."""
        hello1 = HelloWorld()
        hello2 = HelloWorld()
        
        self.assertEqual(hello1.greet(), hello2.greet())
        self.assertEqual(hello1.greet(), 'Hello')


if __name__ == '__main__':
    unittest.main()
