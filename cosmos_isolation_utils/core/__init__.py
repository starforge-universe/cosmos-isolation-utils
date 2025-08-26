"""
Core implementation module for CosmosDB isolation utilities.

This module contains the actual business logic for all the utilities,
separated from the CLI interface.
"""

from .connection import test_connection
from .dump import dump_containers
from .upload import upload_entries
from .delete import delete_database
from .config import (
    DatabaseConfig, UploadConfig, DumpConfig, DeleteConfig, StatusConfig, ConnectionConfig
)
from .logging_utils import (
    log_info, log_success, log_warning, log_error, log_bold, log_panel,
    log_checkmark, log_cross, log_warning_icon, log_step, log_database_info,
    log_container_info, log_upload_summary, log_results_summary
)

__all__ = [
    'test_connection',
    'dump_containers',
    'upload_entries',
    'delete_database',
    'DatabaseConfig',
    'UploadConfig',
    'DumpConfig',
    'DeleteConfig',
    'StatusConfig',
    'ConnectionConfig',
    'log_info',
    'log_success',
    'log_warning',
    'log_error',
    'log_bold',
    'log_panel',
    'log_checkmark',
    'log_cross',
    'log_warning_icon',
    'log_step',
    'log_database_info',
    'log_container_info',
    'log_upload_summary',
    'log_results_summary'
]
