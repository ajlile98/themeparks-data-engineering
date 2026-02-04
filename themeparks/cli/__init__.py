"""
CLI Module - Command-line interface for theme parks data pipeline.

Provides commands for:
- extract: Extract data from API
- transform: Transform extracted data
- load: Load data to targets
- validate: Validate data quality
"""

import click


@click.group()
@click.version_option(version="2.0.0")
def cli():
    """
    Theme Parks Data Pipeline CLI.
    
    A modular, Kubernetes-ready data pipeline for theme park data.
    """
    pass


# Import subcommands
from themeparks.cli.extract import extract_command
from themeparks.cli.transform import transform_command
from themeparks.cli.load import load_command
from themeparks.cli.validate import validate_command

# Register commands
cli.add_command(extract_command, name="extract")
cli.add_command(transform_command, name="transform")
cli.add_command(load_command, name="load")
cli.add_command(validate_command, name="validate")
