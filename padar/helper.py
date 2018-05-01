import click
import pandas as pd
from .api import utils
from .api.helpers import importer
import os
import warnings
import sys

@click.group()
@click.pass_context
def main(ctx):
    """[summary]
    
    [description]
    """
    ctx.obj={}

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('input_file')
@click.pass_context
def sampling_rate(ctx, input_file):
    """
        Function to compute sampling rate for the given file
    """
    click.echo('Loading file...')
    # df = importer.import_sensor_file_mhealth(input_file, verbose=True)
    df = pd.read_csv(input_file, parse_dates=[0], infer_datetime_format=True)
    click.echo('Computing sampling rate...')
    sr = utils._sampling_rate(df) / 600.0
    click.echo('Sampling rate:' + str(sr) + " Hz")

main.add_command(sampling_rate)