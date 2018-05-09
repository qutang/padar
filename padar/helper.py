import click
import pandas as pd
import numpy as np
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

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('input_file')
@click.pass_context
def describe(ctx, input_file):
    """
        Function to describe different type of files
    """
    click.echo('Loading file...')
    df = importer.import_annotation_file_mhealth(input_file, verbose=True)
    filetype = utils.extract_file_type(input_file)
    click.echo('Describing ' + filetype + '...')
    by_groups = []
    if 'pid' in df.columns:
        by_groups.append('pid')
    by_groups.append(df.columns[3])
    result = df.groupby(by_groups).apply(lambda row: np.sum(row.iloc[:,2] - row.iloc[:,1]))
    result = result.reset_index()
    result = result.sort_values(by=['pid', 'LABEL_NAME'])
    print(result.to_csv())

main.add_command(sampling_rate)
main.add_command(describe)