import click
import pandas as pd
import numpy as np
from .api import utils
from .api.helpers import importer, summarizer, visualizer, parser
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
@click.option('--output', '-o', help='Specify output folder')
@click.option('--method', '-m', default='enmo', help='summarization methods')
@click.option('--window', '-w', default=5, help='summarization window size in seconds')
@click.pass_context
def describe(ctx, input_file, output, method, window):
    """
        Function to describe different type of files
    """
    
    filetype = utils.extract_file_type(input_file)
    click.echo('Describing ' + filetype + '...')
    click.echo('Loading file...')
    if filetype == 'annotation':
        df = importer.import_annotation_file_mhealth(input_file, verbose=True)
        result = summarizer.summarize_annotation(df)
        chart = visualizer.view_annotation_summary(result)
    elif filetype == 'sensor':
        df = importer.import_sensor_file_mhealth(input_file, verbose=True)
        result = summarizer.summarize_sensor(df, method=method, window=window)
        chart = visualizer.view_sensor_summary(result)
    if output:
        if not os.path.exists(output):
            os.makedirs(output)
        output_file = os.path.join(output, os.path.splitext(os.path.basename(input_file))[0] + '.summary.csv')
        output_graph = output_file.replace('csv', 'html')
        click.echo('Saving summarization: ' + output_file)
        result.to_csv(output_file, index=False, float_format='%.1f')
        if chart:
            click.echo('Saving summarization graph: ' + output_file)
            chart.save(output_graph)
    else:
        click.echo(result.to_csv(index=False, float_format='%.3f'))
    # visualizer.view_annotation_summarization(df)
    # chart = visualizer.view_annotation_summary(df)

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('input_file')
@click.option('--output', '-o', help='Specify output folder')
@click.pass_context
def visualize(ctx, input_file, output):
    """
        Function to visualize different type of files
    """
    
    filetype = utils.extract_file_type(input_file)
    datatype = utils.extract_datatype(input_file)
    click.echo('Describing ' + filetype + ':' + datatype + '...')
    click.echo('Loading file...')
    if filetype == 'annotation':
        raise NotImplementedError("Not implemented")
    elif filetype == 'sensor':
        raise NotImplementedError("Not implemented")
    elif filetype == 'feature':
        df = pd.read_csv(input_file, parse_dates=[0], infer_datetime_format=True)
        chart = visualizer.view_sensor_summary(df)
    if not output:
        output = '.'
    if not os.path.exists(output):
        os.makedirs(output)
    output_graph = os.path.join(output, os.path.splitext(os.path.basename(input_file))[0] + '.html')
    click.echo('Saving summarization graph: ' + output_graph)
    chart.save(output_graph)

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('input_folder')
@click.option('--output', '-o', help='Specify output folder')
@click.option('--location_pattern', '-l', help='location pattern in the filename')
@click.option('--pid_pattern', '-p', help='pid pattern in the filepath')
@click.pass_context
def parse_location(ctx, input_folder, output, location_pattern, pid_pattern):
    """
        Function to parse location
    """
    if not os.path.exists(output):
        click.echo('Creating output folder')
        os.makedirs(output)
    print('input_folder: ' + os.path.abspath(input_folder))
    print('location_pattern: ' + location_pattern)
    print('pid_pattern: ' + pid_pattern)
    result = parser.parse_location_mapping(input_folder, location_pattern, pid_pattern)
    output_filepath = os.path.join(output, 'location_mapping.csv')
    result.to_csv(output_filepath, index=False)

main.add_command(describe)
main.add_command(parse_location)
main.add_command(visualize)