import click
import pandas as pd
from .api import M
import os
import warnings
import numpy
import importlib
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
@click.argument('script')
@click.option('--feature_set', help='Feature set file, in long tabular format')
@click.option('--class_set', help='Class set file')
@click.option('--output_format', help='output format of the model')
@click.option('--output', '-o', help='output file path')
@click.option('--verbose', help='turn on verbose or not', is_flag=True)
@click.pass_context
def train(ctx, script, feature_set, class_set, output_format, output, verbose):
    """
        Function to apply script (model training) to feature and class set

        script: 
        Python script to be applied to specified files.
    """
    # parse extra input args
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}
    if script.endswith('.py'):
        script_path = os.path.abspath(script)
        sys.path.insert(0, os.path.dirname(script_path))
        script_module = importlib.import_module(os.path.splitext(os.path.basename(script))[0])
    else:
        script_module = importlib.import_module('mhealth.scripts.models.' + script)
    
    model_class = script_module.init(verbose, feature_set, class_set)
    model_class.train(**kwargs)
    model_class.export_model(output_format, output)

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('script')
@click.option('--model', help='model bundle file')
@click.option('--input_format', help='model bundle file format')
@click.option('--test_set', help='Test feature set file, in long tabular format')
@click.option('--gt_set', help='Ground truth class set file')
@click.option('--output', '-o', help='output file path in csv')
@click.option('--verbose', help='turn on verbose or not', is_flag=True)
@click.pass_context
def test(ctx, script, model, input_format, test_set, gt_set, output, verbose):
    """
        Function to apply script (model testing) to feature and class set

        script: 
        Python script to be applied to specified files.
    """
    # parse extra input args
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}
    if script.endswith('.py'):
        script_path = os.path.abspath(script)
        sys.path.insert(0, os.path.dirname(script_path))
        script_module = importlib.import_module(os.path.splitext(os.path.basename(script))[0])
    else:
        script_module = importlib.import_module('mhealth.scripts.models.' + script)
    
    model_class = script_module.init(verbose, None, None)
    pred_df = model_class.test(model, test_set, gt_set, input_format, verbose, **kwargs)
    model_class.export_test(output)


main.add_command(train)
main.add_command(test)