import click
import pandas as pd
from .api import M
import os
import warnings
import numpy
import importlib
import sys
from .scripts.multilocation_2017 import FeatureSetPreparer
from .scripts.models.MDCAS import MDCASClassifier

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
@click.option('--output_file', '-o', help='output file path')
@click.pass_context
def compute_features(ctx, input_file, output_file):
    """
        Function to compute features for MDCAS classifier
    """
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}
    output_file = os.path.abspath(output_file)
    click.echo("Init feature set computation")
    executor = FeatureSetPreparer.build(verbose=True, violate=True, independent=True, session_file=None, location_mapping_file = None, orientation_fix_file=None, ws=12800, ss=12800, threshold=0.2, subwins=4, skip_post=True, **kwargs)
    click.echo("Compute feautures")
    result = executor(input_file)
    if not os.path.exists(os.path.dirname(output_file)):
        click.echo("Create output folder if not exists")
        os.makedirs(os.path.dirname(output_file))
    click.echo("Save feature set to: " + output_file)
    result.to_csv(output_file, index=False, float_format='%.6f')
    click.echo("Saved")

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('input_file')
@click.option('--model', '-m', help='model bundle file')
@click.option('--output_file', '-o', help='output file path in csv')
@click.pass_context
def test(ctx, input_file, model, output_file):
    """
        Run test on data using MDCAS classifier
    """
    # parse extra input args
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}
    if 'use_groups' in kwargs:
        if kwargs['use_groups']:
            no_groups = 0
        else:
            no_groups = 1
    else:
        no_groups = 1
    click.echo('Init model from: ' + model)
    model_class = MDCASClassifier.init(True, None, None)
    click.echo('Make prediction on: ' + input_file)
    pred_df = model_class.test(model_bundle_file = model, test_set_file=input_file, gt_set_file=None, input_format='joblib', verbose=True, prob=1, no_groups=no_groups)
    click.echo('Save predictions to: ' + output_file)
    model_class.export_test(output_file)
    click.echo('Saved')

main.add_command(compute_features)
main.add_command(test)