import click
import pandas as pd
from .api import M
import os
import warnings
import numpy
import importlib
import sys
from .apps import data_quality_check as dqc_app

@click.group()
@click.option('--pid', '-p', help='If provided, things will be tuned for specific participant')
@click.option('--root', '-r', required=True, help='root folder of a mhealth structure')
@click.pass_context
def main(ctx, root, pid):
    """[summary]
    
    [description]
    """
    ctx.obj={}
    ctx.obj['PID'] = pid
    if root:
        ctx.obj['M'] = M(root)
    else:
        ctx.obj['M'] = None
            
@click.command()
@click.pass_context
def summary(ctx):
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    rel_path = ""
    if ctx.obj['PID']:
        rel_path = ctx.obj['PID']
    m = ctx.obj['M']
    result = m.summarize(rel_path, use_parallel=False, verbose=False)
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command()
@click.pass_context
def merge_subject_meta(ctx):
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    rel_path = ""
    m = ctx.obj['M']
    result = m.merged_subject_meta()
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command()
@click.pass_context
def merge_session_meta(ctx):
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    rel_path = ""
    m = ctx.obj['M']
    result = m.merged_session_meta()
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command()
@click.pass_context
@click.argument('filename')
def merge_location_mapping(ctx, filename):
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    rel_path = ""
    m = ctx.obj['M']
    result = m.merged_location_mapping(filename)
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command()
@click.pass_context
def quality_check(ctx):
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    rel_path = ""
    m = ctx.obj['M']
    app = dqc_app.App(m)
    app.init_layout()
    app.setup_callbacks()
    app.run()

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('script')
@click.option('--pattern', help='glob wild card pattern (relative path) to match files to be processed. If omit, will process all files in MasterSynced folder.')
@click.option('--par', help='use parallel or not', is_flag=True)
@click.option('--verbose', help='turn on verbose', is_flag=True)
@click.option('--violate', help='violate mhealth convention except for PID extraction', is_flag=True)
@click.pass_context
def process(ctx, script, pattern, par, verbose, violate):
    """
        Function to apply script to any files matched the pattern

        script: 
        Python script to be applied to specified files. This script file should have a main function with signature as follows:

            def main(input_file, verbose=False, **kwargs)

        And it should return a pandas dataframe, which should be identifiable (by adding idenfier column) after merging with other matched files
    """
    # parse input args
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}
    if script.endswith('.py'):
        script_path = os.path.abspath(script)
        sys.path.insert(0, os.path.dirname(script_path))
        script_module = importlib.import_module(os.path.splitext(os.path.basename(script))[0])
    else:
        script_module = importlib.import_module('mhealth.scripts.' + script)
    # func = script_module.main
    func = script_module.build
    if pattern is None:
        pattern = ""
    pattern.strip("'").strip('"')
    
    rel_pattern = ""
    if ctx.obj['PID']:
        rel_pattern = os.path.join(ctx.obj['PID'], pattern)
    else:
        rel_pattern = pattern

    if par:
        use_parallel = True
    else:
        use_parallel = False
    m = ctx.obj['M']
    result = m.process(rel_pattern, func, use_parallel=use_parallel, verbose=verbose, violate=violate, **kwargs)
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))
    if script.endswith('.py'):
        sys.path.remove(os.path.dirname(script_path))

main.add_command(summary)
main.add_command(process)
main.add_command(quality_check)
main.add_command(merge_subject_meta)
main.add_command(merge_location_mapping)
main.add_command(merge_session_meta)