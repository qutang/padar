import click
import pandas as pd
from .api import M
import os
import warnings
import numpy
import importlib
import sys

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
        rel_path = os.path.join(ctx.obj['PID'], 'MasterSynced')
    m = ctx.obj['M']
    result = m.summarize(rel_path, use_parallel=True, verbose=True)
    click.echo(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('script')
@click.option('--pattern', help='Regular expression pattern to match files to be processed', required=True)
@click.pass_context
def process(ctx, script, pattern):
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
    func = script_module.main
    pattern.strip("'").strip('"')
    rel_pattern = ""
    if ctx.obj['PID']:
        rel_pattern = os.path.join(ctx.obj['PID'], pattern)
    else:
        rel_pattern = pattern
    m = ctx.obj['M']
    result = m.process(rel_pattern, func, use_parallel=True, verbose=False, **kwargs)
    click.echo(result.to_csv(sep=',', index=False))
    if script.endswith('.py'):
        sys.path.remove(os.path.dirname(script_path))

main.add_command(summary)
main.add_command(process)