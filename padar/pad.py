import click
import pandas as pd
from .api import M
import os
import warnings
import numpy
import importlib
import sys
from .utility import logger
from .utility.package_helper import *

@click.group()
@click.option('--pid', '-p', help="The participant ID (folder name) to run the command on. If it is not provided, the command will run against all participants' data")
@click.option('--root', '-r', help='The root folder for a dataset in mhealth convention. If it is not provided, the default is current folder.', default='.')
@click.pass_context
def main(ctx, root, pid):
    """Command entry to run customized script to process raw accelerometer data and annotations stored in mhealth convention (hourly files).
    """
    ctx.obj={}
    ctx.obj['PID'] = pid
    ctx.obj['root'] = root
    
@click.command()
@click.pass_context
def summary(ctx):
    """[summary]
    
    [description]
    """
    rel_path = ""
    if ctx.obj['PID']:
        rel_path = ctx.obj['PID']
    if ctx.obj['root']:
        m = M(ctx.obj['root'])
    else:
        m = None
    result = m.summarize(rel_path, use_parallel=False, verbose=False)
    output(result.to_csv(sep=',', index=False, float_format='%.3f'))

@click.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True
))
@click.argument('script')
@click.option('--pattern', '-p', help="Glob wild card pattern to select files to be processed that is relative to the participant's folder path if PID is provided, otherwise it is relative to the root folder of the dataset. If omit, will process all csv files recursively in the parent folder.")
@click.option('--par', help='If using this flag, files will be processed in parrallel', is_flag=True)
@click.option('--violate', help='If using this flag, the script will not extract meta information from the filenames of raw data and append them as columns in the output csv file.', is_flag=True)
@click.option('--output', '-o', help='Output file path relative to the PID folder or root folder of the dataset', default=None)
@click.pass_context
def process(ctx, script, pattern, par, violate, output):
    """
        Apply data processing script to selected data

        script: 
        Python script to be applied to specified files. padar package provides a rich set of built-in scripts that cover most accelerometer data processing tasks.

        Use command `pad script -l` to see a list of built-in script
        
        Use command `pad script -n SCRIPT_NAME` to check the usage and examples for each built-in script

        And it should return a pandas dataframe, which should be identifiable (by adding idenfier column) after merging with other matched files
    """
    
    logger.info('Start execute command')
    logger.info('Selected dataset root folder: ' + os.path.abspath(ctx.obj['root']))
    logger.info('Selected PID: ' + str(ctx.obj['PID']))
    logger.info('Selected script: ' + script)
    logger.info('Wild card pattern to select files: ' + str(pattern))
    logger.info('Use parallel: ' + str(par))
    logger.info('Violate mhealth filename convention: ' + str(violate))

    if ctx.obj['root']:
        m = M(ctx.obj['root'])
    else:
        m = None

    # parse input args
    kwargs = {ctx.args[i][2:]: ctx.args[i+1].strip('"') for i in range(0, len(ctx.args), 2)}

    try:
        if script.endswith('.py'):
            script_path = os.path.abspath(script)
            sys.path.insert(0, os.path.dirname(script_path))
            module_name = os.path.splitext(os.path.basename(script))[0]
        else:
            module_name = 'padar.scripts.' + script
        script_module = importlib.import_module(module_name)
    except ModuleNotFoundError:
        logger.error('Script is not found: ' + module_name)
        exit(1)
    func = script_module.build

    # process pattern parameter
    if pattern is None:
        pattern = "**/*.csv"
    pattern.strip("'").strip('"')
    rel_pattern = ""
    if ctx.obj['PID']:
        rel_pattern = os.path.join(ctx.obj['root'], ctx.obj['PID'], pattern)
    else:
        rel_pattern = os.path.join(ctx.obj['root'], pattern)

    logger.info('Processed wild card pattern: ' + os.path.abspath(rel_pattern))

    # process output filepath
    if output is None:
        output_filepath = None
        logger.info('Processed output filepath: ' + str(output_filepath))
    else:
        if ctx.obj['PID']:
            output_filepath = os.path.join(ctx.obj['root'], ctx.obj['PID'], output)
        else:
            output_filepath = os.path.join(ctx.obj['root'], output)
        logger.info('Processed output filepath: ' + os.path.abspath(output_filepath))

    # process parallel flag
    use_parallel = par
    
    # run process engine and return result (result should be a pandas dataframe)
    logger.info('Start processing')
    result = m.process(rel_pattern, func, use_parallel=use_parallel, verbose=True, violate=violate, **kwargs)
    logger.info('Finish processing')
    
    if not result.empty:
        logger.output(result.to_csv(sep=',', index=False, float_format='%.3f'))
    
    if output is not None and not result.empty:
        logger.info('Save results to ' + os.path.abspath(output_filepath))
        os.makedirs(os.path.dirname(output_filepath), exist_ok=True)
        result.to_csv(output_filepath, index=False, float_format='%.9f')

     # clear up python search path
    if script.endswith('.py'):
        sys.path.remove(os.path.dirname(script_path))

@click.command()
@click.option('--name', '-n', help="List the usage and examples of the script <name>", default=None)
@click.option('--list', '-l', help="List all available built-in scripts", is_flag=True)
def script(name, list):
    """Inspect the built-in scripts

    Inspect the names, usage and examples of built-in scripts used for `pad process` command.
    """
    if list:
        logger.info('Run command: `pad script -l`')
        modules = list_modules('padar','scripts')
        logger.info('Found %d built-in scripts' % len(modules))
        logger.output('\n'.join(modules))
    else:
        logger.info('Run command: `pad script -n ' + name + '`')
        logger.output(get_doc('padar.scripts.' + name))


@click.command()
@click.pass_context
@click.argument('content')
def ls(ctx, content):
    """[summary]
    
    [description]
    
    """
    rel_path = ""
    if ctx.obj['PID']:
        rel_path = ctx.obj['PID']
    if ctx.obj['root']:
        m = M(ctx.obj['root'])
    else:
        m = None
    
    if content == 'sensors':
        result = m.sensors(rel_path)
        click.echo("\r\n".join(result))
    elif content == 'participants':
        result = m.participants
        click.echo('\r\n'.join(result))
    elif content == 'annotators':
        result = m.annotators(rel_path)
        click.echo('\r\n'.join(result))

# main.add_command(summary)
main.add_command(process)
main.add_command(script)
# main.add_command(ls)