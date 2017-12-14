import click
import pandas as pd
from .api import M

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
@click.argument('info', type=click.Choice(['participants', 'annotators', 'sensors']))
@click.pass_context
def list(ctx, info):
    """[summary]
    
    Without PID, it supports: participants|annotators|sensors; with PID, it supports: annotators|sensors
    
    Decorators:
        click
    """
    m = ctx.obj['M']
    p = ctx.obj['PID']
    if not ctx.obj['PID']:
        if info == 'participants':
            for p in m.participants:
                click.echo(p)
        elif info =='annotators':
            for a in m.annotators(""):
                click.echo(a)
        elif info == 'sensors':
            for s in m.sensors(""):
                click.echo(s)
        else:
            click.echo(info + ' not supported', err=True)
    else:
        if info == 'sensors':
            for s in m.sensors(p):
                click.echo(s)
        elif info =='annotators':
            for a in m.annotators(p):
                click.echo(a)
        else:
            click.echo(info + ' not supported', err=True)

@click.command()
@click.argument('info', type=click.Choice(['annotators', 'sensors', 'all']))
@click.option('--by', '-b', default='all', type=click.Choice(['all', 'each', 'day', 'hour']), help = 'Specify how to summarize size. It will be ignored if pid is not provided.')
@click.pass_context
def size(ctx, info, by):
    """[summary]
    
    INFO supports: annotators|sensors|all
    
    Decorators:
        click
    """
    m = ctx.obj['M']
    p = ctx.obj['PID']
    if not ctx.obj['PID']:
        if info == 'all':
            if by == 'each':
                for p in m.participants:
                    total_size = m.folder_size(p) / 1024/ 1024.0
                    click.echo('%s,%.3f' % (p, total_size))
            elif by == 'all':
                all_size = 0
                for p in m.participants:
                    total_size = m.folder_size(p) / 1024/ 1024.0
                    all_size = all_size + total_size
                click.echo('%s,%.3f' % ('All participants', all_size))
        elif info =='annotators':
            for a in m.annotators(""):
                total_size = m.total_size('**/*' + a + '*.annotation.csv') / 1024 / 1024.0
                click.echo('%s,%.3f' % (a, total_size))
        elif info == 'sensors':
            if by == 'each':
                for s in m.sensors(""):
                    total_size = m.total_size('**/*' + s + '*.sensor.csv') / 1024 / 1024.0
                    click.echo('%s,%.3f' % (s, total_size))
            elif by == 'all':
                all_size = 0
                for s in m.sensors(""):
                    total_size = m.total_size('**/*' + s + '*.sensor.csv') / 1024 / 1024.0
                    all_size = all_size + total_size
                click.echo('%s,%.3f' % ('All participants', all_size))
        else:
            click.echo(info + ' not supported', err=True)
    else:
        if info == 'sensors':
            dfs = []
            all_size = 0
            for s in m.sensors(p):
                if by == 'each':
                    total_size = m.total_size(p + '/**/*' + s + '*.sensor.csv') / 1024 / 1024.0
                    click.echo('%s,%.3f' % (s, total_size))
                elif by == 'all':
                    total_size = m.total_size(p + '/**/*' + s + '*.sensor.csv') / 1024 / 1024.0
                    all_size = all_size + total_size
                else:
                    size_df = m.file_sizes(p + '/**/*' + s + '*.sensor.csv', by=by)
                    size_df['size'] = size_df['size'] / 1024 / 1024.0
                    size_df['sensor_id'] = s
                    dfs.append(size_df)
            if by == 'day' or by == 'hour':
                result = pd.concat(dfs, axis=0)
            if by == 'all':
                click.echo('%s,%.3f' % (p, all_size))
            if by == 'day':
                result = result[['sensor_id', 'date', 'size']].sort_values(by=['sensor_id', 'date'])
            elif by == 'hour':
                result = result[['sensor_id', 'date', 'hour', 'size']].sort_values(by=['sensor_id', 'date', 'hour'])
            if by == 'day' or by == 'hour':
                click.echo(result.to_csv(index=False, float_format='%.3f'))
        elif info =='annotators':
            dfs = []
            all_size = 0
            for a in m.annotators(p):
                if by == 'each':
                    total_size = m.total_size(p + '/**/*' + a + '*.annotation.csv') / 1024 / 1024.0
                    click.echo('%s,%.3f' % (a, total_size))
                elif by == 'all':
                    total_size = m.total_size(p + '/**/*' + a + '*.annotation.csv') / 1024 / 1024.0
                    all_size = all_size + total_size
                else:
                    size_df = m.file_sizes(p + '/**/*' + a + '*.annotation.csv', by=by)
                    size_df['size'] = size_df['size'] / 1024 / 1024.0
                    size_df['annotator'] = a
                    dfs.append(size_df)
            if by == 'day' or by == 'hour':
                result = pd.concat(dfs, axis=0)
            if by == 'all':
                click.echo('%s,%.3f' % (p, all_size))
            if by == 'day':
                result = result[['annotator', 'date', 'size']].sort_values(by=['annotator', 'date'])
            elif by == 'hour':
                result = result[['annotator', 'date', 'hour', 'size']].sort_values(by=['annotator', 'date', 'hour'])
            if by == 'day' or by == 'hour':
                click.echo(result.to_csv(index=False, float_format='%.3f'))
        elif info == 'all':
            if by == 'all':
                total_size = m.folder_size(p) / 1024 / 1024.0
                click.echo('%s,%.3f' % (p, total_size))
            else:
                click.echo('by: ' + by + " is not supported when info is all")
        else:
            click.echo(info + ' not supported', err=True)
            
@click.command()
def qc():
    """[summary]
    
    [description]
    
    Decorators:
        click
    """
    click.echo("quality check!")

main.add_command(qc)
main.add_command(list)
main.add_command(size)
