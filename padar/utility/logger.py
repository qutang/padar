"""Helper functions for terminal programs
"""

import click
import time
import inspect

def caller():
    """Get a name of a caller in the format module.class.method
    
       `skip` specifies how many levels of stack to skip while getting caller
       name. skip=1 means "who calls me", skip=2 "who calls my caller" etc.
       
       An empty string is returned if skipped levels exceed stack height
    """

    def check_caller(caller_module):
        if caller_module is not None:
            return 'padar.pad' in caller_module.__name__ or 'padar.api.dataset' in caller_module.__name__
        else:
            return False

    stack = inspect.stack()
    frames = [frame[0] for frame in stack]
    caller_modules = [inspect.getmodule(frame) for frame in frames]
    try:  
        filtered_caller_modules_bool = [check_caller(module) for module in caller_modules]
    except:
        print(caller_modules)
        exit(1)
    first_match = filtered_caller_modules_bool.index(True)
    name = []
    module = caller_modules[first_match]
    parentframe = frames[first_match]
    # `modname` can be None when frame is executed directly in console
    # TODO(techtonik): consider using __main__
    if module:
        try:
            name.append(module.__name__)
        except:
            print(caller_modules)
            exit(1)
    # detect classname
    if 'self' in parentframe.f_locals:
        # I don't know any way to detect call from the object method
        # XXX: there seems to be no way to detect static method call - it will
        #      be just a function call
        name.append(parentframe.f_locals['self'].__class__.__name__)
    codename = parentframe.f_code.co_name
    if codename != '<module>':  # top level usually
        name.append( codename ) # function or a method
    del parentframe
    return ".".join(name)

def log(text, level='INFO', color='green'):
    # TODO: make this function thread-safe using filelock
    now = time.time()
    ts = time.localtime(now)
    milliseconds = '%03d' % int((now - int(now)) * 1000)
    ts_str = time.strftime('%Y-%m-%d %H:%M:%S.', ts) + milliseconds
    ts_str2 = time.strftime('%Y-%m-%d-%H-%M-%S-', ts) + milliseconds + '-P0000'
    tokens = ts_str2.split('-')
    tokens[4] = '00'
    tokens[5] = '00'
    tokens[6] = '000'
    ts_str2 = '-'.join(tokens)
    log_filename = '%s.%s.log.csv' % (caller(), ts_str2)
    log_f = click.open_file(log_filename, mode='a')
    click.echo(
        click.style(
            '%s,%s,\"%s\"' % (level, ts_str, text),
            fg=color
        ),
        err=True
    )
    click.echo(
        click.style(
            '%s,%s,\"%s\"' % (level, ts_str, text),
            fg=color
        ),
        err=True,
        file=log_f
    )
    log_f.close()

def output(text):
    click.echo(text)

def info(text):
    log(text)

def debug(text):
    log(text, level='DEBUG', color='blue')

def warn(text):
    log(text, level='WARN', color='yellow')

def error(text):
    log(text, level='ERROR', color='red')
    
