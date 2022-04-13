import os
from subprocess import check_call

def make_sure_path_exists(path):
    import errno
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise                
  
  
def copy2clip(txt):
    cmd='echo '+txt.strip()+'|clip'
    return check_call(cmd, shell=True)


def flatten(A):
    rt = []
    for i in A:
        if isinstance(i,list):
            rt.extend(flatten(i))
        else:
            rt.append(i)
    return rt
