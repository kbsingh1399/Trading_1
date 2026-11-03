import multiprocessing as mp
import sys, time
from pathlib import Path
sys.path.insert(0, '/home/user/Trading_2/.audit/patched')
from Terminal.runtime_io import writer_lock

def a(target, inside, release):
    with writer_lock(target):
        inside.set()
        release.wait(5)

def b(target, acquired, release):
    try:
        with writer_lock(target):
            acquired.set()
            release.wait(5)
    except TimeoutError:
        pass

if __name__ == '__main__':
    target = Path('/home/user/Trading_2/.audit/race_target_patched.parquet')
    target.unlink(missing_ok=True)
    inside=mp.Event(); release_a=mp.Event(); b_acquired=mp.Event(); release_b=mp.Event()
    pa=mp.Process(target=a,args=(target,inside,release_a)); pa.start()
    assert inside.wait(3)
    pb=mp.Process(target=b,args=(target,b_acquired,release_b)); pb.start()
    time.sleep(0.2)
    release_a.set(); pa.join(3)
    assert b_acquired.wait(3), 'B did not acquire old inode'
    # While B holds the unlinked old inode, C can create/acquire a new inode.
    try:
        with writer_lock(target):
            print('PATCHED_RACE_REPRODUCED=True')
    except TimeoutError:
        print('PATCHED_RACE_REPRODUCED=False')
    release_b.set(); pb.join(3)
