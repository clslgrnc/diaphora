import os
import shutil
import sqlite3
import subprocess
import sys
from concurrent.futures import ALL_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path
from time import time

IDA = "/home/user/idapro-8.2/idat64"
DIAPHORA = "diaphora-3.0/diaphora.py"

###############################################
# Database merge taken from
# https://stackoverflow.com/a/68526717


def merge_databases(db1, db2):
    con3 = sqlite3.connect(db1)

    con3.execute("ATTACH '" + db2 + "' as dba")

    con3.execute("BEGIN")
    for row in con3.execute("SELECT * FROM dba.sqlite_master WHERE type='table'"):
        combine = "INSERT OR IGNORE INTO " + row[1] + " SELECT * FROM dba." + row[1]
        print(combine)
        con3.execute(combine)
    con3.commit()
    con3.execute("detach database dba")


###############################################


def get_idb(target):
    target_idb = target.parent / (target.name + ".i64")
    if not target_idb.exists():
        subprocess.run(
            [IDA, "-Llog.txt", "-B", str(target)],
            env={
                "TVHEADLESS": "1",
                "HOME": os.getenv("HOME", ""),
                "IDAUSR": os.getenv("IDAUSR", ""),
            },
        )
    return target_idb


def export_part(args):
    target_idb, part, nbr_parts = args
    target = target_idb[: -len(".i64")] + str(part) + ".i64"
    shutil.copyfile(target_idb, target)
    os.system(
        f'TVHEADLESS=1 ~/idapro-8.2/./idat64 -a -A -S"{DIAPHORA} {part} {nbr_parts}" -Llog{part}.txt {target}'
    )


if __name__ == "__main__":
    start = time()
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <file>")
    target = Path(sys.argv[1]).resolve()
    target_idb = str(get_idb(target))
    #  export_part((target_idb, 0, 1))
    print(time() - start)

    #  sys.exit(0)
    MAX_WORKERS = 5
    NBR_PARTS = 10
    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(export_part, (target_idb, i, NBR_PARTS))
            for i in range(NBR_PARTS)
        ]

    wait(futures, return_when=ALL_COMPLETED)
    print(time() - start)

    db_files = [
        str(target.parent / f"{target.name}{i}.sqlite") for i in range(NBR_PARTS)
    ]
    for db_file in db_files[1:]:
        merge_databases(db_files[0], db_file)
    print(time() - start)
