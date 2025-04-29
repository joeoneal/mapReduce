#!/usr/bin/env python3

import sqlite3
import sys
import os

def main():
    if len(sys.argv) < 3:
        print(f'Usage: {sys.argv[0]} [dbname] [inputfile] ...', file=sys.stderr)
        sys.exit(-1)

    dbname = sys.argv[1]
    input_files = sys.argv[2:]

    db = sqlite3.connect(dbname)
    db.execute('CREATE TABLE IF NOT EXISTS pairs (key TEXT, value TEXT)')

    for fname in input_files:
        print(f'Processing {os.path.basename(fname)}')

        try:
            with open(fname, 'r', encoding='utf-8') as fp:
                offset = 0
                for line in fp:
                    line = line.rstrip('\r\n')
                    key = f'{os.path.basename(fname)}:{offset:09d}'
                    value = line
                    db.execute('INSERT INTO pairs VALUES (?, ?)', (key, value))
                    offset += len(line) + 1  # Include newline in offset
        except Exception as e:
            print(f'Error reading file {fname}: {e}', file=sys.stderr)

    db.commit()
    db.close()

if __name__ == '__main__':
    main()
