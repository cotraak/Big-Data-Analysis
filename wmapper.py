#!/usr/bin/env python
import sys

def mapToColDict(lst):
    return {'ID':lst[0],
            'DATE':lst[1],
            'TYPE':lst[2],
            'VALUE':lst[3],
            'MFLAG':lst[4],
            'QFLAG':lst[5],
            'SFLAG':lst[6],
            'OBSTIME':lst[7]}

for line in sys.stdin:
    parse = line.strip().upper().split(',')
    row = mapToColDict(parse)
    if 'TMAX' != row['TYPE'] and 'TMIN' != row['TYPE']:
        continue
    if row['VALUE'] == -9999:
        continue
    if row['SFLAG'] == '':
        continue
    if row['QFLAG'] != '':
        continue
    if row['MFLAG'] == 'P':
        contiune
    print '%s,%s,%s,%s,' % (row['DATE'],row['ID'],row['TYPE'],row['VALUE'])
