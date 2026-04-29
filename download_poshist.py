#!//home/thomas-jacquot/.conda/envs/up2dategts/bin/python
import numpy as np
import argparse
import datetime
import os
from gdt.missions.fermi.gbm.finders import ContinuousFtp
from gdt.missions.fermi.time import Time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='GBM Targeted Search', \
                                     description='The GBM coherent targeted search')
    parser.add_argument("--time", required=True)
    parser.add_argument("--format", required=True, choices=['gps', 'fermi', 'datetime'])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

if args.format == 'datetime':
    value = datetime.datetime.fromisoformat(args.time)
    print(value)
else:
    value = float(args.time)
trigger = Time(value, format=args.format)

GRB_FERMI_TIME = trigger.fermi
#download the poshist file
os.makedirs(f"{args.output}", exist_ok=True)
trigger_id = Time(f"{GRB_FERMI_TIME}", format='fermi')
ftp = ContinuousFtp(trigger_id)
ftp.get_cspec(f"{args.output}")
ftp.get_poshist(f"{args.output}")