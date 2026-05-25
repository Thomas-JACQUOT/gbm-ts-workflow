#!/home/thomas-jacquot/.conda/envs/up2dategts/bin/python
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
        start_value = datetime.datetime.fromisoformat(args.start_time)
        end_value = datetime.datetime.fromisoformat(args.end_time)
    else:
        start_value = float(args.start_time)
        end_value = float(args.end_time)
        start_trigger = Time(start_value, format=args.format)
        end_trigger = Time(end_value, format=args.format)
        triggers = np.arange(start_trigger, end_trigger, 86400)

    # check for files
    tte_files = []
    for trigger in triggers:
        path = os.path.join(args.output, str(trigger).split('T')[0], 'poshist_cspec')
        tte_wildcard = f"{path}/*tte_??_*.fit*"
        finder =  ContinuousFtp(trigger)
        finder.get_cspec(path)
        finder.get_poshist(path)