#!/home/thomas-jacquot/.conda/envs/up2dategts/bin/python
import numpy as np
import os
import glob
import datetime
import argparse

from gdt.missions.fermi.gbm.finders import ContinuousFtp
from gdt.missions.fermi.time import Time

def main():
    parser = argparse.ArgumentParser(prog='GBM Targeted Search', \
                                     description='The GBM coherent targeted search')
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--end-time", required=True)
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
        path = os.path.join(args.output, str(trigger).split('T')[0], 'tte')
        tte_wildcard = f"{path}/*tte_??_*.fit*"
        det_list = np.array(["n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "na", "nb", "b0", "b1"])
        for det in det_list:
            tte_files.extend(glob.glob(tte_wildcard.replace("??", det)))
        finder =  ContinuousFtp(trigger)
        tte_files = [finder.get_tte(path, dets=[det], full_day=True)[0] for det in det_list]

if __name__ == "__main__":
    main()
