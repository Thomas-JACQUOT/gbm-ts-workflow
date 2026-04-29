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
    parser.add_argument("--input-path", required=True)
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

    path = args.input_path
    tte_wildcard = f"{path}/*tte_??_*.fit*"

    # check for files
    tte_files = []
    det_list = np.array(["n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "na", "nb", "b0", "b1"])
    for det in det_list:
        tte_files.extend(glob.glob(tte_wildcard.replace("??", det)))

    if len(tte_files) < len(det_list):
        finder =  ContinuousFtp(trigger, protocol="HTTPS")
        tte_files = [finder.get_tte(path, dets=[det])[0] for det in det_list]

if __name__ == "__main__":
    main()
