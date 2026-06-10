#!/opt/up2dategts/bin/python
import numpy as np
import argparse
import datetime
import os
from gdt.missions.fermi.gbm.finders import ContinuousFtp
from gdt.missions.fermi.time import Time


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='GBM Targeted Search', \
                                     description='The GBM coherent targeted search')
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--end-time", required=True)
    parser.add_argument("--format", required=True, choices=['gps', 'fermi', 'datetime'])
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    triggers = np.arange(np.datetime64(args.start_time), np.datetime64(args.end_time), 86400)
    datetime_array = [datetime.datetime.fromisoformat(f'{trigger}') for trigger in triggers]
    time_array = Time(datetime_array, format="datetime")

    # check for files
    tte_files = []
    for trigger in triggers:
        path = os.path.join(args.output, str(trigger).split('T')[0], 'poshist_cspec')
        datetime_trigger = datetime.datetime.fromisoformat(str(trigger))
        time_trigger = Time(datetime_trigger, format="datetime")
        finder =  ContinuousFtp(time_trigger)
        finder.get_cspec(path)
        finder.get_poshist(path)