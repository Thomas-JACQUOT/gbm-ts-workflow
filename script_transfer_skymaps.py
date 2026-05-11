#!/home/cosmin.stachie/venv/last_targeted_search/bin/python
import numpy as np
import os, glob
import h5py
from argparse import ArgumentParser
from shutil import copy

if __name__ == "__main__":
    parser = ArgumentParser(prog='GBM Targeted Search', \
                                     description='The GBM coherent targeted search')
    parser.add_argument('-i', '--input_path', type = str, help='Path to the input files')
    parser.add_argument('-o', '--output_path', type = str, help='Path to the output files')
args = parser.parse_args()


input_path = args.input_path
output_path = args.output_path
if (os.path.isdir(output_path) == False):
        os.mkdir(output_path)
output_path_with_earth = os.path.join(output_path, "with_earth")
if (os.path.isdir(output_path_with_earth) == False):
        os.mkdir(output_path_with_earth)
output_path_without_earth = os.path.join(output_path, "without_earth")
if (os.path.isdir(output_path_without_earth) == False):
        os.mkdir(output_path_without_earth)


os.chdir(input_path)
names_dir = glob.glob("*")


for name in names_dir:
        os.chdir(name)
        if (len(glob.glob("*with_earth.fit")) != 0):
                for filename in glob.glob("*with_earth.fit"):
                        copy(filename, output_path_with_earth)
        if (len(glob.glob("*without_earth.fit")) != 0):
                for filename in glob.glob("*without_earth.fit"):
                        copy(filename, output_path_without_earth)						
        os.chdir("..")

print(output_path_with_earth)

