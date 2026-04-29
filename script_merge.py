#!/home/cosmin.stachie/venv/last_targeted_search/bin/python
import numpy as np
import os, glob
from argparse import ArgumentParser
import h5py
from ligo.segments import segment, segmentlist

if __name__ == "__main__":
    parser = ArgumentParser(prog='GBM Targeted Search', \
                                     description='The GBM coherent targeted search')
    parser.add_argument('-i', '--input_path', type = str, help='Path to the input files')
    parser.add_argument('-o', '--output_path', type = str, help='Path to the output files')
args = parser.parse_args()


input_path = args.input_path
output_path = args.output_path

os.chdir(input_path)
names_dir = glob.glob("*")
os.makedirs(output_path, exist_ok=True)
output_file_full = os.path.join(output_path, "merged_full_results.hdf")

odata_full = None
number = 0
live_time = segmentlist([])

for i in range(0, len(names_dir), 200):
        names_dir_aux = names_dir[i:i + 200]	
        odata_full_aux = None
        for name in names_dir_aux:
                print(number)
                number = number + 1
                os.chdir(name)
                if (glob.glob("full_results.npy") != []):
                        idata_full = np.load(glob.glob("full_results.npy")[0])	
                        if odata_full_aux is None:
                                odata_full_aux = idata_full.copy()
                        else:
                                odata_full_aux = np.vstack([odata_full_aux, idata_full])
                os.chdir("..")	
        if odata_full is None:
                odata_full = odata_full_aux.copy()
        else:
                odata_full = np.vstack([odata_full, odata_full_aux])



index_not_nan = np.argwhere(np.isnan(odata_full[:,18]) == False)
live_tstart = odata_full[index_not_nan][:,0,0] - odata_full[index_not_nan][:,0,1] / 2
live_tstop = odata_full[index_not_nan][:,0,0] + odata_full[index_not_nan][:,0,1] / 2
live_time = segmentlist([])
for i in range(len(live_tstart)):
        print(live_tstart[i], live_tstop[i])
        live_time = live_time + segmentlist([(live_tstart[i], live_tstop[i])])
live_time.coalesce()



with h5py.File(output_file_full, 'w') as of:
        ds = of.create_dataset('livetime', data = live_time)
        dtype = [('met', '<f8'),
                 ('duration', '<f8'),
                 ('in_gti', '<i'),
                 ('rock', '<i'),
                 ('good', '<i'),
                 ('best_loc_sc_phi', '<f8'),
                 ('best_loc_sc_theta', '<f8'),
                 ('best_loc_cel_phi', '<f8'),
                 ('best_loc_cel_theta', '<f8'),
                 ('best_spec', '<i'),
                 ('ampli', '<f8'),
                 ('snr', '<f8'),
                 ('snr0', '<f8'),
                 ('snr1', '<f8'),
                 ('chisq', '<f8'),
                 ('chisq+', '<f8'),
                 ('sun_angle', '<f8'),
                 ('earth_angle', '<f8'),
                 ('llr', '<f8'),
                 ('coinc_llr', '<f8'),
                 ('cr_var0', '<f8'),
                 ('cr_var1', '<f8'),
                 ('cr_var2', '<f8')]
        ds = of.create_dataset('gbmscan', (odata_full.shape[0],), dtype)
        ds['met'] = odata_full[:,0]
        ds['duration'] = odata_full[:,1]
        ds['in_gti'] = odata_full[:,2]
        ds['rock'] = odata_full[:,3]
        ds['good'] = odata_full[:,4]
        ds['best_loc_sc_phi'] = odata_full[:,5]
        ds['best_loc_sc_theta'] = odata_full[:,6]
        ds['best_loc_cel_phi'] = odata_full[:,7]
        ds['best_loc_cel_theta'] = odata_full[:,8]
        ds['best_spec'] = odata_full[:,9]
        ds['ampli'] = odata_full[:,10]
        ds['snr'] = odata_full[:,11]
        ds['snr0'] = odata_full[:,12]
        ds['snr1'] = odata_full[:,13]
        ds['chisq'] = odata_full[:,14]
        ds['chisq+'] = odata_full[:,15]
        ds['sun_angle'] = odata_full[:,16]
        ds['earth_angle'] = odata_full[:,17]
        ds['llr'] = odata_full[:,18]
        ds['coinc_llr'] = odata_full[:,19]
        ds['cr_var0'] = odata_full[:,20]
        ds['cr_var1'] = odata_full[:,21]
        ds['cr_var2'] = odata_full[:,22]


output_file_filtered = os.path.join(output_path, "merged_filtered_results.hdf")

odata_filtered = None
for name in names_dir:
        os.chdir(name)
        if (glob.glob("filtered_results.npy") != []):
                idata_filtered = np.load(glob.glob("filtered_results.npy")[0])
                if odata_filtered is None:
                        odata_filtered = idata_filtered.copy()
                else:
                        odata_filtered = np.vstack([odata_filtered, idata_filtered])
        os.chdir("..")

print("odata_filtered.shape")
print(odata_filtered.shape)

with h5py.File(output_file_filtered, 'w') as of:
        ds = of.create_dataset('livetime', data = live_time)
        dtype = [('met', '<f8'),
                 ('duration', '<f8'),
                 ('in_gti', '<i'),
                 ('rock', '<i'),
                 ('good', '<i'),
                 ('best_loc_sc_phi', '<f8'),
                 ('best_loc_sc_theta', '<f8'),
                 ('best_loc_cel_phi', '<f8'),
                 ('best_loc_cel_theta', '<f8'),
                 ('best_spec', '<i'),
                 ('ampli', '<f8'),
                 ('snr', '<f8'),
                 ('snr0', '<f8'),
                 ('snr1', '<f8'),
                 ('chisq', '<f8'),
                 ('chisq+', '<f8'),
                 ('sun_angle', '<f8'),
                 ('earth_angle', '<f8'),
                 ('llr', '<f8'),
                 ('coinc_llr', '<f8'),
                 ('cr_var0', '<f8'),
                 ('cr_var1', '<f8'),
                 ('cr_var2', '<f8')]
        ds = of.create_dataset('gbmscan', (odata_filtered.shape[0],), dtype)
        ds['met'] = odata_filtered[:,0]
        ds['duration'] = odata_filtered[:,1]
        ds['in_gti'] = odata_filtered[:,2]
        ds['rock'] = odata_filtered[:,3]
        ds['good'] = odata_filtered[:,4]
        ds['best_loc_sc_phi'] = odata_filtered[:,5]
        ds['best_loc_sc_theta'] = odata_filtered[:,6]
        ds['best_loc_cel_phi'] = odata_filtered[:,7]
        ds['best_loc_cel_theta'] = odata_filtered[:,8]
        ds['best_spec'] = odata_filtered[:,9]
        ds['ampli'] = odata_filtered[:,10]
        ds['snr'] = odata_filtered[:,11]
        ds['snr0'] = odata_filtered[:,12]
        ds['snr1'] = odata_filtered[:,13]
        ds['chisq'] = odata_filtered[:,14]
        ds['chisq+'] = odata_filtered[:,15]
        ds['sun_angle'] = odata_filtered[:,16]
        ds['earth_angle'] = odata_filtered[:,17]
        ds['llr'] = odata_filtered[:,18]
        ds['coinc_llr'] = odata_filtered[:,19]
        ds['cr_var0'] = odata_filtered[:,20]
        ds['cr_var1'] = odata_filtered[:,21]
        ds['cr_var2'] = odata_filtered[:,22]


