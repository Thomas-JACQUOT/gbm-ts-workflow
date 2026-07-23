import numpy as np
from argparse import ArgumentParser
from configparser import ConfigParser
import matplotlib.pyplot as plt
import scipy.stats as stats
import h5py
import glob
import datetime
import os
import subprocess

from gdt.core.tte import PhotonList, EventList
from gdt.core.binning.unbinned import bin_by_time
from gdt.core.plot.lightcurve import Lightcurve
from gdt.missions.fermi.gbm.response import GbmRsp
from gdt.core.spectra.functions import Band, BrokenPowerLaw
from gdt.core.simulate.profiles import norris, tophat
from gdt.core.simulate.tte import TteSourceSimulator
from gdt.core.data_primitives import ResponseMatrix, Gti
from gdt.core.plot.drm import ResponsePlot


class InjectionSampler:
    def __init__(self, ini_file):
        self.cfg = ConfigParser()
        self.cfg.read(ini_file)

        self.priors = {}
        self.constraints = []

        self._parse_priors()
        self._parse_constraints()

    def _parse_priors(self):
        for section in self.cfg.sections():
            if not section.startswith("prior-"):
                continue
            param = section.replace("prior-", "")
            prior_type = self.cfg[section]["name"]
            self.priors[param] = {
                "type": prior_type,
                "config": dict(self.cfg[section]),
            }

    def _parse_constraints(self):
        for section in self.cfg.sections():
            if not section.startswith("constraint-"):
                continue
            expr = self.cfg[section]["constraint_arg"]
            self.constraints.append(expr)

    def sample_prior(self, param, size=10000):
        prior = self.priors[param]
        cfg = prior["config"]
        ptype = prior["type"]

        if ptype == "uniform":
            if "angle" in param:
                low = np.deg2rad(float(cfg["min"]))
                high = np.deg2rad(float(cfg["max"]))
            else:
                low = float(cfg["min"])
                high = float(cfg["max"])
            return np.random.uniform(low, high, size)

        if ptype == "uniform-log":
            low = float(cfg["min"])
            high = float(cfg["max"])
            return np.exp(np.random.uniform(np.log(low), np.log(high), size))

        elif ptype == "gaussian":
            mean = float(cfg["mean"])
            sigma = np.sqrt(float(cfg["var"]))
            
            if 'min' in cfg.keys():
                x = stats.truncnorm((float(cfg['min'])-mean)/sigma, (float(cfg['max'])-mean)/sigma, loc=mean, scale=sigma)
                return x.rvs(size=size)
            else:
                x = np.random.normal(mean, sigma, size)
                return x

        elif ptype == "log-normal":
            mu = float(cfg["mu"])
            sigma = float(cfg["sigma"])
            return np.random.lognormal(mu, sigma, size)
        
        elif ptype == "sin_angle":
            u = np.random.uniform(0, 1, size)
            return np.arccos(1 - u)
        
        else:
            raise ValueError(f"Unsupported prior type: {ptype}")
        
    def sample_luminosity(self, amp, inclination, opening_angle, mass1, mass2):
        l_theta = np.exp((-1/2) * (inclination/opening_angle)**2) ##From Salafia et al. 2023 https://doi.org/10.1051/0004-6361/202347298
        l_mass = np.exp((-1/2) * (mass1/mass2)**2)
        return amp * (l_theta + l_mass)

def main():
    parser = ArgumentParser(prog='GBM Targeted Search', \
                            description='The GBM coherent targeted search')
    parser.add_argument('-c', '--config-files', nargs='+', type=str, required=True, help='The configuration file')
    parser.add_argument('-d', '--input-sample-directory', type=str, required=True, help='Path to directory where GW samples are')
    #ipythparser.add_argument('-t', '--input-table', type=str, help='Input parameters table (RnP injections)')

    args = parser.parse_args()
    ini_files = args.config_files
    gw_sample_path = (glob.glob(f"{args.input_sample_directory}/samples*"))
    gw_samples = {
        'mass1_det': np.array([]),
        'mass2_det': np.array([]),
        'inclination': np.array([]),
        'ra': np.array([]),
        'dec': np.array([])
    }
    for gw_sample in gw_sample_path:
        print(gw_sample)
        with h5py.File(f"{gw_sample}", 'r') as hf:
            gw_samples['mass1_det'] = np.append(gw_samples['mass1_det'],hf['cbc_waveform_params/mass1_det'][:])
            gw_samples['mass2_det'] = np.append(gw_samples['mass2_det'],hf['cbc_waveform_params/mass2_det'][:])
            gw_samples['inclination'] = np.append(gw_samples['inclination'],hf['cbc_waveform_params/inclination'][:])
            gw_samples['ra'] = np.append(gw_samples['ra'],hf['cbc_waveform_params/ra'][:])
            gw_samples['dec'] = np.append(gw_samples['dec'],hf['cbc_waveform_params/dec'][:])
    with open("BrowseTargets-4023665-1784559849.txt", 'r') as f:
        lines = f.readlines()[3:]
        alpha_list, beta_list = [], []
        for line in lines:
            alpha, beta = (line.split("|")[:][1]), (line.split("|")[:][2])
            if "e" in alpha:
                alpha_list.append(float(alpha))
                beta_list.append(float(beta))
            else:
                pass
    plt.hist(alpha_list, density=True, bins=100, label="alpha query distribution")
    #plt.xlim(-3,1)
    plt.title('Alpha dists without x lim')
    plt.legend()
    plt.savefig("Alpha_distribution_no_x_lim.png")
    plt.close()
    plt.hist(beta_list, density=True, bins=100, label="beta query distribution")
    #plt.xlim(-3,1)
    plt.title('beta dists without x lim')
    plt.legend()
    plt.savefig("Beta.png")
    plt.close()
    samplers = {}
    for ini in ini_files:
         samplers[ini[0:4]] = InjectionSampler(ini)
    samples = {}
    for sampler in samplers:
            for key in samplers[sampler].priors.keys():
                samples[key] = samplers[sampler].sample_prior(f'{key}', size=len(gw_samples['mass1_det']/1000)) 
                plt.hist(samples[key], density=True, bins=1000, label=f"{key} distribution")
                plt.title(f"{sampler} {samplers[sampler].priors[key]}")
                plt.legend()
                plt.savefig(f"{key}_distribution.png")
                plt.close()
    
    samples['luminosity'] = samplers['injs'].sample_luminosity(samples['l0'], gw_samples['inclination'][:len(gw_samples['mass1_det']/1000)], samples['opening_angle'], gw_samples['mass1_det'][:len(gw_samples['mass1_det']/1000)], gw_samples['mass2_det'][:len(gw_samples['mass1_det']/1000)])
    plt.hist(samples["luminosity"], density=True, bins=1000, label="Luminosity distribution")
    plt.xscale("log")
    plt.legend()
    plt.savefig("luminosity_distribution")
    plt.close()
    plt.scatter(samples['luminosity'], samples['epeak'])
    plt.title("Luminosity vs epeak")
    plt.savefig('Luminosity_vs_epeak.png')
    plt.close()
    plt.hist(gw_samples['mass2_det'][:]/gw_samples['mass1_det'][:], density=True, bins=100)
    plt.title("m2/m1")
    plt.savefig("m2_m1.png")
    plt.close()
    plt.hist(gw_samples['mass1_det'][:], density=True, bins=100)
    plt.title("m1")
    plt.savefig("m1.png")
    plt.close()
    plt.hist(gw_samples['mass1_det'][:], density=True, bins=100)
    plt.title("m2")
    plt.savefig("m2.png")
    plt.close()
    plt.hist(gw_samples['inclination'][:], density=True, bins=1000)
    plt.xscale("log")
    plt.title("iota")
    plt.savefig("iota.png")
    plt.close()
    plt.hist(gw_samples['inclination'][:]/samples['opening_angle'], density=True, bins=1000)
    plt.xscale("log")
    plt.title("iota/theta_v")
    plt.savefig("iota_theta_v.png")
    plt.close()
    plt.hist(samples['l0'], density=True, bins=1000, label="L0 distribution")
    plt.xscale('log')
    plt.title('L0_dist')
    plt.legend()
    plt.savefig("L0_distribution.png")
    plt.close()
    plt.hist(samples['alpha'], density=True, bins=1000, label="alpha git distribution")
    plt.hist(alpha_list, density=True, bins=1000, label="alpha paper distribution")
    #plt.xlim(-3,1)
    plt.title('Alpha dists without x lim')
    plt.legend()
    plt.savefig("Alpha_distributions_no_x_lim.png")
    plt.close()
    plt.hist(samples['beta'], density=True, bins=1000, label="beta git distribution")
    plt.hist(beta_list, density=True, bins=1000, label="beta paper distribution")
    #plt.xlim(-5, -1)
    plt.title('Beta dists without x lim ')
    plt.legend()
    plt.savefig("Beta_distributions_no_x_lim.png")
    plt.close()
    with h5py.File("EM_samples.hdf",'w') as emf:
        for key in samples.keys():
            print(key)
            emf.create_dataset(key, data=samples[f'{key}'])
        emf.create_dataset("ra", data=gw_samples['ra'])
        emf.create_dataset("dec", data=gw_samples['dec'])
                #hf.create_dataset(key, data=samples[key])

    
    
    static_parameters = samplers['injs'].cfg['static_params']
    #pulse_function, spectral_model, alpha, beta = static_parameters['pulse_function'], static_parameters['spectral_model'], static_parameters['alpha'], static_parameters['beta']
    #start = samples['t_start']
    #os.environ["PATH"] += ":/home/shared/gbm-response-generator/bin/"
#
    #subprocess.run([
    #    "SA_GBM_RSP_Gen.pl",
    #    f"-R{args.inj_ra}",
    #    f"-D{args.inj_dec}",
    #    f"-S{GRB_FERMI_TIME}",
    #    "-V0",
    #    "-Ccspec",
    #    f"{args.output}"
    #], check=True)
    #stop = start + 20
    #det_list = ["n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9", "na", "nb"]
    #for i in range(len(det_list)):
    #    rsp_wildcard = f"Test_Script_Injection/{start}/glg_cspec_*_v00.rsp"
    #    rsp_files = sorted(glob.glob(rsp_wildcard))
    #    rsp = GbmRsp.open(rsp_files[i+2])
    #    print(rsp)
    #    drmplot = ResponsePlot(rsp.drm, colorbar=False)
    #    drmplot.xlim = (8.0, 1000.0)
    #    drmplot.ylim = (8.0, 1000.0)
    #    plt.savefig(f"{args.output}/rspplot_{i}.png")
    #    #breakpoint()
    #    # (amplitude, Epeak, alpha, beta)
    #    #band_params = (0.001, 300.0, -1.0, -2.8)
    #    spectral_params = (0.1, samples['epeak'], alpha, beta)
    #    # (amplitude, tstart, trise, tdecay)
    #    norris_params = (float(args.inj_amp), start, 0.1, 0.5)
    #    # (amplitude, tstart, tstop)
    #    #tophat_params = (float(args.inj_amp),start, start+1)
    #    src_sim = TteSourceSimulator(rsp, eval(spectral_model), spectral_params, eval(pulse_function), norris_params,deadtime=1e-6)
    #    sim_check = src_sim.simulate(start, stop)
    #    if sim_check.time_range is None:
    #        with h5py.File(f"{args.output}/TTE_INJECTION_{i}.hdf5", 'w') as hf:
    #            hf.create_dataset("times", data=None)
    #            hf.create_dataset("channels", data=None)
    #            hf.create_dataset("ebounds", data=None)
    #            hf.create_dataset("ra", args.inj_ra)
    #            hf.create_dataset("dec", args.inj_dec)
    #    else:
    #        gti = Gti.from_bounds([sim_check.time_range[0]], 
    #                              [sim_check.time_range[1]])
    #        src_tte = PhotonList.from_data(sim_check,gti=gti)
    #        with h5py.File(f"{args.output}/TTE_INJECTION_{i}.hdf5", 'w') as hf:
    #            hf.create_dataset("times", data=src_tte.data.times)
    #            hf.create_dataset("channels", data=src_tte.data.channels)
    #            hf.create_dataset("ebounds", data=src_tte.data.ebounds_intervals)
    #            hf.create_dataset("ra", args.inj_ra)
    #            hf.create_dataset("dec", args.inj_dec)
    ##    phaii = src_tte.to_phaii(bin_by_time, 0.064)
    #    lcplot = Lightcurve(data=phaii.to_lightcurve())
    #    plt.savefig(f"test{i}.png")
    #plt.close()


if __name__ == "__main__":
        main()

