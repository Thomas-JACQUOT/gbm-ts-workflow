import numpy as np
from argparse import ArgumentParser
from configparser import ConfigParser
import datetime
import os
import utils
import stat
from gdt.missions.fermi.time import Time

def main():
        parser = ArgumentParser(prog='GBM Targeted Search', \
                                description='The GBM coherent targeted search')
        parser.add_argument('-c', '--config-file', default='config.ini',help='The configuration file')
        parser.add_argument('-n', '--number-of-searches', default=60, 
                            help='Number of times in which the targeted search will be ran')
        args = parser.parse_args()

        # get configuration
        config = ConfigParser()
        config.read(args.config_file)
        num_searches = args.number_of_searches

        #download the data
        time_range = config['time_range']
        start_time = time_range['start_time']
        end_time = time_range['end_time']
        


        time_list = np.arange(np.datetime64(start_time), np.datetime64(end_time), num_searches)
        time_list = np.append(time_list, np.datetime64(end_time))
        datetime_array = [datetime.datetime.fromisoformat(f'{time}') for time in time_list]
        time_array = Time(datetime_array, format="datetime")
        fermi_time_array = time_array.fermi

        results = config['results']
        output_path = results["output_path"]
        error_submit_download = os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0])
        if (os.path.isdir(error_submit_download) == False):
                os.mkdir(error_submit_download)
        
        error_submit_download = os.path.join(error_submit_download,  "condor_download_errors")
        if (os.path.isdir(error_submit_download) == False):
                os.mkdir(error_submit_download)


        submit_download = open("submit_poshist.sub", "w")
        submit_download.write("Executable = download_poshist.py\n")
        submit_download.write("Universe   = vanilla\n")
        submit_download.write("Arguments  = -d $(line) -p " + output_path + "\n")
        submit_download.write("input      = /dev/null\n")
        submit_download.write("Log        =" +  error_submit_download  + "/download_condor_poshist.log\n")
        submit_download.write("error      =" + error_submit_download + "/download_condor_poshist_$(line).err\n")
        submit_download.write("output     = /dev/null\n")
        submit_download.write("notification = never\n")
        submit_download.write("getEnv     = True\n")
        submit_download.write("accounting_group = ligo.prod.o4.cbc.grb.gbm_subthreshold\n")
        submit_download.write("Queue ")
        submit_download.close()

        submit_download = open("submit_tte.sub", "w")
        submit_download.write("Executable = download_tte.py\n")
        submit_download.write("Universe   = vanilla\n")
        submit_download.write("Arguments  = -d $(line) -p " + output_path + "\n") 
        submit_download.write("input      = /dev/null\n")
        submit_download.write("Log        =" +  error_submit_download  + "/download_condor_tte.log\n")
        submit_download.write("error      =" + error_submit_download + "/download_condor_tte_$(line).err\n")
        submit_download.write("output     = /dev/null\n")
        submit_download.write("notification = never\n")
        submit_download.write("getEnv     = True\n")
        submit_download.write("accounting_group = ligo.prod.o4.cbc.grb.gbm_subthreshold\n")
        submit_download.write("Queue ")
        submit_download.close()




        #run the targeted search	
        error_submit_targeted = os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0])
        error_submit_targeted = os.path.join(error_submit_targeted, "condor_targeted_errors")
        os.makedirs(error_submit_targeted, exist_ok=True)
        targeted_output_path = os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0], "output")

        #create a bash script which impose to the targeted-search jobs to exit with 0
        targeted_executable =  config['targeted_search']['targeted_executable']

        secours_targeted = open("script_secours_targeted.sh", "w")
        secours_targeted.write("#!/bin/bash\n")
        secours_targeted.write(targeted_executable + " -t $@" + " -o " + targeted_output_path + "\n")
        secours_targeted.write("exit 0")
        secours_targeted.close()
        st = os.stat('script_secours_targeted.sh')
        os.chmod('script_secours_targeted.sh', st.st_mode | stat.S_IEXEC)



        submit_targeted = open("submit_targeted.sub", "w")
        submit_targeted.write("Executable = script_secours_targeted.sh\n")
        submit_targeted.write("Universe   = vanilla\n")
        submit_targeted.write("Arguments  = $(line)\n")
        submit_targeted.write("input      = /dev/null\n")
        submit_targeted.write("Log        =" +  error_submit_targeted  + "/targeted_condor.log\n")
        submit_targeted.write("error      =" + error_submit_targeted + "/targeted_condor_$(line).err\n")
        submit_targeted.write("output     = /dev/null\n")
        submit_targeted.write("notification = never\n")
        submit_targeted.write("getEnv     = True\n")
        submit_targeted.write("request_memory = 4096\n")
        submit_targeted.write("accounting_group = ligo.prod.o4.cbc.grb.gbm_subthreshold\n")
        submit_targeted.write(" \n")
        submit_targeted.write("Queue ")
        submit_targeted.close()

        #merge the results
        error_submit_merge = os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0])
        error_submit_merge = os.path.join(error_submit_merge, "condor_merged_errors")
        if (os.path.isdir(error_submit_merge) == False):
                os.mkdir(error_submit_merge)

        submit_merge = open("submit_merge.sub", "w")
        submit_merge.write("Executable = script_merge.py\n")
        submit_merge.write("Universe   = vanilla\n")
        submit_merge.write("Arguments  = -i " + os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0], "output/results") +  " -o "  + os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0], "output/compressed_data") + "\n")
        submit_merge.write("input      = /dev/null\n")
        submit_merge.write("Log        =" +  error_submit_merge  + "/merge_merge_condor.log\n")
        submit_merge.write("error      =" + error_submit_merge + "/merge_merge_condor.err\n")
        submit_merge.write("output     = /dev/null\n")
        submit_merge.write("notification = never\n")
        submit_merge.write("getEnv     = True\n")
        submit_merge.write("request_memory = 65536\n")
        submit_merge.write("accounting_group = ligo.prod.o4.cbc.grb.gbm_subthreshold\n")
        submit_merge.write(" \n")
        submit_merge.write("Queue ")
        submit_merge.close()

        #copy the localization files
        error_submit_transfer = os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0])
        error_submit_transfer = os.path.join(error_submit_transfer, "condor_transfer_skymaps_errors")
        if (os.path.isdir(error_submit_transfer) == False):
                os.mkdir(error_submit_transfer)

        submit_transfer = open("submit_transfer_skymaps.sub", "w")
        submit_transfer.write("Executable = script_transfer_skymaps.py\n")
        submit_transfer.write("Universe   = vanilla\n")
        submit_transfer.write("Arguments  = -i " + os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0], "output/results") +  " -o "  + os.path.join(output_path, start_time.split("T")[0] + "_" + end_time.split("T")[0], "output/compressed_data/localization_files") + "\n")
        submit_transfer.write("input      = /dev/null\n")
        submit_transfer.write("Log        =" +  error_submit_transfer  + "/transfer_skymaps_condor.log\n")
        submit_transfer.write("error      =" + error_submit_transfer + "/transfer_skymaps_condor.err\n")
        submit_transfer.write("output     = /dev/null\n")
        submit_transfer.write("notification = never\n")
        submit_transfer.write("getEnv     = True\n")
        submit_transfer.write("accounting_group = ligo.prod.o4.cbc.grb.gbm_subthreshold\n")
        submit_transfer.write(" \n")
        submit_transfer.write("Queue ")
        submit_transfer.close()



        #create the dag file
        submit_dag_file = open("submit_dag_file.dag", "w")
        for i in range(len(time_array)):
                submit_dag_file.write("JOB A"+ str(i) + " submit_poshist.sub\n")
                submit_dag_file.write("VARS A" + str(i) + ''' line="''' + str(time_array[i]) + '''"\n''')
                submit_dag_file.write("JOB B"+ str(i) + " submit_tte.sub\n")
                submit_dag_file.write("VARS B" + str(i) + ''' line="''' + str(time_array[i]) + '''"\n''')
        submit_dag_file.write("JOB C submit_merge.sub\n")
        submit_dag_file.write("JOB D submit_transfer_skymaps.sub\n")
        for i in range(len(fermi_time_array)):
                submit_dag_file.write("JOB TS"+ str(i) + " submit_targeted.sub\n")
                submit_dag_file.write("VARS TS" + str(i) + ''' line="''' + str(fermi_time_array[i]) + '''"\n''')
        for i in range(len(time_array) -1):
                submit_dag_file.write("PARENT  A"+ str(i) + " CHILD A" + str(i+1) + "\n")
        for i in range(len(fermi_time_array)):
                submit_dag_file.write("PARENT")
                for j in range(len(time_array)):
                        submit_dag_file.write(" A"+ str(j) + " B"+ str(j))	
                submit_dag_file.write(" CHILD TS"+str(i) + "\n")
        for i in range(len(fermi_time_array)):
                submit_dag_file.write("PARENT TS"+str(i) + " CHILD C\n")
        submit_dag_file.write("PARENT C CHILD D\n")
        for i in range(len(fermi_time_array)):
                submit_dag_file.write("RETRY TS"+str(i) + " 3\n")


        submit_dag_file.close()
        condor_command = "condor_submit_dag submit_dag_file.dag"
        #os.system(condor_command)

if __name__ == "__main__":
        main()