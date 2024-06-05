'''
Subsets the hydrofabric by gage ID and creates BMI config files with initial parameters.

Inputs:
    gage_id:  a gage_id, e.g., "01123000"
           Look at using the Python rpy2 package for R function calls as the Rscript method is not the best. 

    output_dir: Absolute path to directory where input and output data will be stored.  The directory structure will
    change as the Hydrofabric and NGEN design is refined.  

Outputs:
    Outputs are written to output_dir:  Hydrofabric Subset files (Gage-xxxxxxxx.gpkg) and BMI config files (e.g., cat-10617_bmi_config.ini)
    in CFE-S subdirectory.   
'''

import subprocess
import re
import sys

def hf_subsetter(gage_id, output_dir):

        #validate that gage input is in the proper format (8 digits) 
        x = bool(re.search("\d{8}", gage_id))
        if not x:
            print("Gage ID is not valid")
            sys.exit()

	#call subsetter R function

        print("Create subsetted geopackage files")
        #append "Gages-" to id per hydrofabric naming convention for hl_uri
        subsetter_gage_id = "Gages-"+gage_id
        #strip leading zero of gage ID for gpkg filename
        subsetter_gage_id_filename = "Gage_"+gage_id.lstrip("0")
        gpkg_path_filename = output_dir + subsetter_gage_id_filename + ".gpkg"

        print("Subsetting:  " + subsetter_gage_id)

        run_command = "/usr/bin/Rscript /home/hydrofabric/R/run_subsetter.r" + " " + subsetter_gage_id + " " + gpkg_path_filename
        subprocess.call(run_command, shell=True)


        #Run BMI config file R script
        print("Create BMI config files with initial parameter estimates")
        #create string containing R c (combine) function and gage IDs
        #gage_id_string = ','.join(gage_id)
        gage_id_string = 'c(' + gage_id + ')'
        gage_id_string = "'"+gage_id_string+"'"

        print(gage_id_string)        
        run_command = ["/usr/bin/Rscript /home/hydrofabric/R/run_create_cfe_init_bmi_config.R", gage_id_string, output_dir]
        run_command_string = " ".join(run_command)

        subprocess.call(run_command_string, shell=True) 


#Call function for test.  

gage =  "01123000"

hf_subsetter(gage, "/Hydrofabric/data/output/")
