#An interface between the Rscript command line arguments and the R functions
args = commandArgs(trailingOnly = TRUE)

source('../create_cfe_init_bmi_config.R')
source('../rasterize_basins.R')
source('../create_crosswalk_gwbucket_catchment.R')

gage_ids <- eval(parse(text=args[1]))
data_dir <- args[2]
output_dir <- args[3]
const_names <- eval(parse(text=args[4]))
const_values <- eval(parse(text=args[5]))
nwm_names <- eval(parse(text=args[6]))
cfe_names <- eval(parse(text=args[7]))
model_name <- args[8]

print("Running rasterize_basins")
rasterize_basins(gage_ids, data_dir, output_dir)
print("Running create_crosswalk_gwbucket_catchment")
create_crosswalk_gwbucket_catchment(data_dir)
print("Running create_cfe_init_bmi_config")
create_cfe_init_bmi_config(gage_ids, data_dir, output_dir, const_names, const_values, nwm_names, cfe_names, model_name)
