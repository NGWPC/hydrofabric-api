#An interface between the Rscript command line arguments and the R functions
args <- commandArgs(trailingOnly = TRUE)

gage_id <- args[1]
data_dir <- args[2]
output_dir <- args[3]
const_names <- eval(parse(text=args[4]))
const_values <- eval(parse(text=args[5]))
nwm_names <- eval(parse(text=args[6]))
cfe_names <- eval(parse(text=args[7]))
model_name <- args[8]
base_dir <- args[9]
gpkg_file <- args[10]

cfe_init_bmi <- paste0(base_dir,'/R/create_cfe_init_bmi_config.R')
basins <- paste0(base_dir,'/R/rasterize_basins.R')
crosswalk <- paste0(base_dir,'/R/create_crosswalk_gwbucket_catchment.R')
source(cfe_init_bmi)
source(basins)
source(crosswalk)

print("Running rasterize_basins")
rasterize_basins(gage_id, data_dir, gpkg_file)
print("Running create_crosswalk_gwbucket_catchment")
create_crosswalk_gwbucket_catchment(data_dir)
print("Running create_cfe_init_bmi_config")
create_cfe_init_bmi_config(gage_id, data_dir, output_dir, const_names, const_values, nwm_names, cfe_names, gpkg_file)
