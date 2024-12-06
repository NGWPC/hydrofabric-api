# This script is called in get_geopackage via Rscript using a
#python subprocess run command.  Arguments are listed in the Rscript command
# and placed in to individual variables to be passed to get_subset.
#Any exceptions from get_subset are captured and sent back to python in stderr.
args <- commandArgs(trailingOnly = TRUE)

suppressMessages(library(hydrofabric))

gauge_id <- args[1]
outpath <- args[2]
outfile <- args[3]
hydrofabric_data <- args[4]
hydrofabric_version <- args[5]
hydrofabric_type <- args[6]

outpathfile <- paste(outpath, outfile, sep = "/")
domain <- 'conus'
hf_gpkg <- 'conus_nextgen.gpkg'
layers22 <- c('flowpaths', 'divides', 'lakes', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes',
           'flowpath-attributes-ml', 'network', 'divide-attributes')

layers21 <- c('divides', 'flowlines',
            'model-attributes', 'network', 'nexus')

if (hydrofabric_version == '2.1.1'){
  get_subset(hl_uri = gauge_id, lyrs = layers21, source = hydrofabric_data,
  hf_version = hydrofabric_version,
  type = hydrofabric_type, outfile = outpathfile, overwrite = TRUE)
}   else if(hydrofabric_version == '2.2'){
    hydrofabric_version <- paste('v',hydrofabric_version,sep='')
    hf_gpkg_path = paste(hydrofabric_data,hydrofabric_version,hydrofabric_type,domain,hf_gpkg, sep='/')
    
    
    get_subset(hl_uri = gauge_id, gpkg = hf_gpkg_path, lyrs = layers22,
    outfile = outpathfile, overwrite = TRUE)
}