# This script is called in get_geopackage via Rscript using a python subprocess run command.  Arguments are listed in the Rscript command
# and placed in to individual variables to be passed to get_subset.  Any exceptions from get_subset are captured and sent back to python 
# in stderr.
args = commandArgs(trailingOnly = TRUE)

suppressMessages(library(hydrofabric))

gauge_id = args[1]
outpath = args[2]
outfile = args[3]
hydrofabric_data = args[4]
hydrofabric_version = args[5]
hydrofabric_type = args[6]

outpathfile = paste(outpath, outfile, sep="/")
domain = 'conus'
hf_gpkg = 'conus_nextgen.gpkg'

hf_gpkg_path = paste(hydrofabric_data,hydrofabric_version,hydrofabric_type,domain,hf_gpkg, sep='/')

tryCatch( {
if(hydrofabric_version == '2.1.1'){     
    get_subset(hl_uri = gauge_id, source = hydrofabric_data, hf_version = hydrofabric_version,
    type = hydrofabric_type, outfile = outpathfile, overwrite = TRUE)
} else if(hydrofabric_version == '2.2'){
    get_subset(hl_uri = gauge_id, gpkg=,outfile = outpathfile, overwrite = TRUE )
}
},
error = function(cond) {
    message(paste('error', conditionMessage(cond), sep=':'))
},
warning = function(cond) {
},
finally = {
}
)
