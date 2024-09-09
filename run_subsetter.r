args = commandArgs(trailingOnly = TRUE)

library(hydrofabric)

gauge_id = args[1]
outpath = args[2]
outfile = args[3]
hydrofabric_data = args[4]
hydrofabric_version = args[5]
hydrofabric_type = args[6]

outpathfile = paste(outpath, outfile, sep="/")

#get_subset(hl_uri = gauge_id, source = hydrofabric_data, outfile = outpathfile)
get_subset(hl_uri = gauge_id, source = hydrofabric_data, hf_version = hydrofabric_version, type = hydrofabric_type, outfile = outpathfile)
