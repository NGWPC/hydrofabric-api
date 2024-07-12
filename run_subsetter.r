args = commandArgs(trailingOnly = TRUE)

library(hydrofabric)

gauge_id = args[1]
outfile = args[2]
hydrofabric_data = args[3]

get_subset(hl_uri = gauge_id, source = hydrofabric_data, outfile = outfile)
