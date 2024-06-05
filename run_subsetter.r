args = commandArgs(trailingOnly = TRUE)

library(hydrofabric)

gauge_id = args[1]
outfile = args[2]

get_subset(hl_uri = gauge_id, source = "/Hydrofabric/data/hydrofabric", outfile = outfile)
