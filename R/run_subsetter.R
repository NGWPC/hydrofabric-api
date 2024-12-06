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
domain <- args[7]
hydrofabric_filename <- args[8]

outpathfile <- paste(outpath, outfile, sep = "/")

layers22 <- c('flowpaths', 'divides', 'lakes', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes',
           'flowpath-attributes-ml', 'network', 'divide-attributes')

layers22_oCONUS <- c('flowpaths', 'divides', 'lakes', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes',
           'network', 'divide-attributes')

layers21 <- c('divides', 'flowlines',
            'model-attributes', 'network', 'nexus')

if (hydrofabric_version == '2.1.1'){
  gauge_id <- paste('Gages',gauge_id, sep = '-')
  get_subset(hl_uri = gauge_id, lyrs = layers21, source = hydrofabric_data,
  hf_version = hydrofabric_version,
  type = hydrofabric_type, outfile = outpathfile, overwrite = TRUE)
}   else if(hydrofabric_version == '2.2'){
    hydrofabric_version <- paste('v',hydrofabric_version,sep='')
    hf_gpkg_path = paste(hydrofabric_data,hydrofabric_version,hydrofabric_type,domain,hydrofabric_filename, sep='/')
    
    if (domain == 'CONUS'){
      gauge_id <- paste('gages',gauge_id, sep = '-')
      get_subset(hl_uri = gauge_id, gpkg = hf_gpkg_path, lyrs = layers22,
      outfile = outpathfile, overwrite = TRUE)
    } else {
      print('ALASKA!!')
      poi <- as_ogr(hf_gpkg_path, 'hydrolocations') |>
      dplyr::filter(hl_reference == 'Gages', hl_link == !!gauge_id) |>
      dplyr::collect()
      get_subset(poi_id = poi$poi_id, gpkg = hf_gpkg_path, lyrs = layers22_oCONUS,
      outfile = outpathfile, overwrite = TRUE)
    }
}