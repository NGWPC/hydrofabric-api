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
source <- args[9]

outpathfile <- paste(outpath, outfile, sep = "/")

layers22 <- c('flowpaths', 'divides', 'lakes', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes',
           'flowpath-attributes-ml', 'network', 'divide-attributes')

#Removed lake layer because subsetter fails because hl_uri is missing
layers22_oCONUS <- c('flowpaths', 'divides', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes', 'divide-attributes',
           'network')

layers22_GL <- c('flowpaths', 'divides', 'nexus', 'pois',
           'hydrolocations', 'flowpath-attributes',
           'network')

layers21 <- c('divides', 'flowlines',
            'model-attributes', 'network', 'nexus')

if (hydrofabric_version == '2.1.1'){
  gauge_id <- paste('Gages',gauge_id, sep = '-')
  suppressWarnings(get_subset(hl_uri = gauge_id, lyrs = layers21, source = hydrofabric_data,
  hf_version = hydrofabric_version,
  type = hydrofabric_type, outfile = outpathfile, overwrite = TRUE))
}   else if(hydrofabric_version == '2.2'){
    hydrofabric_version <- paste('v',hydrofabric_version,sep='')
    hf_gpkg_path = paste(hydrofabric_data,hydrofabric_version,hydrofabric_type,domain,hydrofabric_filename, sep='/')
    gages_csv = paste(hydrofabric_data,hydrofabric_version,hydrofabric_type,'gages_xy.csv',sep='/')

    #Difference in capitalization between CONUS and oCONUS for hl_reference value.
    #Also, set layers for CONUS and oCONUS
    if (domain == 'CONUS'){
      gages <- 'gages'
      lyrs <- layers22
    } else {
      gages <- 'Gages'
      lyrs <- layers22_oCONUS
    }

    #Select layers for Great Lakes dataset
    if(source == 'ENVCA'){
      lyrs <- layers22_GL
    }  

    #Subset using the POI.  First check if gage exists as a hydrolocation.  Otherwise,
    #find gage lat/lon in csv file and subset.  All Alaska gages must use lat/lon because
    #hydrolocations are incorrect
    poi <- as_ogr(hf_gpkg_path, 'hydrolocations') |>
    dplyr::filter(hl_reference == gages, hl_link == !!gauge_id) |>
    dplyr::collect()

    if(nrow(poi) > 0 & domain != 'Alaska') {
      suppressWarnings(get_subset(poi_id = poi$poi_id, gpkg=hf_gpkg_path, lyrs=lyrs,
      outfile=outpathfile, overwrite=TRUE))
    } else {
         gages_xy <- read.csv(gages_csv)
         gage <- dplyr::filter(gages_xy, gageid == gauge_id)
          if(nrow(gage) > 0){
            lon <- gage$lon
            lat <- gage$lat
            xy <- c(lon,lat)
            suppressWarnings(get_subset(xy=xy, gpkg=hf_gpkg_path, lyrs=lyrs,
            outfile=outpathfile, overwrite=TRUE))
      } else {
        cat('Gage not found as hydrolocation or in gage lat/lon file', file = stderr())
      }
    }   
}