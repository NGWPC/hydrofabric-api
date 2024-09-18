create_cfe_init_bmi_config = function(basins, data_dir, output_dir, const_name, const_value, nwm_names, cfe_names, module_name){

# Derive initial parameters for CFE based on NWM v3 parameter files and 
# create the BMI config files for each catchment in the selected basins

#rm(list=ls())

library(terra)
library(zonal)
library(data.table)
library(sf)
library(ncdf4)
library(raster)
library(rwrfhydro)

# define basin or basins to extract aorc data for
#group <- 1; basins <- c("01123000","01350080","14141500","14187000")

group <- 1

# define start and end dates 
start_date <- "20170101"
end_date <- "20191231"
n_timesteps <- length(seq(as.POSIXct(start_date,format="%Y%m%d"),as.POSIXct(end_date,format="%Y%m%d")+23*3600,by="hour"))

# define surface partition scheme (infiltration scheme)

if (model_name == "CFE-S")
{
    scheme <- "Schaake"
} else if (model_name == "CFE-X")
{
    scheme <- "Xinanjiang"
}

# hydrofabric file for the basins (all catchments put together)
sf1 <- data.frame()
for (gage1 in basins) {
    str_gage1 <- ifelse(substr(gage1,1,1)=="0",substr(gage1,2,nchar(gage1)),gage1)
    hydro_file <- paste0(output_dir,"Gage_",str_gage1,".gpkg")
    sf0 <- read_sf(hydro_file, "divides")
    sf0$gage <- gage1
    sf1 <- rbind(sf1,sf0)
}

# mask file (reference raster for NWM domain crs/extent/res)
mask_file <- paste0(data_dir, "final_combined_calib_v3.tif")
r0 <- raster(mask_file)

message("processing soil parameters ...")

# NWM and cfe soil parameter name mapping
#pars_nwm <- c("bexp","dksat","psisat","slope","smcmax","smcwlt","AXAJ","BXAJ","XXAJ")
#pars_cfe <- c(paste0("soil_params.",c("b", "satdk","satpsi","slop","smcmax","wltsmc")),
#                "a_Xinanjiang_inflection_point_parameter",
#                "b_Xinanjiang_shape_parameter_set",
#                "x_Xinanjiang_shape_parameter")

if (model_name == "CFE-S")
{
    pars_nwm <- nwm_names[1:6]
    pars_cfe <- cfe_names[1:6]

} else if (model_name == "CFE-X")
{
    pars_nwm <- nwm_names[1:9]
    pars_cfe <- cfe_names[1:9]
}

# NWM v3 soil parameters
soil_file <- paste0(data_dir, "soilproperties_CONUS_FullRouting.nc")
nc <- nc_open(soil_file)
vars <- names(nc$var)
pars0 <- pars_nwm[!pars_nwm %in% vars]
if (length(pars0)>0) message(paste0("WARNING: the following parameters are not found in NWM soil properties file: ",
    paste(pars0,collapse=", ")))

# loop through soil parameters to extract values for each catchment from NWM parameter grids
dtSoilPars <- data.table()
for (p1 in pars_nwm) {
    message(p1)
    v1 <- ncvar_get(nc, p1)
    if (length(dim(v1))>2) v1 <- v1[,,1] # just take the first soil layer

    # convert 2D matrix to raster
    r1 <- raster(t(v1)[nrow(t(v1)):1,])

    # set extent,crs, and resolution
    crs(r1) <- crs(r0)
    extent(r1) <- extent(r0)
    res(r1) <- res(r0)

    # compute areal mean for all catchments
    v2 <- execute_zonal(rast(r1),sf1,ID="divide_id",join=FALSE)
    names(v2) <- c("divide_id",pars_cfe[match(p1,pars_nwm)])

    if (p1 == pars_nwm[1]) {
        dtSoilPars <- v2
    } else {
        dtSoilPars <- merge(dtSoilPars, v2, by="divide_id")
    }

    rm(v1,r1)
    gc()
}
nc_close(nc)

# set the remaining soil parameters of CFE to default values
#dtSoilPars[["soil_params.mult"]] <- 1000.0
#dtSoilPars[["soil_params.depth"]] <- 2.0

dtSoilPars[[const_name[8]]] <- const_value[8]
dtSoilPars[[const_name[1]]] <- const_value[1]

# Groundwater parameters
message("processing groundwater parameters ...")
gw_file <- paste0(data_dir, "GWBUCKPARM_CONUS_FullRouting.nc")
gwparm <- GetNcdfFile(gw_file,quiet=TRUE)
gwvars <- names(gwparm)
gwparm <- data.table(gwparm)
setkey(gwparm, ComID)

# bucket/catchment mapping
gwWeightFile <- paste0(data_dir,"gwbuck_to_maskid_basins_group_",group,".txt")
dtWgt <- read.table(gwWeightFile, header=TRUE, sep="\t", stringsAsFactors=FALSE)
dtWgt <- data.table(dtWgt)
setkey(dtWgt, ComID)
gwparm <- merge(gwparm, dtWgt, all.x=TRUE, all.y=FALSE, suffixes=c("", ".cat"), by="ComID")
dtGwPars <- subset(gwparm, !is.na(cat_id))

# add divide_id from the crosswalk table
cwt <- read.table(paste0(data_dir,"raster_id_crosswalk_basins_group_1.csv"), header=TRUE, sep=",",
    colClasses=rep("character",7),stringsAsFactors=FALSE)
cwt$cat_id <- as.integer(cwt$cat_id)
dtGwPars <- merge(dtGwPars,cwt[,c("divide_id","gage","cat_id")],by="cat_id")
dtGwPars <- dtGwPars[,c("Coeff","Expon","Zmax","sumwt","divide_id","gage"),with=FALSE]

# compute weighted mean
dtGwPars1 <- dtGwPars[,.(Coeff=sum(Coeff*sumwt)/sum(sumwt),
                              Expon=sum(Expon*sumwt)/sum(sumwt),
                              #Zmax=sum(Zmax*sumwt)/sum(sumwt)),
                              #Zmax=sum(Zmax*sumwt)/sum(sumwt)/1000*10),
                              Zmax=sum(Zmax*sumwt)/sum(sumwt)/1000), #to be confirmed, Zmax for NMW is in mm (but m for CFE)
                              by=.(divide_id,gage)]
names(dtGwPars1) <- c("divide_id","gage","Cgw","expon","max_gw_storage")

dtParsAll <- merge(dtSoilPars,dtGwPars1,by="divide_id")

# set all other CFE parameters to default values 
#dtParsAll[["gw_storage"]] <- "50%"
#dtParsAll[["alpha_fc"]] <- 0.33
#dtParsAll[["soil_storage"]] <- "66.7%"
#dtParsAll[["K_lf"]] <- 0.1
#dtParsAll[["K_nash"]] <- 0.3
#dtParsAll[["nash_storage"]] <- "0.0,0.0"

dtParsAll[[const_name[2]]] <- const_value[2]
dtParsAll[[const_name[3]]] <- const_value[3]
dtParsAll[[const_name[4]]] <- const_value[4]
dtParsAll[[const_name[5]]] <- const_value[5]
dtParsAll[[const_name[6]]] <- const_value[6]
dtParsAll[[const_name[7]]] <- const_value[7]


# The GIUH ordinates is also set to some default values here. 
# More accurate estimates can be produced using the approached adopted in the following script in the ngen repo:
# extern/cfe/cfe/params/src/generate_giuh_per_basin_params.py
#dtParsAll[["giuh_ordinates"]] <- "0.06,0.51,0.28,0.12,0.03"
dtParsAll[[const_name[9]]] <- const_value[9]

# write the BMI configs file for CFE-S/CFE-X
message("write BMI config with initial parameters ...")
lines <- c("forcing_file=BMI","verbosity=0")
lines <- c(lines,paste0("surface_partitioning_scheme=",scheme))
lines <- c(lines,paste0("num_timesteps=",n_timesteps))
pars_in_order <- c("soil_params.depth","soil_params.b","soil_params.mult","soil_params.satdk",
    "soil_params.satpsi","soil_params.slop","soil_params.smcmax","soil_params.wltsmc")

out_dir <- output_dir 
if (scheme=="Schaake") {
    out_dir <- paste0(out_dir,"CFE-S")
} else if (scheme=="Xinanjiang") {
    out_dir <- paste0(out_dir,"CFE-X")
    pars_in_order <- c(pars_in_order, "a_Xinanjiang_inflection_point_parameter",
                "b_Xinanjiang_shape_parameter_set","x_Xinanjiang_shape_parameter")
}

pars_in_order <- c(pars_in_order,"max_gw_storage","Cgw","expon","gw_storage","alpha_fc","soil_storage","K_lf",
    "K_nash","nash_storage","giuh_ordinates")

for (c1 in dtParsAll$divide_id) {
    lines1 <- lines
    for (p1 in pars_in_order) lines1 <- c(lines1,paste0(p1,"=",subset(dtParsAll,divide_id==c1)[[p1]]))
    gage1 <- subset(dtParsAll,divide_id==c1)$gage

    outfile <- paste0(out_dir,"/Gage_",gage1,"/",c1,"_bmi_config.ini")
    if(!dir.exists(dirname(outfile))) dir.create(dirname(outfile),recursive=TRUE)

    writeLines(lines1,outfile)
}

}   

