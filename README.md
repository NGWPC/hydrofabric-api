1.)  Get Docker container from s3://ngwpc-dev/DanielCumpton/hydrofabric_service_v1.tar
2.   Get supporting data from s3://ngwpc-dev/DanielCumpton/hydrofabric_data.tgz

3.)  Start Docker and load docker image:  sudo systemctl start docker  
       sudo docker load -i hydrofabric_service_v1.tar

4.)  Untar hydrofabric_data.tgz in home directory (or wherever you want)

5.)  Get docker image id, create container, and mount hydrofabric data directory:
       sudo docker images  
       sudo docker run -ti -v <root path to where you untarred the hydrofabric data>/Hydrofabric:/Hydrofabric <image id from previous command > bash
                

6.)  Clone hydrofabric_api repo to container. 
                Paths for input data, hydrofabric data, the temp directory, and s3 bucket information are stored in
                config.yml.  You shouldn't need to change any of the paths except for maybe the s3 bucket and/or prefix.  
                 The django endpoints are under djangoApps/init_param_app/views.py
                
7.)  In the terminal where django will be run:
     Set environment variable for GDAL:  export LD_LIBRARY_PATH=/usr/local/lib64
     Copy and paste AWS S3 credentials.  
                 
8.)  Run Django server:  cd <root path>/hydrofabric_api/djangoApps
       python3 manage.py runserver
                
9.)  curl commands for getting the geopackage and for getting initial parameters for CFE-S and CFE-X are in 
     /home/hydrofabric_api/curl.py.
     python3 curl.py          
                 
10.)  Files are written to the s3 bucket and you can see them in the temp directory (/Hydrofabric/data/temp)
