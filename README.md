# draost2caom2
Application to generate CAOM2 Observations from DRAO Synthesis Telescope FITS files.

# How to run the draost2caom2 pipeline

In an empty directory (the 'working directory'), on a machine with Docker installed:

1. This is the working directory, so it should probably have some space.

1. Set up credentials. A CADC account is required (you can request one here: 
http://www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca/en/auth/request.html) In this directory, create a file named 'netrc'. 
This is the expected .netrc file that will have the credentials required for the 
CADC services. These credentials allow the user to read, write, and delete 
CAOM2 observations, and read file header metadata and files 
from data. This file should have content that looks like the following:

   ```
   machine www.cadc-ccda.hia-iha.nrc-cnrc.gc.ca login canfarusername password canfarpassword
   machine www.canfar.net login canfarusername password canfarpassword
   machine sc2.canfar.net login canfarusername password canfarpassword
   ```
   
   1. Replace canfarusername and canfarpassword with your CADC username and 
   password values.

   1. The permissions for this file must be 600 (owner rw only).
   
   1. The man page for netrc:
   https://www.systutorials.com/docs/linux/man/5-netrc/
   
   1. The name and location of this file may be changed by modifying the 
   netrc_filename entry in the config.yml file. This entry requires a 
   fully-qualified pathname.

1. Get the script that executes the pipeline by doing the following:

   ```
   wget https://raw.github.com/opencadc-metadata-curation/draost2caom2/master/scripts/draost_run.sh
   ```

1. Ensure the script is executable:

    ```
    chmod +x draost_run.sh
    ```

1. To run the application:

    ```
    ./draost_run.sh
    ```

1. To debug the application from inside the container:

   ```
   user@dockerhost:<cwd># docker run --rm -ti -v <cwd>:/usr/src/app --name draost_run opencadc/draost2caom2 /bin/bash
   root@53bef30d8af3:/usr/src/app# draost_run
   ```

1. For some instructions that might be helpful on using containers, see:
https://github.com/opencadc-metadata-curation/collection2caom2/wiki/Docker-and-Collections

1. For some insight into what's happening, see: https://github.com/opencadc-metadata-curation/collection2caom2