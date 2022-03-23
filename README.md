# get_DNAnexus_URLs


Clinical Scietists reported that they could not view the mappped files in the IGV browser easily. 

Therefore, get\_DNAnexus\_URLs was created to speed up the process

This code is written to generate .csv containing URLs for index and BAM files located on the DNAnexus for use in the IGV browser. 

The code is to be run every day to refresh the .csv file with udated information. 

The URLs are generated for files created within the requested time frame.

The URL links are active for 23 hours.

## Running the script

Run this script in python 3. 

python script\_name length output\_name

### Example:

python3 path/get\_DNANexus\_URLs.py -12w path/hg19z_dnanexus.csv

-12w = search the previous 12 weeks from now;
hg19_dnanexus.csv = output file name

## Part of Ansible Playbook

This script can be deployed on the Genapp server using the following ansible playbook:

igvwebapp.yml located at https://github.com/moka-guys/deployment/tree/develop/playbooks

## Author
Igor Malashchuk, 
STP in Clinical Boinformatics
Guys and St Thomas's NHS Foundation Trust
Updated: 21st March 2022 
