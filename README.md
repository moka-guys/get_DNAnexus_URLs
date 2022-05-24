# get_DNAnexus_URLs
Clinical Scietists reported that they could not view the mappped files in the IGV browser easily. 

Therefore, get\_DNAnexus\_URLs was created to speed up the process

This code is written to generate .json containing URLs for VCF, BAM and index files located on DNAnexus for use in the IGV browser. 

Furthermore, the link to VCF files for WES, SNP, ONC, TSO500 are generated. 

The code is to be run every day to refresh the .json file with updated information. 

The URLs are generated for files created within the requested time frame.

The URL links are active for 24 hours.

## Running the script

Run this script in python 3. 

python script\_name length output\_name

### Example:

python3 path/get\_DNANexus\_URLs.py -12w path/hg19\_dnanexus.json

-12w = search the previous 12 weeks from now;
hg19\_dnanexus.json = output file name

## Part of Ansible Playbook

This script can be deployed on the Genapp server using the following ansible playbook:

igvwebapp.yml located at https://github.com/moka-guys/deployment/tree/develop/playbooks

## Contributions
Always use the git flow pattern when making contributions!
The `main` branch is reserved for code-reviewed production(-ready) code

## Author
Igor Malashchuk, 
STP in Clinical Boinformatics
Guys and St Thomas's NHS Foundation Trust
Updated: 16th May 2022 
