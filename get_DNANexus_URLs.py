import dxpy
import pandas as pd
from DNAnexus_auth_token import token
import sys
import math
import json

'''
Running this script example: 
python path/get_DNANexus_URLs.py -12w path/hg19_dnanexus.csv
'''

dxpy.set_security_context({"auth_token_type": "Bearer", "auth_token": token})

json_data = {
                    "label": "DNAnexus tracks",
                    "type": "custom-data-modal",
                    "description": "BAM and VCF tracks from DNAnexus",
                    "columns": [
                        "name",
                        "project_name",
                        "folder"
                    ],
                    "columnDefs": {
                        "name": {
                        "title": "Sample"
                        },
                        "project_name": {
                        "title": "Project"
                        },
                        "folder" : {
                        "title": "Folder"
                        }

                    },
                    "data": []
            }

# generate download link for DNAnexus object
def download_url(file_ID, project_ID):
    dxfile = dxpy.DXFile(file_ID)
    download_link = dxfile.get_download_url(
        duration=86400,
        preauthenticated=True,
        project=project_ID,
    )
    return download_link[0]

# find data based on the name of the file
def find_data(filename, length):
    data = list(
        dxpy.bindings.search.find_data_objects(
            name=filename, name_mode="glob", describe=True, created_after=length
        )
    )
    return data

def find_data_regex(filename, length):
    data = list(
        dxpy.bindings.search.find_data_objects(
            name=filename, name_mode="regexp", describe=True, created_after=length
        )
    )
    return data

# find project name using unique project id
def find_project_name(project_id):
    project_data = dxpy.bindings.dxproject.DXProject(dxid=project_id)
    return project_data.describe().get("name")

# This function generates a dataframe containing the modified names of BAM files (in Index format) including unique object ids and project id
def create_BAM_df(BAM_Data):
    data = []
    for object in BAM_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        BAI_name = file_name + ".bai"
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, BAI_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(
        data, columns=["name","bai_name", "folder", "project_id", "bam_file_id"]
    )

def create_vcf_gz(BAM_Data):
    data = []
    for object in BAM_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        tbi_name = file_name + ".tbi"
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, tbi_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(
        data, columns=["name","tbi_name", "folder", "project_id", "file_id"]
    )

# generate a dataframe containing the names of Index files including unique object ids and project id
def create_BAI_df(Index_Data):
    data = []
    for object in Index_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(data, columns=["bai_name", "folder", "project_id", "index_file_id"])

def create_tbi_df(Index_Data):
    data = []
    for object in Index_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(data, columns=["tbi_name", "folder", "project_id", "index_file_id"])

def create_df(Index_Data):
    data = []
    for object in Index_Data:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(data, columns=["name", "folder", "project_id", "vcf_file_id"])


# This function produces a dataframe with the URLs for BAM and BAM Index files
def final_url_links(merged_df):
    #Find project names
    uniqueProjects = merged_df["project_id"].unique()
    project_dict = {}
    for project in range(0, len(uniqueProjects)):
        proj_id = uniqueProjects[project]
        proj_name = find_project_name(proj_id)
        project_dict[proj_id] = { 'name' : proj_name}
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    merged_df["indexURL"] = ""
    # show progress of the loop
    toolbar_width = len(merged_df)
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
    sys.stdout.flush()
    # Generate URL links for each BAM and BAM index file in the df
    for i in range(0, len(merged_df)):
        bai_name = merged_df["bai_name"][i]
        bam_name = merged_df["name"][i]
        project_id = merged_df["project_id"][i]
        bam_id = merged_df["bam_file_id"][i]
        index_id = merged_df["index_file_id"][i]
        merged_df["project_name"][i] = project_dict[project_id]['name']
        merged_df["url"][i] = download_url(bam_id, project_id) + "/" + bam_name
        merged_df["indexURL"][i] = download_url(index_id, project_id) + "/" + bai_name
        #Show progress bar
        sys.stdout.write("{} ".format(i+1))
        sys.stdout.flush()
    sys.stdout.write("]\n")
    merged_df = merged_df.drop(["bai_name", "project_id", "bam_file_id", "index_file_id"], axis=1)
    return merged_df.sort_values(["name", "folder"])

def final_wes_url_links(merged_df):
    #Find project names
    uniqueProjects = merged_df["project_id"].unique()
    project_dict = {}
    for project in range(0, len(uniqueProjects)):
        proj_id = uniqueProjects[project]
        proj_name = find_project_name(proj_id)
        project_dict[proj_id] = { 'name' : proj_name}
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    merged_df["indexURL"] = ""
    # show progress of the loop
    toolbar_width = len(merged_df)
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
    sys.stdout.flush()
    # Generate URL links for each BAM and BAM index file in the df
    for i in range(0, len(merged_df)):
        tbi_name = merged_df["tbi_name"][i]
        name = merged_df["name"][i]
        project_id = merged_df["project_id"][i]
        bam_id = merged_df["file_id"][i]
        index_id = merged_df["index_file_id"][i]
        merged_df["project_name"][i] = project_dict[project_id]['name']
        merged_df["url"][i] = download_url(bam_id, project_id) + "/" + name
        merged_df["indexURL"][i] = download_url(index_id, project_id) + "/" + tbi_name
        #Show progress bar
        sys.stdout.write("{} ".format(i+1))
        sys.stdout.flush()
    sys.stdout.write("]\n")
    merged_df = merged_df.drop(["tbi_name", "project_id", "file_id", "index_file_id"], axis=1)
    return merged_df.sort_values(["name", "folder"])

def vcf_url_links(merged_df):
    #Find project names
    uniqueProjects = merged_df["project_id"].unique()
    project_dict = {}
    for project in range(0, len(uniqueProjects)):
        proj_id = uniqueProjects[project]
        proj_name = find_project_name(proj_id)
        project_dict[proj_id] = { 'name' : proj_name}
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    # show progress of the loop
    toolbar_width = len(merged_df)
    sys.stdout.write("[%s]" % (" " * toolbar_width))
    sys.stdout.flush()
    sys.stdout.write("\b" * (toolbar_width+1)) # return to start of line, after '['
    sys.stdout.flush()
    # Generate URL links for each BAM and BAM index file in the df
    for i in range(0, len(merged_df)):
        name = merged_df["name"][i]
        project_id = merged_df["project_id"][i]
        vcf_id = merged_df["vcf_file_id"][i]
        merged_df["project_name"][i] = project_dict[project_id]['name']
        merged_df["url"][i] = download_url(vcf_id, project_id) + "/" + name
        #Show progress bar
        sys.stdout.write("{} ".format(i+1))
        sys.stdout.flush()
    sys.stdout.write("]\n")
    merged_df = merged_df.drop(["project_id", "vcf_file_id"], axis=1)
    return merged_df.sort_values(["name", "folder"])




if __name__=="__main__":

    if len(sys.argv) != 3:
        raise RuntimeError("EXAMPLE USAGE: python get_DNAnexus_URLs.py -12w output.csv")
    
    length = sys.argv[1] # e.g. argument '-12w' = search DNAnexus for the previous 12 weeks from now
    
    # Retrieve infomration for BAM files
    print("Searching for BAM files...")
    all_BAM = find_data("*.bam", length)
    all_BAM_df = create_BAM_df(all_BAM)

    # Retrieve information for index files
    print("Searching for BAM Index files...")
    all_BAI = find_data("*.bam.bai", length)
    all_BAI_df = create_BAI_df(all_BAI)
    
    # merged two dataframes by matching modified bam file name with index filen name as well as folder and project id
    print("Merging BAM and BAM Index files...")
    merged_BAM = pd.merge(all_BAM_df, all_BAI_df, on=["bai_name", "folder", "project_id"])
    

    print("Searching for TSO500 VCF files...")
    all_TSO = find_data("*MergedSmallVariants.genome.vcf", length)
    all_TSO_df = create_df(all_TSO)
    

    print("Searching for ONC VCF files...")
    primer_clipped = find_data("*primerclipped.vardict.vcf", length)
    primer_clipped_df = create_df(primer_clipped)
   
   
    print("Searching for SNP VCF files...")
    all_snp = find_data("*.sites_present_reheader_filtered_normalised.vcf", length)
    all_snp_df = create_df(all_snp)
    

    # Retrieve infomration for BAM files
    print("Searching for WES vcf files...")
    all_wes = find_data_regex("^NGS\S+_Haplotyper.vcf.gz$", length)
    all_wes_df = create_vcf_gz(all_wes)

    # Retrieve information for index files
    print("Searching for WES VCF Index files...")
    all_wes_tbi = find_data_regex("^NGS\S+_Haplotyper.vcf.gz.tbi$", length)
    all_wes_tbi_df = create_tbi_df(all_wes_tbi)
    # merging vcf and index dataframes
    merged_wes = pd.merge(all_wes_df, all_wes_tbi_df, on=["tbi_name", "folder", "project_id"])

    

    url_list = []
    if len(all_TSO_df) > 0:
        print("Generating URL links for {} TSO VCF files:".format(len(all_TSO_df)))
        TSO_links = vcf_url_links(all_TSO_df)
        url_list.append(TSO_links)
    if len(primer_clipped_df) > 0:
        print("Generating URL links for {} ONC VCF files:".format(len(primer_clipped_df)))
        primer_clipped_links = vcf_url_links(primer_clipped_df)
        url_list.append(primer_clipped_links)
    if len(all_snp_df) > 0:
        print("Generating URL links for {} ONC VCF files:".format(len(all_snp_df)))
        snp_links = vcf_url_links(all_snp_df)
        url_list.append(snp_links)
    if len(merged_BAM) > 0:
        print("Generating URL links for {} BAM and BAM Index files:".format(len(merged_BAM)))
        BAM_url_links = final_url_links(merged_BAM)
        url_list.append(BAM_url_links)
    if len(merged_wes) > 0:
        print("Generating URL links for {} ONC VCF files:".format(len(merged_wes)))
        wes_url_links = final_wes_url_links(merged_wes)
        url_list.append(wes_url_links)
    url_links = pd.concat(url_list, ignore_index=True)
    url_links = url_links.sort_values(["name", "folder"])
    raw_data = []
    for i in range(0,len(url_links.index)):
        name = url_links['name'][i]
        folder = url_links['folder'][i]
        project_name = url_links['project_name'][i]
        url = url_links['url'][i]
        index = url_links['indexURL'][i]
        try: 
            if math.isnan(index):
                value = { 'name' : name,
                'project_name' : project_name,  
                'folder' : folder, 
                'url' : url, }
        except:
            value = { 'name' : name,
            'project_name' : project_name,  
            'folder' : folder, 
            'url' : url, 
            'indexURL' : index }
        raw_data.append(value)
    json_data['data'] = raw_data
    with open(sys.argv[2], 'w') as f:
        json.dump(json_data, f)


'''
Search patterns for VCF files
-TSO
    #MergedSmallVariants.genome.vcf

-ONC
#primerclipped.vardict.vcf
#primerclipped.varscan.bedfiltered.vcf

-WES
#NGS\S+Haplotyper.vcf.gz 
#NGS\S+Haplotyper.vcf.gz.tbi

-SNP:
    #.sites_present_reheader_filtered_normalised.vcf
'''










