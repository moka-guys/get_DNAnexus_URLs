import dxpy
import pandas as pd
from DNAnexus_auth_token import token
import re
import sys

'''
Running this script: 
python path/get_DNANexus_URLs.py -12w path/hg19_dnanexus.csv
'''

dxpy.set_security_context({"auth_token_type": "Bearer", "auth_token": token})

# generate download link for DNAnexus object
def download_url(file_ID, project_ID):
    dxfile = dxpy.DXFile(file_ID)
    download_link = dxfile.get_download_url(
        duration=82800,
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
        pattern = re.compile(r"(Pan\d+)")
        pan_number = pattern.search(file_name)
        pan_num = pan_number.group()
        BAI_name = file_name + ".bai"
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, BAI_name, folder, pan_num, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(
        data, columns=["name","bai_name", "folder", "pan_num", "project_id", "bam_file_id"]
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


# This function produces a dataframe with the URLs for BAM and BAM Index files
def final_url_links(merged_df):
    merged_df = merged_df.sort_values("project_id")

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

    pattern = re.compile(r"(\S+.bam)")

    # Copy and shift project_ID column to enable comparsion
    merged_df["prev_project_id"] = merged_df["project_id"].shift(1)

    # Generate URL links for each BAM and BAM index file in the df
    for i in range(0, len(merged_df)):
        bai_name = merged_df["bai_name"][i]
        pattern_search = pattern.search(bai_name)
        bam_name = pattern_search.group()
        project_id = merged_df["project_id"][i]
        prev_project_id = merged_df["prev_project_id"][i]
        bam_id = merged_df["bam_file_id"][i]
        index_id = merged_df["index_file_id"][i]
        # this if statement reduces the numer of requests made by the dxpy to find project names
        if project_id == prev_project_id:
            merged_df["project_name"][i] = merged_df["project_name"][i - 1]
        else:
            merged_df["project_name"][i] = find_project_name(project_id)
        merged_df["url"][i] = download_url(bam_id, project_id) + "/" + bam_name
        merged_df["indexURL"][i] = download_url(index_id, project_id) + "/" + bai_name
        #Show progress bar

        sys.stdout.write("{} ".format(i+1))
        sys.stdout.flush()
    sys.stdout.write("]\n")

    merged_df = merged_df.drop(["prev_project_id", "bai_name"], axis=1)
    return merged_df.sort_values(["name", "folder"])


if __name__=="__main__":
    
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
    merged = pd.merge(all_BAM_df, all_BAI_df, on=["bai_name", "folder", "project_id"])
    
    if len(merged) > 0:
        # generate the url links and project name
        print("Generating URL links for {} BAM and BAM Index files:".format(len(merged)))
        url_links = final_url_links(merged)
        url_links.to_csv(sys.argv[2], index=False, sep=",")
        print("csv file with URL links created successfully")
    else:
        print("No matching BAM and BAM Index files were found within the time frame specified: {}".format(length))
        
