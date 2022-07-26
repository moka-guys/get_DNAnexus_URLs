import dxpy
import pandas as pd
from DNAnexus_auth_token import token
import sys
import math
import json
from tqdm import tqdm

"""
Running this script example: 
python path/get_DNANexus_URLs.py -12w path/hg19_dnanexus.csv
"""
version = "version 1.2.0"
dxpy.set_security_context({"auth_token_type": "Bearer", "auth_token": token})

json_data = {
    "label": "DNAnexus tracks",
    "type": "custom-data-modal",
    "description": "BAM and VCF tracks from DNAnexus",
    "columns": ["name", "project_name", "folder"],
    "columnDefs": {
        "name": {"title": "Sample"},
        "project_name": {"title": "Project"},
        "folder": {"title": "Folder"},
    },
    "data": [],
}

# generate download link for DNAnexus object
def download_url(file_ID, project_ID):
    """
    Generates url link for a specific file
    """
    dxfile = dxpy.DXFile(file_ID)
    download_link = dxfile.get_download_url(
        duration=86400,
        preauthenticated=True,
        project=project_ID,
    )
    return download_link[0]


# find data based on the name of the file
def find_data(filename, length):
    """
    Searches the DNA nexus for files when given a file name and returns the data in a list
    """
    data = list(
        dxpy.bindings.search.find_data_objects(
            name=filename, name_mode="glob", describe=True, created_after=length
        )
    )
    return data


def find_data_regex(filename, length):
    """
    Same function as find_data but uses regex pattern to search for data in DNAnexus
    """
    data = list(
        dxpy.bindings.search.find_data_objects(
            name=filename, name_mode="regexp", describe=True, created_after=length
        )
    )
    return data


def find_project_name(project_id):
    """
    Find the name of a project when giving a file ID
    """
    project_data = dxpy.bindings.dxproject.DXProject(dxid=project_id)
    return project_data.describe().get("name")


#
def create_df_for_BAM_VCF(data_list, type_of_index):
    """
    This function generates a dataframe containing the modified names of BAM/VCF files
    including unique file ids and project id
    """
    data = []
    for object in data_list:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        if type_of_index == "bai":
            index_name = file_name + ".bai"
        elif type_of_index == "tbi":
            index_name = file_name + ".tbi"
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, index_name, folder, project_id, object_id]
        data.append(merged_data)
    return pd.DataFrame(
        data, columns=["name", "index_name", "folder", "project_id", "file_id"]
    )


def create_df_for_VCF(data_list, index):
    """
    Generate a dataframe containing the names of Index files including unique object ids and project id
    """
    data = []
    for object in data_list:
        file_name = object.get("describe").get("name")
        folder = object.get("describe").get("folder")
        object_id = object.get("describe").get("id")
        project_id = object.get("describe").get("project")
        merged_data = [file_name, folder, project_id, object_id]
        data.append(merged_data)
    if index == "Y":
        name = "index_name"
        file_id = "index_file_id"
    else:
        name = "name"
        file_id = "file_id"
    return pd.DataFrame(data, columns=[name, "folder", "project_id", file_id])


def generate_url_links_with_index(merged_df):
    """
    This function generates URL links for a list of BAM/VCF files with indexes
    """
    uniqueProjects = merged_df["project_id"].unique()
    project_dict = {}
    for project in uniqueProjects:
        proj_id = project
        proj_name = find_project_name(proj_id)
        project_dict[proj_id] = {"name": proj_name}
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    merged_df["indexURL"] = ""
    # Generate URL links for each BAM and BAM index file in the df
    for i in tqdm(range(0, len(merged_df))):
        index_name = merged_df["index_name"][i]
        name = merged_df["name"][i]
        project_id = merged_df["project_id"][i]
        bam_id = merged_df["file_id"][i]
        index_id = merged_df["index_file_id"][i]
        merged_df["project_name"][i] = project_dict[project_id]["name"]
        merged_df["url"][i] = download_url(bam_id, project_id) + "/" + name
        merged_df["indexURL"][i] = download_url(index_id, project_id) + "/" + index_name
    merged_df = merged_df.drop(
        ["index_name", "project_id", "file_id", "index_file_id"], axis=1
    )
    return merged_df.sort_values(["name", "folder"])


def generate_url_links_without_index(merged_df):
    """
    This function generates URL links for a list of BAM/VCF files without indexes
    """
    uniqueProjects = merged_df["project_id"].unique()
    project_dict = {}
    for project in uniqueProjects:
        proj_id = project
        proj_name = find_project_name(proj_id)
        project_dict[proj_id] = {"name": proj_name}
    # create new columns with url links and project name
    merged_df["project_name"] = ""
    merged_df["url"] = ""
    # Generate URL links for each BAM and BAM index file in the df
    for i in tqdm(range(0, len(merged_df))):
        name = merged_df["name"][i]
        project_id = merged_df["project_id"][i]
        vcf_id = merged_df["file_id"][i]
        merged_df["project_name"][i] = project_dict[project_id]["name"]
        merged_df["url"][i] = download_url(vcf_id, project_id) + "/" + name
    merged_df = merged_df.drop(["project_id", "file_id"], axis=1)
    return merged_df.sort_values(["name", "folder"])


def generate_json_data(df):
    """
    This function generates a list of dictionaries containing a list of URL links.
    """
    raw_data = []
    print('Generating a list of dictionaries for each BAM/VCF file with name, project_name, folder and urls (including index file if exists)')
    for i in tqdm(range(0, len(df.index))):
        name = url_links["name"][i]
        folder = url_links["folder"][i]
        project_name = url_links["project_name"][i]
        url = url_links["url"][i]
        index = url_links["indexURL"][i]
        try:
            if math.isnan(index):
                value = {
                    "name": name,
                    "project_name": project_name,
                    "folder": folder,
                    "url": url,
                }
        except:
            value = {
                "name": name,
                "project_name": project_name,
                "folder": folder,
                "url": url,
                "indexURL": index,
            }
        raw_data.append(value)
    return raw_data


if __name__ == "__main__":

    if len(sys.argv) != 3:
        raise RuntimeError(
            "EXAMPLE USAGE: python get_DNAnexus_URLs.py -12w output.json"
        )

    length = sys.argv[
        1
    ]  # e.g. argument '-12w' = search DNAnexus for the previous 12 weeks from now

    # Retrieve infomration for BAM files
    print("Searching for BAM files...")
    all_BAM = find_data("*.bam", length)
    all_BAM_df = create_df_for_BAM_VCF(all_BAM, "bai")

    # Retrieve information for index files
    print("Searching for BAM Index files...")
    all_BAI = find_data("*.bam.bai", length)
    all_BAI_df = create_df_for_VCF(all_BAI, "Y")

    # merged two dataframes by matching modified bam file name with index filen name as well as folder and project id
    print("Merging BAM and BAM Index files...")
    merged_BAM = pd.merge(
        all_BAM_df, all_BAI_df, on=["index_name", "folder", "project_id"]
    )

    print("Searching for TSO500 VCF files...")
    all_TSO = find_data_regex("^TSO\S+_MergedSmallVariants.genome.vcf.gz$", length)
    all_TSO_df = create_df_for_BAM_VCF(all_TSO, "tbi")
    print("Searching for TSO500 VCF Index files...")
    all_TSO_tbi = find_data_regex("^TSO\S+_MergedSmallVariants.genome.vcf.gz.tbi$", length)
    all_TSO_tbi_df = create_df_for_VCF(all_TSO_tbi, "Y")
     # merging vcf and index dataframes
    print("Merging TSO500 VCF and VCF Index files...")
    merged_TSO = pd.merge(
        all_TSO_df, all_TSO_tbi_df, on=["index_name", "folder", "project_id"]
    )

    print("Searching for ONC VCF files...")
    pc_vardict = find_data("*.refined.primerclipped.vardict.vcf", length)
    pc_vardict_df = create_df_for_VCF(pc_vardict, "N")
    pc_varscan = find_data("*.refined.primerclipped.varscan.bedfiltered.vcf", length)
    pc_varscan_df = create_df_for_VCF(pc_varscan, "N")

    print("Searching for SNP VCF files...")
    all_snp = find_data("*.sites_present_reheader_filtered_normalised.vcf", length)
    all_snp_df = create_df_for_VCF(all_snp, "N")

    # Retrieve infomration for BAM files
    print("Searching for WES vcf files...")
    all_wes = find_data_regex("^NGS\S+_Haplotyper.vcf.gz$", length)
    all_wes_df = create_df_for_BAM_VCF(all_wes, "tbi")

    # Retrieve information for index files
    print("Searching for WES VCF Index files...")
    all_wes_tbi = find_data_regex("^NGS\S+_Haplotyper.vcf.gz.tbi$", length)
    all_wes_tbi_df = create_df_for_VCF(all_wes_tbi, "Y")
    # merging vcf and index dataframes
    merged_wes = pd.merge(
        all_wes_df, all_wes_tbi_df, on=["index_name", "folder", "project_id"]
    )

    # Retrieve infomration for MokaPipe
    print("Searching for MokaPipe vcf files...")
    all_mokapipe = find_data_regex("^NGS\S+.bedfiltered.vcf.gz$", length)
    all_mokapipe_df = create_df_for_BAM_VCF(all_mokapipe, "tbi")

    url_list = []
    if len(merged_TSO) > 0:
        print("Generating URL links for {} TSO VCF files:".format(len(merged_TSO)))
        TSO_links = generate_url_links_with_index(merged_TSO)
        url_list.append(TSO_links)
    if len(all_mokapipe_df) > 0:
        print("Generating URL links for {} MokaPipe VCF files:".format(len(all_mokapipe_df)))
        mokapipe_links = generate_url_links_without_index(all_mokapipe_df)
        url_list.append(mokapipe_links)
    if len(pc_vardict_df) > 0:
        print(
            "Generating URL links for {} ONC vardict VCF files:".format(len(pc_vardict_df))
        )
        pc_vardict_links = generate_url_links_without_index(pc_vardict_df)
        url_list.append(pc_vardict_links)
    if len(pc_varscan_df) > 0:
        print(
            "Generating URL links for {} ONC varscan VCF files:".format(len(pc_varscan_df))
        )
        pc_varscan_links = generate_url_links_without_index(pc_varscan_df)
        url_list.append(pc_varscan_links)
    if len(all_snp_df) > 0:
        print("Generating URL links for {} SNP VCF files:".format(len(all_snp_df)))
        snp_links = generate_url_links_without_index(all_snp_df)
        url_list.append(snp_links)
    if len(merged_BAM) > 0:
        print(
            "Generating URL links for {} BAM and BAM Index files:".format(
                len(merged_BAM)
            )
        )
        BAM_url_links = generate_url_links_with_index(merged_BAM)
        url_list.append(BAM_url_links)
    if len(merged_wes) > 0:
        print("Generating URL links for {} WES VCF files:".format(len(merged_wes)))
        wes_url_links = generate_url_links_with_index(merged_wes)
        url_list.append(wes_url_links)
    url_links = pd.concat(url_list, ignore_index=True)
    url_links = url_links.sort_values(["name", "folder"])

    json_data["data"] = generate_json_data(url_links)

    with open(sys.argv[2], "w") as f:
        json.dump(json_data, f)
    print("get_DNAnexus_URLs.py {} script created JSON file: {}".format(version, sys.argv[2]))

"""
Search patterns for VCF files
-TSO
    #MergedSmallVariants.genome.vcf.gz
    #MergedSmallVariants.genome.vcf.gz.tbi

-ONC
#primerclipped.vardict.vcf
#primerclipped.varscan.bedfiltered.vcf

-WES
#NGS\S+Haplotyper.vcf.gz 
#NGS\S+Haplotyper.vcf.gz.tbi

-SNP:
    #.sites_present_reheader_filtered_normalised.vcf

-MokaPipe
 bedfiltered.vcf.gz
"""
