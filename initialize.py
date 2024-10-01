# import scrape_confluence as sc
import load_data as ld
from load_s3_file import load as lsf
import zipfile

log_file = "init.log"
model_file = "all-mpnet-base-v2.zip"
vdb_file = "confluence_vdb"
if __name__ == "__main__":
    # Creating log file
    with open(f"./{log_file}", "w") as f:
        f.write("\nStarting initalize.py...")

    print("Loading VDB...")
    lsf(vdb_file)
    with open(f"./{log_file}", "a") as f:
        f.write("\nVDB loaded.")

    print("Loading Json data from DDL...")
    # sc.scrape_confluence()
    ld.load(log_file)
    with open(f"./{log_file}", "a") as f:
        f.write("\nJson data loaded.")

    # print("Loading Transformer model from S3...")
    lsf(model_file)
    with zipfile.ZipFile(model_file, "r") as zip_ref:
        zip_ref.extractall()

    print("Initialize completed!")
    with open(f"./{log_file}", "a") as f:
        f.write("\ninitalize.py completed!")
