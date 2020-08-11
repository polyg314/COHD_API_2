import pandas as pd
import json
import os

PAIRED_CONCEPT_FILE_NAME = 'pc_sorted_test.txt'
PAIRED_CONCEPT_COLUMN_NAMES = ["dataset_id","concept_id_1","concept_id_2","concept_count","concept_prevalence","chi_square_t","chi_square_p","expected_count","ln_ratio","rel_freq_1","rel_freq_2"]
CONCEPT_XREF_FILE_NAME = 'concept_xref.json'
CHUNK_SIZE = 1000000
MAX_COMBOS = 100

def load_annotations(data_folder):

    paired_concept_url = os.path.join(data_folder,PAIRED_CONCEPT_FILE_NAME)
    concept_xref_url = os.path.join(data_folder,CONCEPT_XREF_FILE_NAME)
    
    with open(concept_xref_url) as f:
        xref_data = json.load(f)
    
    xref_data_dict = {}

    for x in xref_data:
        xref_data_dict[x["_id"]] = x

    paired_concepts_table_total = pd.read_csv(paired_concept_url, sep='\t', header=None, names= PAIRED_CONCEPT_COLUMN_NAMES, chunksize=CHUNK_SIZE)  
    for chunk in paired_concepts_table_total:
        paired_concepts_table = chunk
        for i,j in paired_concepts_table.iterrows():
            current_id_1 = str(int(j["concept_id_1"]))
            current_id_2 = str(int(j["concept_id_2"]))
            current_dict = {
              "_id": current_id_1 + "-" + current_id_2,
              "concept1": {
                 "omop": current_id_1,
                 "xrefs": xref_data_dict[current_id_1]["xrefs"]
              },
              "concept2": {
                 "omop": current_id_2,
                 "xrefs": xref_data_dict[current_id_2]["xrefs"]
              },
              "results": [
                {
                    "concept_count": int(j["concept_count"]),
                    "concept_prevalence": j["concept_prevalence"],
                    "dataset_id": j["dataset_id"],
                    "chi_square_t": j["chi_square_t"],
                    "chi_square_p": j["chi_square_p"],
                    "ln_ratio": j["ln_ratio"],
                    "rel_freq_1": j["rel_freq_1"],
                    "rel_freq_2":j["rel_freq_2"]
                }
              ]
            }
#             print(current_dict)
            yield(current_dict)
            