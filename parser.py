import pandas as pd
import json
import os

PAIRED_CONCEPT_FILE_NAME = 'paired_concept_counts_associations.csv'
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

    # last_id_1 = ''
    current_id = ''
    ids_included = []
    # yield_result = True

    current_results = []

    paired_concepts_table_total = pd.read_csv(paired_concept_url, chunksize=CHUNK_SIZE)  


    for chunk in paired_concepts_table_total:
        paired_concepts_table = chunk
        for i,j in paired_concepts_table.iterrows():
            last_id_1 = current_id
            current_id_1 = str(int(j["concept_id_1"]))
            current_id_2 = str(int(j["concept_id_2"]))
            current_id = current_id_1 + "-" + current_id_2
            if(last_id_1 == current_id):
                if(current_id in ids_included):
                    for i in range(0,len(current_results)):
                        if(current_results[i]["_id"] == current_id):
                            current_results[i]["results"].append({
                                "concept_count": int(j["concept_count"]),
                                "concept_prevalence": j["concept_prevalence"],
                                "dataset_id": j["dataset_id"],
                                "chi_square_t": j["chi_square_t"],
                                "chi_square_p": j["chi_square_p"],
                                "ln_ratio": j["ln_ratio"],
                                "rel_freq_1": j["rel_freq_1"],
                                "rel_freq_2":j["rel_freq_2"]
                            })
                else:
                    ids_included.append(current_id)
                    current_dict = {
                      "_id": current_id,
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
                    current_results.append(current_dict)
            else:
                id_2_matches = [];
                
                for x in current_results:
                   yield(x)

                current_dict = {
                  "_id": current_id,
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
                current_results = []
                current_results.append(current_dict)
                ids_included = []
                ids_included.append(current_id)
    
    for x in current_results:
        yield(x)
            
