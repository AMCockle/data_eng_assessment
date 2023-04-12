import pandas as pd
from connect_postgres import connect_to_postgres
import os
import json

def insert_patient_json_data(path):
    files = [path + "/" + file for file in os.listdir(path) if file.endswith(".json")]

    con = connect_to_postgres()
    cursor = con.cursor()
    con.autocommit = True
    
    for file in files:
        with open(file, "r", encoding="utf8") as patient_data_json:
            patient_data = json.load(patient_data_json)
            entries = patient_data["entry"]
            for entry in entries:
             print(entry)