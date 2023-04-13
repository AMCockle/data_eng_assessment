import unittest
import json
from extract_patient_data import get_patient_data,get_patient_identifier_data
from connect_postgres import connect_to_postgres

class testPatientData(unittest.TestCase):
    def runTest(self):
        with open("test_files/Aaron697_Dickens475_8c95253e-8ee8-9ae8-6d40-021d702dc78e.json", "r", encoding="utf8") as patient_data_json:
            patient_all_data = json.load(patient_data_json)
            entries = patient_all_data["entry"]
            patient_data = [entry["resource"] for entry in entries if entry["resource"]["resourceType"]=="Patient"][0]
            patient_information = get_patient_data(patient_data)
            assert patient_information[0] == "8c95253e-8ee8-9ae8-6d40-021d702dc78e"
            assert patient_information[8] == 0.05826471038258488
            
            patient_identifiers = get_patient_identifier_data(patient_data)
            assert patient_identifiers[1] == ('8c95253e-8ee8-9ae8-6d40-021d702dc78e', 'Social Security Number', '999-86-2571')
            assert patient_identifiers[3] == ('8c95253e-8ee8-9ae8-6d40-021d702dc78e', 'Passport Number', 'X67249552X')


class testPostgresConnect(unittest.TestCase):
    def runTest(self):
        connect_to_postgres()

unittest.main()