from connect_postgres import connect_to_postgres
import os
import json

def insert_patient_json_data(path):
    files = [path + "/" + file for file in os.listdir(path) if file.endswith(".json")]
    
    for file in files:
        with open(file, "r", encoding="utf8") as patient_data_json:
            patient_data = json.load(patient_data_json)
            entries = patient_data["entry"]
            get_patient_data(entries)
            # get identifier data


    con = connect_to_postgres()
    cursor = con.cursor()
    con.autocommit = True

    # save data to postgres


            
def get_patient_data(entries):
    patient_data = [entry["resource"] for entry in entries if entry["resource"]["resourceType"]=="Patient"][0]
    id = patient_data["id"]
    extension = patient_data["extension"]

    def get_extension_field(ext_path, field_name):
        ext = [ex for ex in extension if ex["url"].endswith(field_name)]
        if ext_path in ext[0]:
            return ext[0][ext_path]
        value_list = [ex[ext_path] for ex in ext[0]["extension"] if ext_path in ex.keys()]
        if value_list:
            return value_list[0]
        return None
    
    def get_field_from_patient_data(fields: list[str], return_list: bool = False):
        if not fields:
            return None
        if isinstance(fields, str):
            fields = [fields]
        data = patient_data
        for index, field in enumerate(fields):
            if field in data:
                data = data[field]
                if isinstance(data, list):
                    if index+1 == len(fields) and return_list:
                        return data
                    data = data[0]
            else:
                return None
        return data
    
    us_core_race = get_extension_field("valueString", "us-core-race")
    us_core_ethnicity = get_extension_field("valueString", "us-core-ethnicity")
    mothers_maiden_name = get_extension_field("valueString", "mothersMaidenName")
    us_core_birthsex = get_extension_field("valueCode", "us-core-birthsex")
    birthplace_data = get_extension_field("valueAddress", "birthPlace")
    birthplace_city = birthplace_data["city"]
    birthplace_state = birthplace_data["state"]
    birthplace_country = birthplace_data["country"]
    disability_adjusted_life_years = get_extension_field("valueDecimal", "disability-adjusted-life-years")
    quality_adjusted_life_years = get_extension_field("valueDecimal", "quality-adjusted-life-years")
    name_use = get_field_from_patient_data(["name", "use"])
    name_family = get_field_from_patient_data(["name", "family"])
    name_given = get_field_from_patient_data(["name","given"])
    name_prefix = get_field_from_patient_data(["name","prefix"])
    telecom_system = get_field_from_patient_data(["telecom","system"])
    telephone_value = get_field_from_patient_data(["telecom","value"])
    telecom_use = get_field_from_patient_data(["telecom","use"])
    gender = get_field_from_patient_data("gender")
    birthdate = get_field_from_patient_data("birthDate")
    deceasedDateTime = get_field_from_patient_data("deceasedDateTime")
    lat_long_data = get_field_from_patient_data(["address","extension","extension"], return_list=True)
    address_lat = [ll["valueDecimal"] for ll in lat_long_data if ll["url"] == "latitude"][0]
    address_long = [ll["valueDecimal"] for ll in lat_long_data if ll["url"] == "longitude"][0]
    address_line = get_field_from_patient_data(["address","line"])
    address_city = get_field_from_patient_data(["address","city"])
    address_state = get_field_from_patient_data(["address","state"])
    address_country = get_field_from_patient_data(["address","country"])
    marital_status = get_field_from_patient_data(["maritalStatus","text"])
    multiple_birth = get_field_from_patient_data(["multipleBirthBoolean"])
    language = get_field_from_patient_data(["communication","language","text"])
    



if __name__ == "__main__":
    insert_patient_json_data("data")