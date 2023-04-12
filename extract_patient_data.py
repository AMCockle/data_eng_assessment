from connect_postgres import insert_bulk_patient_data, insert_bulk_identifier_data
import os
import json


def insert_patient_json_data(path):
    """
    Extract all relevant data from the patient JSON records and insert into
    Postgres in bulk
    """
    # Pull only Json files from file path provided
    files = [path + "/" + file for file in os.listdir(path) if file.endswith(".json")]
    bulk_patient_data_insert = []
    bulk_patient_identifier_insert = []
    for file in files:
        with open(file, "r", encoding="utf8") as patient_data_json:
            # Load Json data
            patient_all_data = json.load(patient_data_json)
            entries = patient_all_data["entry"]
            patient_data = [entry["resource"] for entry in entries if entry["resource"]["resourceType"]=="Patient"][0]
            # Extract information for patient table
            patient_information = get_patient_data(patient_data)
            if patient_information:
                bulk_patient_data_insert.append(patient_information)
            # Extract information for patient identifier table
            identifier_information = get_patient_identifier_data(patient_data)
            if identifier_information:
                bulk_patient_identifier_insert.extend(identifier_information)

    # Run bulk inserts
    insert_bulk_patient_data(bulk_patient_data_insert)
    insert_bulk_identifier_data(bulk_patient_identifier_insert)


            
def get_patient_data(patient_data):
    """
    Get all data required for the patient table
    Some fields are not always available so return None in these cases
    """
    id = patient_data["id"]
    extension = patient_data["extension"]


    def get_extension_field(ext_path, field_name):
        """
        Get nested fields from extension data  
        """
        ext = [ex for ex in extension if ex["url"].endswith(field_name)]
        if ext_path in ext[0]:
            return ext[0][ext_path]
        value_list = [ex[ext_path] for ex in ext[0]["extension"] if ext_path in ex.keys()]
        if value_list:
            return value_list[0]
        return None
    

    def get_field_from_patient_data(fields: list[str], return_list: bool = False):
        """
        Pull nested fields from patient data with option to return a list or first element 
        """
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
    
    return (id,us_core_race,us_core_ethnicity,mothers_maiden_name,us_core_birthsex,birthplace_city,
            birthplace_state,birthplace_country,disability_adjusted_life_years,
            quality_adjusted_life_years,name_use,name_family,name_given,name_prefix,
            telecom_system,telephone_value,telecom_use,gender,birthdate,deceasedDateTime,
            address_lat,address_long,address_line,address_city,address_state,address_country,
            marital_status,multiple_birth,language)


def get_patient_identifier_data(patient_data):
    """
    Get just the identifier data - could be many different values
    """
    identifier_data_records = patient_data["identifier"]
    identifier_information = []
    for record in identifier_data_records:
        if not "type" in record:
            continue
        identifier_information.append((patient_data["id"],
                                       record["type"]["coding"][0]["display"],
                                       record["value"]))
    return identifier_information


if __name__ == "__main__":
    insert_patient_json_data("data")