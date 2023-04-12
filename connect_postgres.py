import psycopg2
import os

USER = os.getenv('pgadmin_user')
PASSWORD = os.environ.get('pgadmin_password')


def connect_to_postgres():
    return psycopg2.connect(
            host="localhost", database="postgres", user=USER, password=PASSWORD
        )
    

def prepare_data_tables():
    """
    Create a new schema for our patient data (if it doesn't already exist)
    and create relevant tables needed to easily display the data
    """
    con = connect_to_postgres()
    cursor = con.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS patient_data;")
    cursor.execute("CREATE TABLE IF NOT EXISTS patient_data.patient(id varchar(255) NOT NULL UNIQUE PRIMARY KEY, \
                   us_core_race VARCHAR(255), us_core_ethnicity VARCHAR(255), \
                   mothers_maiden_name VARCHAR(255), us_core_birthsex VARCHAR(255), \
                   birthplace_city VARCHAR(255), birthplace_state VARCHAR(255), \
                   birthplace_country VARCHAR(255), disability_adjusted_life_years FLOAT, \
                   quality_adjusted_life_years FLOAT, name_use VARCHAR(255), \
                   name_family VARCHAR(255), name_given VARCHAR(255), \
                   name_prefix VARCHAR(255), telecom_system VARCHAR(255), \
                   telephone_value VARCHAR(255), telecom_use VARCHAR(255), gender VARCHAR(255), \
                   birthdate DATE, deceasedDateTime TIMESTAMP, address_lat FLOAT, \
                   address_long FLOAT, address_line VARCHAR(255), address_city VARCHAR(255), \
                   address_state VARCHAR(255), address_country VARCHAR(255), \
                   marital_status VARCHAR(255), multiple_birth BOOL, language VARCHAR(255))")
    cursor.execute("CREATE TABLE IF NOT EXISTS patient_data.patient_identifier(identifier_id SERIAL, \
                                                                patient_id VARCHAR(255) NOT NULL, \
                                                                identifier_type varchar(255) NOT NULL, \
                                                                identifier_value VARCHAR(255) NOT NULL, \
                                                                PRIMARY KEY (identifier_id), \
                                                                FOREIGN KEY(patient_id) REFERENCES patient_data.patient(id) \
                                                                ON DELETE CASCADE \
                                                                ON UPDATE CASCADE)")
    con.commit()
    cursor.close()
    con.close()


def insert_bulk_patient_data(data):
    """
    Insert patient data into the patient table in bulk
    """
    con = connect_to_postgres()
    cursor = con.cursor()

    args = ','.join(cursor.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", i).decode('utf-8')
                for i in data)
    
    cursor.execute("INSERT INTO patient_data.patient(id,us_core_race,us_core_ethnicity,mothers_maiden_name,us_core_birthsex,birthplace_city,birthplace_state,birthplace_country,disability_adjusted_life_years,quality_adjusted_life_years,name_use,name_family,name_given,name_prefix,telecom_system,telephone_value,telecom_use,gender,birthdate,deceasedDateTime,address_lat,address_long,address_line,address_city,address_state,address_country,marital_status,multiple_birth,language) \
                   VALUES " + (args))

    con.commit()
    cursor.close()
    con.close()


def insert_bulk_identifier_data(data):
    """
    Insert patient identifier data into the patient identifier table in bulk
    """
    con = connect_to_postgres()
    cursor = con.cursor()

    args = ','.join(cursor.mogrify("(%s,%s,%s)", i).decode('utf-8')
                for i in data)
    
    cursor.execute("INSERT INTO patient_data.patient_identifier(patient_id, identifier_type, identifier_value) \
                   VALUES " + (args))

    con.commit()
    cursor.close()
    con.close()

if __name__ == "__main__":
    prepare_data_tables()