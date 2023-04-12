import psycopg2
import os

USER = os.getenv('pgadmin_user')
PASSWORD = os.environ.get('pgadmin_password')


def connect_to_postgres():
    return psycopg2.connect(
            host="localhost", database="postgres", user="postgres", password=PASSWORD
        )
    

def prepare_data_tables():
    con = connect_to_postgres()
    cursor = con.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS patient_data;")
    cursor.execute("CREATE TABLE patient_data.patient(id varchar(255) NOT NULL UNIQUE PRIMARY KEY, \
                   us_core_race VARCHAR(255), us_core_ethnicity VARCHAR(255), \
                   mothers_maiden_name VARCHAR(255), us_core_birthsex VARCHAR(1), \
                   birthplace_city VARCHAR(255), birthplace_state VARCHAR(255), \
                   birthplace_country VARCHAR(255), disability_adjusted_life_years FLOAT, \
                   quality_adjusted_life_years FLOAT, name_use VARCHAR(255), \
                   name_family VARCHAR(255), name_given VARCHAR(255), \
                   name_prefix VARCHAR(255), telecom_system VARCHAR(255), \
                   telephone_value VARCHAR(255), telecom_use VARCHAR(255), gender VARCHAR(1), \
                   birthdate DATE, deceasedDateTime TIMESTAMP, address_lat FLOAT, \
                   address_long FLOAT, address_line VARCHAR(255), address_city VARCHAR(255), \
                   address_state VARCHAR(255), address_country VARCHAR(255), \
                   marital_status VARCHAR(1), multiple_birth BOOL, language VARCHAR(255))")
    cursor.execute("CREATE TABLE patient_data.patient_identifier(identifier_id SERIAL, \
                                                                patient_id VARCHAR(255) NOT NULL, \
                                                                identifier_type varchar(255) NOT NULL, \
                                                                identifier_value VARCHAR(255) NOT NULL, \
                                                                PRIMARY KEY (identifier_id), \
                                                                FOREIGN KEY(patient_id) REFERENCES patient(id))")
    con.commit()
    cursor.close()
    con.close()


if __name__ == "__main__":
    prepare_data_tables()