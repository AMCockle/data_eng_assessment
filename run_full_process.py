import connect_postgres
import extract_patient_data

connect_postgres.prepare_data_tables()
extract_patient_data.insert_patient_json_data()