import connect_postgres
import extract_patient_data
import logging

logger = logging.getLogger("Inserting Patient Data into Postgres")

#connect_postgres.drop_tables()
connect_postgres.prepare_data_tables()
extract_patient_data.insert_patient_json_data("data")

logger.info("All done!")