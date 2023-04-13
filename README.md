How to install and run:

Summary:
- A 'patient_data' schema is created in Postgres
- Empty tables are created for patient and patient identifier information
- The JSON files are read in and the relevant information pulled from each record
- The retrieved information is inserted, in bulk, into the postgres tables

Run the following command: python -m run_full_process

How to see and use the results:
- Results are held in the 'patient_data' schema in postgres. The table 'patient' contains most of the information on each individual, such as name, address, contact information and identifying information, such as, social security number, passport number are held in the 'patient_identifer' table.
- With more time, you could hook the database up to a tool, such as PowerBI, to get a better overview of the data held.

Architecture:
- Data is provided by FHIR and downloaded as a JSON file
- Data is picked up and transformed in Python
- Data is inserted into Postgres

Next steps:
- Pull list into their own tables (due to time constraints, just retrieved first element of list for this exercise) - for example, patient.prefix is provided as a list so this should be held in a seperate table with a foreign key referencing the patient id
- Add any sensitive or PII data to its own table with more restrictive permissions so it is only accessible when necessary
- Put data into tables based on categories - for example, for address data, this should be in it's own table with an address_id that is referenced by the patient table as a foreign key. For address data, it is helpful to standardise using a software like AFD PAF solution, this allows us to receive address data that is not identical but references the same location and resolve it to one ID, especially as you will have a many-to-one mapping from patient to address
- Create the remaining tables for the different resourceTypes - for example, Claims, Encounters, Conditions - some of these have many-to-many mappings to other tables so we would need to create a join table to have two sets of many-to-one relationships instead.
- With more time, I would have liked to have properly defined and documented the scope of the project before starting. The Synthea documentation referenced would have been a great starting point to design a relational database diagram, include all fields and more accurately define the data types.
- Use docker to containerise
