import pandas as pd
import numpy as np
import os
import sqlite3
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(filename='change_log.log', encoding='utf-8', level=logging.DEBUG, format='')
logger.info('DETAILED SUMMARY OF DATABASE CHANGES')
database_path = 'cademycode_subscriber_data.db'
if os.path.isfile(database_path):
	connection = sqlite3.connect(database_path)
else:
	raise NameError("database does not exist.")

cursor = connection.cursor()

# create new dataframes with current database
students_df = pd.read_sql_query("""SELECT * FROM codecademy_students""", connection)
students_df['job_id'] = students_df['job_id'].fillna(0.0)
students_df['job_id'] = students_df['job_id'].astype('float')
students_df['job_id'] = students_df['job_id'].astype('int')

students_df['name'] = students_df['name'].astype('str')
students_df['dob'] = pd.to_datetime(students_df['dob'])

students_df['num_course_taken'] = students_df['num_course_taken'].fillna(-1)
students_df['num_course_taken'] = students_df['num_course_taken'].astype('float')
students_df['num_course_taken'] = students_df['num_course_taken'].astype('int8')

students_df['current_career_path_id'] = students_df['current_career_path_id'].fillna(-1)
students_df['current_career_path_id'] = students_df['current_career_path_id'].astype('float')
students_df['current_career_path_id'] = students_df['current_career_path_id'].astype('int8')

students_df['time_spent_hrs'] = students_df['time_spent_hrs'].astype('float32')

courses_df = pd.read_sql_query("""SELECT * FROM codecademy_courses""", connection)
courses_df = courses_df.drop_duplicates()
courses_df.career_path_id = courses_df.career_path_id.astype('int8')
courses_df.hours_to_complete = courses_df.hours_to_complete.astype('int8')

subscriber_data_merged_df = pd.merge(students_df, courses_df, left_on='current_career_path_id', right_on='career_path_id', how='left').drop('career_path_id', axis=1)

jobs_df = pd.read_sql_query("""SELECT * FROM codecademy_subscriber_jobs""", connection)
jobs_df = jobs_df.drop_duplicates()
jobs_df.job_id = jobs_df.job_id.astype('int8')
jobs_df.avg_salary = jobs_df.avg_salary.astype('int32')
subscriber_data_merged_df = pd.merge(subscriber_data_merged_df, jobs_df, left_on='job_id', right_on='job_id', how='left')
subscriber_data_merged_df = subscriber_data_merged_df.drop(['index_x', 'index_y', 'index'], axis=1)
# load up existing csv file
existing_database = pd.read_csv('active_subscriber_data_merged.csv')
# check schema to see if new columns have been added
old_columns = set(existing_database.columns)
new_columns = set(existing_database.columns)
added_columns = list(new_columns - old_columns)
if len(added_columns) > 0:
	logger.info('--------COLUMNS ADDED--------')
	for col in added_columns:
		logger.info("+ new field called ", col)
# check rows to see new users that have been added
new_rows = existing_database.merge(subscriber_data_merged_df, on='uuid', how='outer', suffixes=['', '_'], indicator=True)
added_rows = new_rows[new_rows._merge == 'right_only']
added_rows = added_rows[['uuid', 'name']]
removed_rows = new_rows[new_rows._merge == 'left_only']
removed_rows = removed_rows[['uuid', 'name']]

# create change log with new updates to database
if len(added_rows) > 0:
	logger.info('--------ADDED ROWS--------')
	logger.info(added_rows)
if len(removed_rows) > 0:
	logger.info('-------REMOVED ROWS-------')
	logger.info(removed_rows)
# update revision number of old database and save in archives folder

# save new dataframe in place of most current data
subscriber_data_merged_df.to_csv('active_subscriber_data_merged_temp.csv', index=False) 

connection.close()