#!/bin/bash

echo "Checking Database Revision"

python database_change_log.py

echo "Database change log created"
echo "Moving old database revision to dev folder"

mv 'active_subscriber_data_merged.csv' 'dev/active_subscriber_data_merged_rev1.csv'

echo "Saving new changes to the database"
mv 'active_subscriber_data_merged_temp.csv' 'active_subscriber_data_merged.csv'
