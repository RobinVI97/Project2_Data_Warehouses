# Documentation

### Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
Sparkify is an innovative and growing start-up. Since there user base and song database is growing it is crucial to build a stable Cloud Data Warehouse in which they are able to properly extract data from S3, transform this data for analytical purposes and load this data so it can be used by the analytics team. Goal is get more insights in customer listening behaviour and give more relevant recommendations 

### State and justify your database schema design and ETL pipeline.
##### Database Schema design: 
There is one fact table: songplays 
There are 4 dimensions tables: users, songs, artists and time

###### ETL Pipeline
On S3 JSON files are being placed in two main folders, namely song_data and log_data. Using Redshift  the data is being loaded into a Data Warehouse using staging tables. To make sure that the data can be used by the analytical department the data is being transformed and 4 dimension tables are being loaded in the Redshift Data Warehouse

