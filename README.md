# pginfo
A collection of python scripts for collecting information from postgres databases

# Description
The following scripts connect to a Postgresql database via Python. The scripts then collect various pieces of information, including database names, database sizes.

# Version History
## 2024-01-16
Initial commit.
Added script to get databases on a server.
Added script to get database sizes.
Added script to insert database information into the tables in dbaadmin.

## 2024-01-17
Added script to get all tables in a database.
Added script to get table size for all tables in a database.
Added script to get table usage for all tables in a database.
Added script to insert table usage information into dbaadmin.

## 2024-01-18 
Updated query for table sizes. Now includes index sizes and estimated row count.
Added script to insert table sizes into dbaadmin.

## 2024-01-30
Added script for grants
Added script to process all servers.
Added header comments.
Formatted files to python standards.

## 2024-02-07
Added script for users at the server level.
This includes rights, such as Super User, Create DB, etc.

## 2024-02-12
Added script to send emails.
Integrated send mail script into all python scripts. 