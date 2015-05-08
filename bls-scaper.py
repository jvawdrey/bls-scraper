#!/usr/bin/env python

"""
Python script to scrape state level unemployment and consumer price index data
from Bureau of Labor Statistics ftp (http://download.bls.gov/pub/time.series/)
and ingest data into Postgres/Greenplum/HAWQ database

Link
http://download.bls.gov/pub/time.series/la/

To Do
* extract config
* add parsing
* dynamic micro batch
"""

# 3rd party dependencies
import psycopg2     # postgres client
import sys          # System-specific parameters and function
import requests     # Python HTTP for Humans
import csv          # CSV File Reading and Writing

__author__ = "Jarrod Vawdrey (jjvawdrey@gmail.com)"
__version__ = "0.0.2"
__status__ = "Development"

# postgres/greenplum/hawq connection
DBNAME="postgres"                       # database name
DBUSER="postgres"                       # database username
DBPASSWORD="postgres"                   # database password
DBHOST="localhost"                      # database host
DBPORT="5432"                           # database port
MICROBATCHSIZE=20000                    # size of insert batch

# postgres table information
TABLE_NAME="public.bls_unemployment_stats"   # schema.table to insert bls data
COL_NAMES=['state','series_id','year','period','month','value']
COL_TYPES=['text','text','text','text','integer','numeric(10,2)']

# link
link = 'http://download.bls.gov/pub/time.series/la/'

# array of state lookups
state_array = [
  "la.data.11.California"
 ,"la.data.10.Arkansas"
 ,"la.data.12.Colorado"
 ,"la.data.13.Connecticut"
 ,"la.data.14.Delaware"
 ,"la.data.15.DC"
 ,"la.data.16.Florida"
 ,"la.data.17.Georgia"
 ,"la.data.18.Hawaii"
 ,"la.data.19.Idaho"
 ,"la.data.20.Illinois"
 ,"la.data.21.Indiana"
 ,"la.data.22.Iowa"
 ,"la.data.23.Kansas"
 ,"la.data.24.Kentucky"
 ,"la.data.25.Louisiana"
 ,"la.data.26.Maine"
 ,"la.data.27.Maryland"
 ,"la.data.28.Massachusetts"
 ,"la.data.29.Michigan"
 ,"la.data.30.Minnesota"
 ,"la.data.31.Mississippi"
 ,"la.data.32.Missouri"
 ,"la.data.33.Montana"
 ,"la.data.34.Nebraska"
 ,"la.data.35.Nevada"
 ,"la.data.36.NewHampshire"
 ,"la.data.37.NewJersey"
 ,"la.data.38.NewMexico"
 ,"la.data.39.NewYork"
 ,"la.data.40.NorthCarolina"
 ,"la.data.41.NorthDakota"
 ,"la.data.42.Ohio"
 ,"la.data.43.Oklahoma"
 ,"la.data.44.Oregon"
 ,"la.data.45.Pennsylvania"
 ,"la.data.46.PuertoRico"
 ,"la.data.47.RhodeIsland"
 ,"la.data.48.SouthCarolina"
 ,"la.data.49.SouthDakota"
 ,"la.data.50.Tennessee"
 ,"la.data.51.Texas"
 ,"la.data.52.Utah"
 ,"la.data.53.Vermont"
 ,"la.data.54.Virginia"
 ,"la.data.56.Washington"
 ,"la.data.57.WestVirginia"
 ,"la.data.58.Wisconsin"
 ,"la.data.59.Wyoming"
 ,"la.data.7.Alabama"
 ,"la.data.8.Alaska"
 ,"la.data.9.Arizona"
]

# connect to database (database name, database user, database password, database host, databse port)
def connect_db(dbname, dbuser, dbpassword, dbhost, dbport):
    try:
        conn = psycopg2.connect("dbname=" + dbname + " user=" + dbuser + " host=" + dbhost + " password=" + dbpassword + " port=" + dbport)
        cur = conn.cursor()
        e = None
        print "Connected to database " + dbname
        return (conn, cur, e)
    except psycopg2.Error as e:
        conn = None
        cur = None
        print "Unable to connect to database " + dbname
        return (conn, cur, e.diag.message_primary)

# disconnect from database
def disconnect_db(connection, cursor):
    try:
        if connection:
            connection.rollback()
        cursor.close()
        connection.close()
        print "Disconnected from database"
    except:
        print "Error disconnecting from database"

# drop database table
def dropTable(connection, cursor, tableName):
    try:
        cursor.execute('DROP TABLE IF EXISTS ' + tableName)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

# create database table
def createTable(connection, cursor, tableName, cols, colTypes):
    # number of columns
    N = len(cols)
    if (N ==0):
        return("Error no columns found")

    # build create table string
    sqlString = "CREATE TABLE " + tableName + " (%s)" % ", ".join("%s %s" % (n,t) for n,t in zip(cols, colTypes))

    try:
        # execute and commit query
        cursor.execute(sqlString)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

def insertRecords(connection, cursor, tableName, data, state_array):

    print len(data)

    # batch insert string
    for i in range(1, len(data)):
        if(i == 1):
            str = "('" + state_array + "'," + filterParseString(data[i]) + ")"
        else:
            str = str + ",('" + state_array + "'," + filterParseString(data[i]) + ")"

        if (i % 20000) == 0:

            sqlString = "INSERT INTO " + tableName + " VALUES " + str

            str = " "

            try:
                # execute and commit query
                cursor.execute(sqlString)
                connection.commit()
            except psycopg2.Error as e:
                return(e.diag.message_primary)

    # load any remaining
    if (str != " "):
        sqlString = "INSERT INTO " + tableName + " VALUES " + str
        try:
            # execute and commit query
            cursor.execute(sqlString)
            connection.commit()
        except psycopg2.Error as e:
            return(e.diag.message_primary)

    return(None)

def filterParseString(str):
    return("'" + str[0:20] + "','" + str[31:35] +
           "','" + str[36:39] +"'," + str[37:39] +
           "," + str[49:52])

# main driver (calls above functions in sequence)
def main():

    # Connect to database
    conn, cur, e = connect_db(DBNAME, DBUSER, DBPASSWORD, DBHOST, DBPORT)
    # If database connection not made exit
    if (e is not None):
        print "Exiting: Unable to connect to database \n" + e
        sys.exit()

    # drop table if exists
    e = dropTable(conn, cur, TABLE_NAME);
    # If error with dropping table then exit
    if (e is not None):
        print "Exiting: Unable to drop table \n" + e
        disconnect_db(conn, cur)
        sys.exit()

    # create table
    e = createTable(conn, cur, TABLE_NAME, COL_NAMES, COL_TYPES);
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Unable to create table \n" + e
        disconnect_db(conn, cur)
        sys.exit()

    # for each state
    for i in range(0, len(state_array)):

        print state_array[i]

        # grab data from link
        r = requests.get(link + state_array[i])
        data = r.text.splitlines()

        # insert lines into db
        e = insertRecords(conn, cur, TABLE_NAME, data, state_array[i])
        # If error with inserting data then exit
        if (e is not None):
            print "Exiting: Unable to insert records into table \n" + e
            disconnect_db(conn, cur)
            sys.exit()

    # Disconnect from database
    disconnect_db(conn, cur)

    # system exit
    sys.exit()

# call driver
main()
