#!/usr/bin/python3
# pip3 install psycopg2-binary

#for v2
#import psycopg2
#from psycopg2.pool import ThreadedConnectionPool

#for v3
import psycopg
from psycopg.pool import ConnectionPool

import random
import string
import uuid
import datetime 
from time import sleep
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from concurrent import futures
import concurrent
import time
import logging
import socket
import os
import argparse
import math

parser = argparse.ArgumentParser()
parser.add_argument("command", help="initschema, initmain, initperf, initchild")
parser.add_argument("--dsn", help="hostname to use to connect to the db, ex. postgres://user:pass@host:port/database?option=value")
parser.add_argument("--threads", type=int, default=1, help="how many threads should be started? default:1")
parser.add_argument("--insertsize", type=int, default=100, help="number of inserts to do in a single query; default=100")
parser.add_argument("--accounts", type=int, default=2000000, help="number of initial accounts to create; default=2M")
parser.add_argument("--cards", type=int, default=4000000, help="number of initial credit cards to create; default=4M")
parser.add_argument("--xrefs", type=int, default=8000000, help="number of initial accounts to create; default=8M")


args = parser.parse_args()

#remove all of the DB information that was previously setup
def dropitall(tcp):
    myquery = "USE system; DROP database defaultdb cascade; create database defaultdb;"
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    try:    
        cursor.execute(str(myquery))
    except Exception as err:
        print("ERROR: " + str(myquery) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    teardown(cursor, conn, tcp)

#tear down a connect and return it to the pool
def teardown(cursor, conn, tcp):
    try:
        cursor
    except NameError:
        pass
    else:
        cursor.close()

    try:
        conn
    except NameError:
        pass
    else:
        tcp.putconn(conn)

#create tables
def mk_tables(tcp):
    f = open("schema.sql", "r")
    schema = f.read()
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    try:    
        cursor.execute(str(schema))
    except Exception as err:
        print("ERROR: " + str(schema) + " " + str(err))
        conn.rollback()
    else:       
        conn.commit()
    f.close()
    teardown(cursor, conn, tcp)

#create the initial data in the parent table, spawning threads
def mk_data(pool, tcp):
    #start up a bunch of threads each doing a subset of the inserts into the account and cards tables
    futures = []
    subcount = math.ceil(args.accounts / args.threads)
    for x in range(1, args.threads+1):
        futures.append(pool.submit(insert_account_data, tcp, subcount, x))
        mythread = futures[-1]
        mythread.add_done_callback(idone)
    for _ in concurrent.futures.as_completed(futures):
        #wait for the threads to finish
        pass

    futures = []
    subcount = math.ceil(args.cards / args.threads)
    for x in range(1, args.threads+1):
        futures.append(pool.submit(insert_card_data, tcp, subcount, x))
        mythread = futures[-1]
        mythread.add_done_callback(idone)
    for _ in concurrent.futures.as_completed(futures):
        #wait for the threads to finish
        pass 

#this is my basic sql executor that does retries (up to 5) with the random pause in the case of a retry
def run_sql(cursor, sql, values="", retries=0):
    LIMIT_RETRIES = 5
    try:
        if values:
            cursor.execute(sql, values, prepare=True)
        else:
            cursor.execute(sql, prepare=True)
        retries = 0
    except (psycopg.OperationalError, psycopg.DatabaseError) as error:
        if retries >= LIMIT_RETRIES:
            raise error
        else:
            retries += 1
            rnd = random.randint(1,100)
            delay = retries/10 + rnd/1000
            sleep(delay)
            run_sql(cursor, sql, values, retries)
    return cursor

#set the replication values to what is desired
def mk_replication(tcp):
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()

    myquery = "ALTER DATABASE defaultdb CONFIGURE ZONE USING num_replicas = 5;"

    cursor = run_sql(cursor, myquery)
    teardown(cursor, conn, tcp)

#use subqueries to insert values into the cross reference table.
def xref_table(tcp):
    batchsize = 10000;

    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    inserts = math.ceil(args.xrefs / batchsize)

    myquery = "SET experimental_enable_temp_tables=on;"
    cursor = run_sql(cursor, myquery)
    myquery = "create temp table a (row int, account_id UUID PRIMARY KEY);"
    cursor = run_sql(cursor, myquery)
    myquery = "create temp table c (row int, card_id UUID PRIMARY KEY);"
    cursor = run_sql(cursor, myquery)

    for _ in range(1,inserts+1):
        #myquery = "insert into xref (account_id, card_id, association_type) select account_id, card_id, association_type from account, credit_card, association order by random() limit 10000;"
        
        myquery = "insert into a select row_number() over (), * from (select account_id from account order by random() limit 10000);"
        cursor = run_sql(cursor, myquery)
        myquery = "insert into c select row_number() over (), * from (select card_id from credit_card order by random() limit 10000)"
        cursor = run_sql(cursor, myquery)
        myquery = "insert into xref (account_id, card_id) select account_id, card_id from a inner join c on a.row = c.row;"
        cursor = run_sql(cursor, myquery)
        myquery = "truncate table a cascade;"
        cursor = run_sql(cursor, myquery)
        myquery = "truncate table c cascade;"
        cursor = run_sql(cursor, myquery)

    teardown(cursor, conn, tcp)

#given a sql filename, get the queries from it so they can be used later in the workflow
def get_sqllines(file):
    sqllines = []
    with open(file, "r") as f:
        for line in f:
            sqllines.append(line.strip())
    f.close()
    return sqllines

#This creates a subset of data in the account table.  It is called once per thread.
def insert_account_data(tcp, numvalues, x):
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    insertsize = args.insertsize
    inserts = math.ceil(numvalues/insertsize)

    for _ in range(1,inserts+1):
        myquery = "INSERT INTO account (account_ref_number) VALUES "
        for i in range (1, insertsize+1):
            a = str(get_num())
            myquery = myquery + "(" + a + ")"
            if (i != insertsize):
                myquery = myquery + ", "
        myquery = myquery + ";"
        cursor = run_sql(cursor, myquery)

    teardown(cursor, conn, tcp)

def insert_card_data(tcp, numvalues, x):
    conn = tcp.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    insertsize = args.insertsize
    inserts = math.ceil(numvalues/insertsize)

    for _ in range(1,inserts+1):
        myquery = "INSERT INTO credit_card (card_number) VALUES "
        for i in range (1, insertsize+1):
            a = str(get_num())
            myquery = myquery + "(" + a + ")"
            if (i != insertsize):
                myquery = myquery + ", "
        myquery = myquery + ";"
        cursor = run_sql(cursor, myquery)

    teardown(cursor, conn, tcp)

#generic thread completion callback handler
def idone(fn):
    if fn.cancelled():
        print("{}: canceled")
    if fn.done():
        error = fn.exception()
        if error:
            print(str(fn) + ": error returned: " + str(error))
        else:
            pass

#generate a random 16 character string
def get_string():
    return ''.join(random.sample(string.ascii_letters, 16))

#generate a random 16 character number
def get_num():
    return random.randint(1,9999999999999999)

#main is well... main
def main():
    starttime = datetime.datetime.now()
    #make sure we received at least a dsn, fail if we didn't because there is nothing to do
    if (not args.dsn):
        print("At minimum, a dsn needs to be provided.  Please see help.")
        return 1

    #for psycopg3
    tcp = ConnectionPool(conninfo=args.dsn, max_size=args.threads+8)

    #for psycopg2
    #tcp = ThreadedConnectionPool(1, args.threads +8, args.dsn)

    #check for various commands to be specified
    if (args.command == "initschema"):
        mk_tables(tcp)
    elif (args.command == "initmain"):
        print("Making data in main tables")
        pool = ThreadPoolExecutor(args.threads+4)
        mk_data(pool, tcp)
        pool.shutdown(wait=True)
        #insert_account_data(tcp, 5000, 1)
    elif (args.command == "initxref"):
        print("Making xref tables")
        xref_table(tcp)
    elif (args.command == "replication"):
        print("Setting replication")
        mk_replication(tcp);
    elif (args.command == "dropitall"):
        dropitall(tcp)
    else:
        print("I don't know what to do.  This command did nothing.")

    #print out the runtime of the work done when complete
    print("Runtime of " + str(datetime.datetime.now() - starttime))

if __name__ == "__main__":
    main()