#!/usr/bin/python2.7
#
# Interface for LoadRatings, rangePartition, roundRobinPartition, roundrobininsert,
# rangeinsert
#
# author: Naveen Reddy Kontham: knreddy1@asu.edu

import psycopg2
from itertools import islice
from StringIO import StringIO


#Change the names Once you are done with the testing 

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


# 
# Implement a Python function Load Ratings() that takes a file system path that contains the rating.dat 
# file as input. Load Ratings() then load the rating.dat content into a table (saved in PostgreSQL) 
# named Ratings that has the following schema.
#

def loadRatings(ratingstablename, ratingsfilepath, openconnection):

    print('the Table name is ', ratingstablename)
    cursor = openconnection.cursor()
    cursor.execute("DROP TABLE IF EXISTS " + ratingstablename)
    cursor.execute("CREATE TABLE " + ratingstablename + "(userid int not null, movieid int, rating real, timestamp int);")
    with open(ratingsfilepath) as f:
        for nlines in iter(lambda: tuple(islice(f, 5000)), ()):
            batch = StringIO()
            batch.write(''.join(l.replace('::', ',') for l in nlines))
            batch.seek(0)
            cursor.copy_from(batch, ratingstablename, sep=',',columns=('userid','movieid','rating','timestamp'))
            #Dropping the time stamp  as only userid, movieid, rating are required
    cursor.execute("ALTER TABLE " + ratingstablename + " DROP timestamp")
    openconnection.commit()
    cursor.close()

#
# Implement a Python function Range_Partition() that takes as input: (1) the Ratings table stored 
# in PostgreSQL and (2)  an integer value N; that represents the number of partitions. 
# Range_Partition() then generates N horizontal fragments of the Ratings table and store them in PostgreSQL.
#  The algorithm should partition the ratings table based on N uniform ranges of the Rating attribute.
#


def rangePartition(ratingstablename, numberofpartitions, openconnection):

    #hardcoded 5 as there are ratings from 0 to 5 and no global variables can be created
    stepsize = 5.0/numberofpartitions
    createpart_init = 'CREATE TABLE range_part{0} AS SELECT * FROM RATINGS WHERE rating>={1} and rating<={2}'
    createpart = 'CREATE TABLE range_part{0} AS SELECT * FROM RATINGS WHERE rating>{1} and rating<={2}'
    cursor = openconnection.cursor()
    for i in xrange(numberofpartitions):
        if i == 0:
            cursor.execute(createpart_init.format(i, i*stepsize, (i+1)*stepsize))
        else:
            cursor.execute(createpart.format(i, i * stepsize, (i + 1) * stepsize))
    cursor.close()

#
# Implement a Python function RoundRobin_Partition() that takes as input: (1) the Ratings table stored in 
# PostgreSQL and (2) an integer value N; that represents the number of partitions. 
# The function then generates N horizontal fragments of the Ratings table and stores them in PostgreSQL. 
# The algorithm should partition the ratings table using the round robin partitioning approach 
# 

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):

    createrr = '''
    CREATE TABLE rrobin_part{0} AS 
    SELECT userid,movieid,rating 
    FROM (SELECT userid, movieid, rating, ROW_NUMBER() OVER() as rowid FROM {1}) AS temp
    WHERE mod(temp.rowid-1,{2}) = {3}'''

    cursor = openconnection.cursor()
    for i in xrange(numberofpartitions):
        cursor.execute(createrr.format(i, ratingstablename, numberofpartitions, i))
    cursor.close()

#
# Implement a Python function RoundRobin_Insert() that takes as input: (1) Ratings table stored in 
# PostgreSQL, (2) UserID, (3) ItemID, (4) Rating. RoundRobin_Insert() then inserts a new tuple to the 
# Ratings table and the right fragment based on the round robin approach.
#

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    cursor = openconnection.cursor()
    
    cursor.execute('''INSERT INTO {0} VALUES ({1},{2},{3})'''.format(ratingstablename, userid, itemid, rating))
    
    cursor.execute('''SELECT * FROM {0} '''.format(ratingstablename))
    numrecords = len(cursor.fetchall())

    print('no of records is', numrecords)
    
    cursor.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%' ''')
    
    numparts = len(cursor.fetchall())
    print('no of parts is ', numparts)

    tbid = (numrecords-1)%numparts

    cursor.execute('''INSERT INTO rrobin_part{0} VALUES ({1},{2},{3})'''.format(tbid, userid, itemid, rating))
    cursor.close()

#
# Implement a Python function Range_Insert() that takes as input: (1) Ratings table stored in 
# Post- greSQL (2) UserID, (3) ItemID, (4) Rating. Range_Insert() then inserts a new tuple to the 
# Ratings table and the correct fragment (of the partitioned ratings table) based upon the Rating value.
#

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    
    cursor = openconnection.cursor()
    cursor.execute('''INSERT INTO {0} VALUES ({1},{2},{3})'''.format(ratingstablename, userid, itemid, rating))
    
    cursor.execute('''SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_part%' ''')
    numparts = len(cursor.fetchall())

    insertrng = '''
        INSERT INTO range_part{0} VALUES ({1},{2},{3})
        '''

    stepsize = 5.0/numparts

    for i in xrange(numparts):
        if i==0:
            if rating>=i*stepsize and rating<=(i+1)*stepsize:
                cursor.execute(insertrng.format(i, userid, itemid, rating))
        else:
            if rating>i*stepsize and rating<=(i+1)*stepsize:
                cursor.execute(insertrng.format(i, userid, itemid, rating))
    cursor.close()


 #
 # Creation of DB
 #    

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

#
# Deleting the partitions and exiting
#

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

#
# delete tables
#

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()

