#!/usr/bin/python2.7
#
# Assignment for Implementing RangeQuery and PointQuery
#
#author: Naveen Reddy Kontham: knreddy1@asu.edu

import psycopg2
import os
import sys


#
# Implementation:Implement a Python function PointQuery that takes as input: (1) Ratings table stored in PostgreSQL, (2) RatingValue. (3) openconnection
# Return: The returned tuples should be stored in a text file, named RangeQueryOut.txt (in the same directory where Interface.py is present)
# such that each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e.
# RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides
#


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):

    finalResult = []
    cursor = openconnection.cursor()

    partitionQuery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'''.format(ratingMinValue, ratingMaxValue)
    cursor.execute(partitionQuery)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]

    rangeselectquery = '''SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'''

    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0,'RangeRatingsPart{}'.format(partition))
            finalResult.append(res)

    rrcountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''
    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    rrselectquery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating>={1} and rating<={2};'''

    for i in xrange(0,rrparts):
        cursor.execute(rrselectquery.format(i, ratingMinValue, ratingMaxValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            finalResult.append(res)

    writeToFile('RangeQueryOut.txt', finalResult)

#
# Implementation: Implement a Python function RangeQuery that takes as 
# input: (1) Ratings table stored in PostgreSQL, (2) RatingMinValue (3) RatingMaxValue (4) openconnection.
#
# Return: # Return: he returned tuples should be stored in a text file, named PointQueryOut.txt (in the same directory where Interface.py is present) 
# such that each line represents a tuple that has the following format such that PartitionName represents the full name of the partition i.e. 
# RangeRatingsPart1 or RoundRobinRatingsPart4 etc. in which this tuple resides.
#


def PointQuery(ratingsTableName, ratingValue, openconnection):
    finalResult = []
    cursor = openconnection.cursor()

    partitionQuery = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={0};'''.format(ratingValue)
    cursor.execute(partitionQuery)
    partitions = cursor.fetchall()
    partitions = [partition[0] for partition in partitions]

    rangeselectquery = '''SELECT * FROM rangeratingspart{0} WHERE rating={1};'''

    for partition in partitions:
        cursor.execute(rangeselectquery.format(partition, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RangeRatingsPart{}'.format(partition))
            finalResult.append(res)

    rrcountquery = '''SELECT partitionnum FROM roundrobinratingsmetadata;'''

    cursor.execute(rrcountquery)
    rrparts = cursor.fetchall()[0][0]

    rrselectquery = '''SELECT * FROM roundrobinratingspart{0} WHERE rating={1};'''

    for i in xrange(0, rrparts):
        cursor.execute(rrselectquery.format(i, ratingValue))
        sqlresult = cursor.fetchall()
        for res in sqlresult:
            res = list(res)
            res.insert(0, 'RoundRobinRatingsPart{}'.format(i))
            finalResult.append(res)

    writeToFile('PointQueryOut.txt', finalResult)


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
