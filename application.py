#Dynamic Query and Memcache implementation on AWS
#author: Rajat

from flask import Flask, redirect, render_template, request, session
import MySQLdb
from werkzeug.utils import secure_filename
import boto
import boto.s3
from boto.s3.key import Key
import datetime
import os
import csv
import random
import memcache
import hashlib

application = Flask(__name__)
application.secret_key = 'rajat'

# AWS Credentials
AWS_ACCESS_KEY_ID = 'Enter_access_key_id'
AWS_SECRET_ACCESS_KEY = 'Enter_secret_access_key'

# Creating and connecting to AWS S3 Bucket
bucket_name = 'newBucket'
conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)

# Connecting to Memcache client
memc = memcache.Client(['Enter_memcached_host'], debug=1)

# Connecting to AWS RDS instance
def initdbConn():
    return MySQLdb.connect(host="hostname", user="username",
                           passwd="password", db="databaseName")

# Listing the files in the bucket
@application.route('/')
def home():
    list_of_folder = []

    folders = bucket.list("", "/")
    for folder in folders:
        list_of_folder.append(folder.name)

    return render_template('home.html', list_of_buckets=list_of_folder)

# Deleting the file from the bucket
@application.route('/delete', methods=['POST'])
def delete():
    file_to_delete = request.form['delete_file_name']
    k = Key(bucket)
    k.key = file_to_delete
    bucket.delete_key(k)

    return '<h1>File Deleted Successfully!</h1><br><br><br><form action="../"><input type="Submit" value="Back"></form>'

# Uploading the file to S3
@application.route('/s3upload', methods=['POST'])
def s3upload():
    file = request.files['s3upload']
    file_name = file.filename
    content = file.read()

    starttime = datetime.datetime.now()

    fo = open(file_name, "w")
    fo.write(content)

    k = Key(bucket)
    k.key = file_name
    starttime = datetime.datetime.now()
    k.set_contents_from_filename(secure_filename(file_name))
    endtime = datetime.datetime.now()
    res = endtime - starttime
    try:
        os.remove(secure_filename(file_name))
    except:
        None
    return '<h1>File uploaded Successfully!</h1><br>Time Taken = ' + str(
        res) + '<br><br><form action="../"><input type="Submit" value="Back"></form>'


# Importing csv into MySQL table
@application.route('/rdsupload', methods=['POST'])
def rdsupload():
    file_to_upload = request.files['rdsupload']
    target = file_to_upload.read()

    fo = open(file_to_upload.filename, "w")
    fo.truncate()
    fo.write(target)
    fo.close()

    file = open(file_to_upload.filename)
    csvFile = csv.reader(file)
    starttime = datetime.datetime.now()

    columns = []

    row1 = next(csvFile)

    qStr = "("

    for i in row1:
        i = i.replace(" ", "_")
        if (i.lower() == 'dec'):
            i = 'DECM'
        columns.append(i)
        qStr += (i + ' VARCHAR(50),')

    dbconn = initdbConn()
    c = dbconn.cursor()

    session['firstColumn'] = i
    qStr += "id INT AUTO_INCREMENT,column_random VARCHAR(50), PRIMARY KEY (id)"
    qStr += ')'

    try:
        queryStr = 'drop table ' + file_to_upload.filename[:-4]
        c.execute(queryStr)
        dbconn.commit()
    except:
        None

    queryStr = 'Create Table ' + file_to_upload.filename[:-4] + ' ' + qStr
    print queryStr
    c.execute(queryStr);
    dbconn.commit()

    c.close()
    dbconn.close()

    dbconn = initdbConn()
    c = dbconn.cursor()
    queryStr = "LOAD DATA LOCAL INFILE '" + file_to_upload.filename + "' INTO TABLE " + file_to_upload.filename[
                                                                                        :-4] + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES SET column_random = FLOOR(RAND()*100)"
    print queryStr
    c.execute(queryStr)
    dbconn.commit()
    endtime = datetime.datetime.now()
    c.close()
    dbconn.close()

    DBtime = endtime - starttime
    session['columns'] = columns
    session['tableName'] = file_to_upload.filename[:-4]
    try:
        os.remove(secure_filename(file_to_upload))
    except:
        None

    return "<h1>File uploaded Successfully!</h1><br>Time Taken = " + str(
        DBtime) + "<br><br><form action='/./.'><input type='Submit' value='Back'></form>"


# Uploading the file to S3 and importing it to MySQL database
@application.route('/upload_import', methods=['POST'])
def upload_import():
    file_to_upload = request.files['upload_import']
    file_name = file_to_upload.filename
    content = file_to_upload.read()

    starttime = datetime.datetime.now()

    fo = open(file_name, "w")
    fo.write(content)

    k = Key(bucket)
    k.key = file_name
    starttime = datetime.datetime.now()
    k.set_contents_from_filename(secure_filename(file_name))
    endtime = datetime.datetime.now()
    resS3 = endtime - starttime

    file = open(file_to_upload.filename)
    csvFile = csv.reader(file)
    starttime = datetime.datetime.now()

    columns = []

    row1 = next(csvFile)

    qStr = "("

    dbconn = initdbConn()
    c = dbconn.cursor()
    try:
        queryStr = 'drop table ' + file_to_upload.filename[:-4]
        c.execute(queryStr)
        dbconn.commit()
    except:
        None

    queryStr = '''Create Table UNPrecip (Country_or_territory VARCHAR(50),Station_Name VARCHAR(50),WMO_Station_Number VARCHAR(50),Unit VARCHAR(50),E FLOAT,F FLOAT,G FLOAT,H FLOAT,I FLOAT,J FLOAT,K FLOAT,L FLOAT,M FLOAT,N VARCHAR(50),O FLOAT,P INT,id INT AUTO_INCREMENT,column_random VARCHAR(50), PRIMARY KEY (id))'''

    # queryStr = 'Create Table ' + file_to_upload.filename[:-4] + ' ' + qStr
    print queryStr
    c.execute(queryStr);
    dbconn.commit()

    c.close()
    dbconn.close()

    dbconn = initdbConn()
    c = dbconn.cursor()
    queryStr = "LOAD DATA LOCAL INFILE '" + file_to_upload.filename + "' INTO TABLE " + file_to_upload.filename[
                                                                                        :-4] + " FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES SET column_random = FLOOR(RAND()*100)"
    print queryStr
    c.execute(queryStr)
    dbconn.commit()
    endtime = datetime.datetime.now()
    c.close()
    dbconn.close()

    DBtime = endtime - starttime
    session['columns'] = columns
    session['tableName'] = file_to_upload.filename[:-4]
    try:
        os.remove(secure_filename(file_to_upload))
    except:
        None

    return render_template('random_query.html', columns=columns, tableName=session['tableName'], DBtime=DBtime)


# Performing memcache query
@application.route('/memQuery', methods=['POST'])
def memCacheQuery():
    numQueries = request.form['tuples']
    dbconn = initdbConn()
    hit = 0
    miss = 0
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    memc.flush_all()
    for num in range(0, int(numQueries)):
        queryStr = "SELECT * FROM " + session['tableName'] + " WHERE column_random = " + str(random.randint(0, 99))
        hashObject = hashlib.sha256(queryStr)
        key = str(hashObject.hexdigest())
        cacheval = memc.get(key)
        if not cacheval:
            c.execute(queryStr)
            rows = c.fetchall()
            memc.set(key, rows)
            miss = miss + 1
        else:
            hit = hit + 1

    endtime = datetime.datetime.now()
    res = endtime - starttime

    c.close()
    dbconn.close()
    return '<h4>Time Taken:<br> ' + str(res) + '</h4><h4>Cache hit:<br> ' + str(
        hit) + '</h4><h4>Cache miss:<br> ' + str(
        miss) + '</h4><br><br><form action="../"><input type="Submit" value="Back"></form>'


# Running a random query
@application.route('/normQuery', methods=['POST'])
def normalQuery():
    numQueries = request.form['tuples']
    print numQueries
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    print starttime
    for num in range(0, int(numQueries)):
        queryStr = "SELECT * FROM " + session['tableName'] + " WHERE column_random = " + str(random.randint(0, 99))
        c.execute(queryStr)

    endtime = datetime.datetime.now()
    res = endtime - starttime

    c.close()
    dbconn.close()
    return '<h4>Time Taken:<br> ' + str(
        res) + '</h4><br><br><form action="./"><input type="Submit" value="Back"></form>'


@application.route('/userQuery', methods=['POST'])
def userQuery():
    operations = []
    operations.append('Select')
    operations.append('Update')
    operations.append('Delete')
    return render_template('user_gen_query.html', columns=session['columns'], operations=operations)


@application.route('/userQueryMemCache', methods=['POST'])
def userQueryMem():
    operations = []
    operations.append('Select')
    operations.append('Update')
    operations.append('Delete')
    return render_template('user_gen_query_memcache.html', columns=session['columns'], operations=operations)


# Dynamic User query
@application.route('/user_query', methods=['POST'])
def user_query():
    records = []
    print request.form['countryname']
    countryname = request.form['countryname']
    query1 = request.form['query1']
    query2 = request.form['query2']

    query1strings = query1.split(" ")
    query2strings = query2.split(" ")

    queryString = "Select * from " + session['tableName'] + " where Country_or_territory = '" + countryname + "' and "
    queryString += (query1strings[0])
    if query1strings[1] == 'E':
        queryString += " = "
    else:
        queryString += " < "
    queryString += query1strings[2]

    queryString += (" and " + query2strings[0])
    if query2strings[1] == 'E':
        queryString += " = "
    else:
        queryString += " < "
    queryString += query2strings[2]
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    for num in range(0, 250):
        c.execute(queryString)
        records = c.fetchall()
    endtime = datetime.datetime.now()
    res = endtime - starttime
    c.close()
    dbconn.close()
    return render_template('user_gen_query.html', records=records, res=res)

# Updating Column values
@application.route('/updateColumnData', methods=['POST'])
def updateColumnData():
    column2update = request.form['column2update']
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    queryStr = "Update UNPrecip set " + column2update + "= -1 where " + column2update + "> 10000"
    c.execute(queryStr)
    dbconn.commit()
    c.close()
    dbconn.close()
    endtime = datetime.datetime.now()
    res = endtime - starttime
    return '<h4>Update complete. Time Taken:<br> ' + str(
        res) + '</h4><br><br><form action="../"><input type="Submit" value="Back"></form>'

# Removing tuple values
@application.route('/removeCanada', methods=['POST'])
def removeCanada():
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    queryStr = "delete from UNPrecip where Country_or_territory = 'canada'"
    c.execute(queryStr)
    dbconn.commit()
    c.close()
    endtime = datetime.datetime.now()
    res = endtime - starttime
    return '<h4>Delete complete. Time Taken:<br> ' + str(
        res) + '</h4><br><br><form action="../"><input type="Submit" value="Back"></form>'


# Dynamic User query using memcache
@application.route('/user_query_memcache', methods=['POST'])
def user_query_memcache():
    records = []
    countryname = request.form['countryname']
    query1 = request.form['query1']
    query2 = request.form['query2']

    query1strings = query1.split(" ")
    query2strings = query2.split(" ")

    queryString = "Select * from " + session['tableName'] + " where Country_or_territory = '" + countryname + "' and "
    queryString += (query1strings[0])
    if query1strings[1] == 'E':
        queryString += " = "
    else:
        queryString += " < "
    queryString += query1strings[2]

    queryString += (" and " + query2strings[0])
    if query2strings[1] == 'E':
        queryString += " = "
    else:
        queryString += " < "
    queryString += query2strings[2]
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    memc.flush_all()
    for num in range(0, 250):
        hashObject = hashlib.sha256(queryString)
        key = str(hashObject.hexdigest())
        cacheval = memc.get(key)
        if not cacheval:
            c.execute(queryString)
            records = c.fetchall()
            memc.set(key, records)
    endtime = datetime.datetime.now()
    res = endtime - starttime
    c.close()
    dbconn.close()
    return render_template('user_gen_query_memcache.html', records=records, res=res)

# Finding equal columns query
@application.route('/sameColumns', methods=['POST'])
def sameColumns():
    dbconn = initdbConn()
    c = dbconn.cursor()
    starttime = datetime.datetime.now()
    queryString = "select * from UNPrecip where Country_or_territory = Station_Name"
    c.execute(queryString)
    records = c.fetchall()
    endtime = datetime.datetime.now()
    res = endtime - starttime
    c.close()
    dbconn.close()
    return render_template('results.html', records=records, res=res)


if __name__ == "__main__":
    application.debug = True
    application.run()
