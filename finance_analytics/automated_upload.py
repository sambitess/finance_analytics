#!/usr/bin/env python3

#Importing required libraries.
import json
import os, shutil, uuid,datetime,pytz
from sqlalchemy import DateTime, create_engine,Column,String,Integer
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID

try:
    #calling the config.json file
    j = open (r'D:\finance_analytics\config.json', 'r')
    val = json.loads(j.read())
    #asigning the key values to a variabe
    user = val['dbuser']
    password = val['dbpassword']
    port = val['dbport']
    dbName = val['dbname']
    sourceDir = val['sourceDir']
    targetDir = val['targetDir']

    #connecting the database using sqlalchemy
    engine = create_engine('postgresql://'+user+':'+password+'@'+port+'/'+dbName+'')
    db = scoped_session(sessionmaker(bind=engine))
    phase = db()
    base = declarative_base()
    #creating the table named as Audit_Table
    class file(base):
        __tablename__ = 'Audit_Table'
        Audit_ID = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
        Start_time = Column(DateTime)
        End_time = Column(DateTime)
        Status_Description = Column(String(255))
        Processed_Files = Column(Integer)
        Status = Column(String(50))
    base.metadata.create_all(engine)

    #creating a empty list for appending the processed files
    x = []
    count = 0
    fileName1 = os.listdir(sourceDir)
    #for capture the start time before the execution
    startTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M:%S.%f")
    for fileName in fileName1:
        if fileName.lower().endswith('.pdf'):
            #for moving the pdf files from sourceDir directory to targetDir
            shutil.move(os.path.join(sourceDir, fileName), targetDir)
            count += 1
            x.append(fileName)
            #creating the exception
            if count > 5:
                raise Exception('Error occured in',fileName1[len(x)])
            reason = 'All files are uploaded'
            status = 'Successful'
    endTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M:%S.%f")
    #insert the values in the Audit_Table, if the all files are succefully inserted
    files = file(Start_time = startTime , End_time = endTime, Status_Description = reason, Processed_Files = len(x), Status = status)
    phase.add(files)
    phase.commit()

#exception
except Exception as e:
    for fileName2 in x:
        #moving the pdf file from target directory to source directory 
        shutil.move(os.path.join(targetDir, fileName2), sourceDir)
    reason = (str(e))
    status = 'Failed'
    #to catch the end time of the execution
    endTime = datetime.datetime.now(tz=pytz.timezone("Asia/Calcutta")).strftime("%Y-%m-%d %H:%M:%S.%f")
    #Insert the values in the Audit_Table for failed files
    files = file(Start_time = startTime , End_time = endTime, Status_Description = reason, Processed_Files = len(x), Status = status)
    phase.add(files)
    phase.commit() 