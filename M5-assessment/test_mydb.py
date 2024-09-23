from urllib.parse import urlparse

import boto3
import mysql.connector
from botocore.config import Config

region = 'us-west-2'

clientRDS = boto3.client('rds',region_name=region)
clientSM = boto3.client('secretsmanager',region_name=region)
clientSQS = boto3.client('sqs',region_name=region)
clientS3 = boto3.client('s3', region_name=region,config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4') )


print("Retrieving RDS instance information...")
responseRDS = clientRDS.describe_db_instances()

##############################################################################
# Get secrets
##############################################################################
print("Retrieving username SecretString...")
responseUNAME = clientSM.get_secret_value(
    SecretId='uname'
)
print("Retrieving password SecretString...")
responsePWORD = clientSM.get_secret_value(
    SecretId='pword'
)

##############################################################################
# Set database credentials
##############################################################################
hosturl = responseRDS['DBInstances'][0]['Endpoint']['Address']
uname = responseUNAME['SecretString']
pword = responsePWORD['SecretString']


print("Getting a list of SQS queues...")
responseURL = clientSQS.list_queues()

print("Retrieving the message on the queue...")
responseMessages = clientSQS.receive_message(
    QueueUrl=responseURL['QueueUrls'][0],
    VisibilityTimeout=180
)

# Check to see if the queue is empty
try:
  responseMessages['Messages']
  messagesInQueue = True
except:
  print("No messages found on the queue -- try to upload one image to your app...")
  exit(0)

if messagesInQueue is True:
    print("Message body content: " + str(responseMessages['Messages'][0]['Body']) + "...")
    print("Proceeding assuming there are messages on the queue...")
    ##############################################################################
    # Connect to Mysql database to retrieve SQS queue record
    # https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html
    ##############################################################################
    print("Connecting to the RDS instances and retrieving the record with the ID passed via the SQS message...")
    cnx = mysql.connector.connect(host=hosturl, user=uname, password=pword, database='company')
    cursor = cnx.cursor()

    query = ("SELECT * FROM entries WHERE ID = %s")

    print("Message Body: " + str(responseMessages['Messages'][0]['Body']))
    print("Executing the SQL query against the DB to retrieve all field of the record...")
    cursor.execute(query, [(responseMessages['Messages'][0]['Body'])])

    print("Printing out all the fields in the record...")
    for (ID, RecordNumber, CustomerName, Email, Phone, Stat, RAWS3URL, FINSIHEDS3URL) in cursor:
        print("ID: " + str(ID) + " RecordNumber: " + str(RecordNumber) + " CustomerName: " + CustomerName )
        print("Email: " + Email + " Phone: " + str(Phone) + " Status: " + str(Stat))
        print("Raw S3 URL: " + str(RAWS3URL) + " Finished URL: " + str(FINSIHEDS3URL))

    cursor.close()
    cnx.close()
    #######################################################################
    # Hack to skip first blank first record
    if str(RAWS3URL) == "http://":
      # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
      print("Now deleting the initial blank record off of the queue...")
      responseDelMessage = clientSQS.delete_message(
        QueueUrl=responseURL['QueueUrls'][0],
        ReceiptHandle=responseMessages['Messages'][0]['ReceiptHandle']
      )
      exit(0)
    #######################################################################
    # Parse URL retrieved from record to get S3 Object key
    # https://docs.python.org/3/library/urllib.parse.html
    #######################################################################
    url = urlparse(RAWS3URL)
    key = url.path.lstrip('/')
    print("S3 Object Key name: " + key)

    #######################################################################
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_buckets.html
    responseS3 = clientS3.list_buckets()

    for n in range(0,len(responseS3['Buckets'])):
        if "raw" in responseS3['Buckets'][n]['Name']:
            BUCKET_NAME = responseS3['Buckets'][n]['Name']
