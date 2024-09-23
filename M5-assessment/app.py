import logging
from io import BytesIO
from urllib.parse import urlparse

import boto3
import mysql.connector
from botocore.config import Config
from botocore.exceptions import ClientError
from PIL import Image

messagesInQueue = False

# https://stackoverflow.com/questions/40377662/boto3-client-noregionerror-you-must-specify-a-region-error-only-sometimes
region = 'us-west-2'

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
clientSQS = boto3.client('sqs',region_name=region)
clientRDS = boto3.client('rds',region_name=region)
clientSM = boto3.client('secretsmanager',region_name=region)
clientSNS = boto3.client('sns',region_name=region)
# https://github.com/boto/boto3/issues/1644
# Needed to help generate pre-signed URLs
clientS3 = boto3.client('s3', region_name=region,config=Config(s3={'addressing_style': 'path'}, signature_version='s3v4') )

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/secretsmanager/client/get_secret_value.html
print("Retrieving username SecretString...")
responseUNAME = clientSM.get_secret_value(
    SecretId='uname'
)
print("Retrieving password SecretString...")
responsePWORD = clientSM.get_secret_value(
    SecretId='pword'
)

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds/client/describe_db_instances.html
print("Retrieving RDS instance information...")
responseRDS = clientRDS.describe_db_instances()

##############################################################################
# Set database credentials
##############################################################################
hosturl = responseRDS['DBInstances'][0]['Endpoint']['Address']
uname = responseUNAME['SecretString']
pword = responsePWORD['SecretString']

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/list_queues.html
print("Getting a list of SQS queues...")
responseURL = clientSQS.list_queues()

# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/receive_message.html
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

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_objects.html
    #responseS3Object = clientS3.list_objects(
    #    Bucket=BUCKET_NAME
    #    )

    #OBJECT_NAME = responseS3Object['Contents'][0]['Key']

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
    responseGetObject = clientS3.get_object(
        Bucket=BUCKET_NAME,
        Key=key
    )

    # Saving S3 stream to a local byte stream
    print("Saving S3 byte stream to local byte stream...")
    file_byte_string = responseGetObject['Body'].read()

    # Convert byte stream to an Image object and pass it to PIL
    print("Converting local byte stream to an image...")
    im = Image.open(BytesIO(file_byte_string))

    print("Printing Image size meta-data...")
    print(im.format, im.size, im.mode)

    # https://pythonexamples.org/pillow-convert-image-to-grayscale/
    print("Converting image to grayscale...")
    # Convert the image to grayscale
    im = im.convert("L")
    print("Saving newly created image to disk...")
    # Save the grayscale image
    file_name = "/tmp/grayscale-" + key
    im.save(file_name)

    print("Printing Grayscale Image size meta-data...")
    print(im.format, im.size, im.mode)

    # Uploading Files to S3
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    # Upload the file
    print("Pushing modified image to Finished S3 bucket...")
    for n in range(0,len(responseS3['Buckets'])):
      if "finished" in responseS3['Buckets'][n]['Name']:
        FIN_BUCKET_NAME = responseS3['Buckets'][n]['Name']

    try:
        responseS3Put = clientS3.upload_file(file_name, FIN_BUCKET_NAME, key)
    except ClientError as e:
        logging.error(e)

    # Generate Presigned URL
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    print("Generating presigned S3 URL...")
    try:
        responsePresigned = clientS3.generate_presigned_url('get_object', Params={'Bucket': FIN_BUCKET_NAME,'Key': key},ExpiresIn=7200)
    except ClientError as e:
        logging.error(e)

    print(str(responsePresigned))

    # Update Finished URL to RDS Entry
       ##############################################################################
    # Connect to Mysql database Update record with Finished URL
    # https://dev.mysql.com/doc/connector-python/en/connector-python-example-cursor-select.html
    ##############################################################################
    print("Connecting to the RDS instances, and updating the Finished URL for record: " + str(ID) + "...")
    cnx = mysql.connector.connect(host=hosturl, user=uname, password=pword, database='company')
    cursor = cnx.cursor()

    update = ("UPDATE entries SET FINSIHEDS3URL = '" + str(responsePresigned) + "' WHERE ID = " + str(ID) + ";")
    print(update)

    print("Executing the UPDATE command against the DB...")
    cursor.execute(update)
    cnx.commit()

    cursor.close()
    cnx.close()
    #################################################################################
    # SEND Presigned URL to SNS Topics
    #################################################################################
    # Retrieve the TopicARN
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/list_topics.html
    print("Listing SNS Topic ARNs...")
    responseTopics = clientSNS.list_topics()
    print(responseTopics['Topics'][0]['TopicArn'])
    # Publish Message to the Topic ARN
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    messageToSend = "Your image: " + str(file_name) + " is ready for download at: " + str(responsePresigned)
    print("Message we will be sending: " + str(messageToSend))
    responsePublish = clientSNS.publish(
    TopicArn=responseTopics['Topics'][0]['TopicArn'],
    Subject="Your image is ready for download!",
    Message=messageToSend,
    )
    print("Message published to SNS Topic, all who are subscribed will receive it...")

    ############################################################################
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs/client/delete_message.html
    print("Now deleting the message off of the queue...")
    responseDelMessage = clientSQS.delete_message(
        QueueUrl=responseURL['QueueUrls'][0],
        ReceiptHandle=responseMessages['Messages'][0]['ReceiptHandle']
    )

    print(responseDelMessage)

    #############################################################################
    # Delete Object from Raw S3 bucket
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
    #############################################################################
    print("Now deleting the Image object from the RawS3 bucket...")
    responseDelObject = clientS3.delete_object(
    Bucket=BUCKET_NAME,
    Key=key
    )

    #############################################################################
    # Graded component
    # Add code to update the RAWS3URL to have the value: done after the image is processed
    #############################################################################



    #############################################################################
    # Extra challenge, not graded...
    # Could you add code to unsubscribe your email from the Topic once you received the image?
    #############################################################################
