// Basic ExpressJS hello world.
const express = require('express')
const app = express();
const multer = require("multer");
const multerS3 = require("multer-s3");

const {
  S3Client,
  ListBucketsCommand,
  ListObjectsCommand,
  GetObjectCommand
} = require('@aws-sdk/client-s3');

const {
  SNSClient,
  ListTopicsCommand,
  GetTopicAttributesCommand,
  SubscribeCommand,
  PublishCommand,
} = require("@aws-sdk/client-sns");

const {
  SQSClient,
  GetQueueUrlCommand,
  SendMessageCommand,
  ListQueuesCommand,
} = require("@aws-sdk/client-sqs")

const {
  ListTablesCommand,
  DynamoDBClient,
  ScanCommand,
  PutItemCommand,
  QueryCommand
} = require("@aws-sdk/client-dynamodb");

// https://www.npmjs.com/package/uuid
const { v4: uuidv4 } = require('uuid');
// https://www.npmjs.com/package/ip
var ip = require('ip');
//////////////////////////////////////////////////////////////////////////////
// Change this to match YOUR default REGION
//////////////////////////////////////////////////////////////////////////////
const REGION = "us-west-2"; //e.g. "us-east-1";
const s3 = new S3Client({ region: REGION });
///////////////////////////////////////////////////////////////////////////
// I hardcoded my S3 bucket name, this you need to determine dynamically
// Using the AWS JavaScript SDK
///////////////////////////////////////////////////////////////////////////
var bucketName = 'jrh-raw-bucket';
//listBuckets().then(result =>{bucketName = result;}).catch(err=>{console.error("listBuckets function call failed.")});
var upload = multer({
  storage: multerS3({
    s3: s3,
    bucket: bucketName,
    key: function (req, file, cb) {
      cb(null, file.originalname);
    }
  })
});

//////////////////////////////////////////////////////////
// Add S3 ListBucket code
//
var bucket_name = "";
const listBuckets = async () => {

  const client = new S3Client({ region: REGION });
  const command = new ListBucketsCommand({});
  try {
    const results = await client.send(command);
    //console.log("List Buckets Results: ", results.Buckets[0].Name);
    for (element of results.Buckets) {
      if (element.Name.includes("raw")) {
        console.log(element.Name)
        bucket_name = element.Name
      }
    }

    const params = {
      Bucket: bucket_name
    }
    return params;

  } catch (err) {
    console.error(err);
  }
};

///////////////////////////////////////
// ListObjects S3
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-s3/interfaces/listobjectscommandoutput.html
//
const listObjects = async (req, res) => {
  const client = new S3Client({ region: REGION });
  const command = new ListObjectsCommand(await listBuckets());
  try {
    const results = await client.send(command);
    console.log("List Objects Results: ", results);
    var url = [];
    for (let i = 0; i < results.Contents.length; i++) {
      url.push("https://" + results.Name + ".s3.amazonaws.com/" + results.Contents[i].Key);
    }
    console.log("URL: ", url);
    return url;
  } catch (err) {
    console.error(err);
  }
};


///////////////////////////////////////////////
/// Get posted data as an async function
//
const getPostedData = async (req, res) => {
  try {
    let s3URLs = await listObjects(req, res);
    const fname = req.files[0].originalname;
    var s3URL = "URL not generated due to technical issue.";
    for (let i = 0; i < s3URLs.length; i++) {
      if (s3URLs[i].includes(fname)) {
        s3URL = s3URLs[i];
        break
      }
    }
    res.write('Successfully uploaded ' + req.files.length + ' files!')

    // Use this code to retrieve the value entered in the username field in the index.html
    var username = req.body['name'];
    // Use this code to retrieve the value entered in the email field in the index.html
    var email = req.body['email'];
    // Use this code to retrieve the value entered in the phone field in the index.html
    var phone = req.body['phone'];
    res.write(username + "\n");
    res.write(s3URL + "\n");
    res.write(email + "\n");
    res.write(phone + "\n");

    res.end();
  } catch (err) {
    console.error(err);
  }
};

////////////////////////////////////////////////
// Get images for Image Gallery
//
const getImagesFromS3Bucket = async (req, res) => {
  try {
    let imageURL = await listObjects(req, res);
    console.log("ImageURL:", imageURL);
    res.set('Content-Type', 'text/html');
    res.write("<div>Welcome to the gallery" + "</div>");
    for (let i = 0; i < imageURL.length; i++) {
      res.write('<div><img src="' + imageURL[i] + '" /></div>');
    }
    res.end();
  } catch (err) {
    console.error(err);
  }
};

/////////////////////////////////////////////////
// add list SNS topics here
//

const getListOfSnsTopics = async () => {
  const client = new SNSClient({ region: REGION });
  const command = new ListTopicsCommand({});
  try {
    const results = await client.send(command);
    //console.error("Get SNS Topic Results: ", results.Topics.length);
    //console.error("ARN: ", results.Topics[0].TopicArn);
    //return results.Topics[0];
    return results;
  } catch (err) {
    console.error(err);
  }
};

///////////////////////////////////////////
// List of properties of Topic ARN
//
const getSnsTopicArn = async () => {
  let snsTopicArn = await getListOfSnsTopics();
  //	console.log(snsTopicArn.Topics[0].TopicArn);
  const params = {
    TopicArn: snsTopicArn.Topics[0].TopicArn,
  };
  const client = new SNSClient({ region: REGION });
  const command = new GetTopicAttributesCommand(params);
  try {
    const results = await client.send(command);
    //console.log("Get SNS Topic Properties results: ",results);
    return results;
  } catch (err) {
    console.error(err);
  }
};

///////////////////////////////////////////////////
// Register email with Topic
//
const subscribeEmailToSNSTopic = async (req, res) => {
  let topicArn = await getListOfSnsTopics();
  let email = req.body['email']
  const params = {
    // CHANGE ENDPOINT EMAIL TO YOUR OWN
    Endpoint: email,
    Protocol: "email",
    TopicArn: topicArn.Topics[0].TopicArn,
  };
  const client = new SNSClient({ region: REGION });
  const command = new SubscribeCommand(params);
  try {
    const results = await client.send(command);
    console.log("Subscribe Results: ", results);
    return results;
  } catch (err) {
    console.error(err);
  }
};

////////////////////////////////////////////////////////////////////////////////
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/sqs/command/ListQueuesCommand/

const listSqsQueueURL = async (req, res) => {
  const client = new SQSClient({ region: REGION });
  const input = {};
  const command = new ListQueuesCommand(input);
  try {
    const response = await client.send(command);
    console.log(response['QueueUrls'][0]);
    return response['QueueUrls'][0];
  } catch (err) {
    console.error(err);
  }
};

////////////////////////////////////////////////////////////////////////////////
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/sqs/command/SendMessageCommand/
// get Send Messages
//

const sendMessageToQueue = async (req, res, recordID) => {

  //let recordID = getRecordNumber.then(RecordNumber => recordID = RecordNumber);
  //function basicReturn() {  return Promise.resolve(getRecordNumber);}

  console.log("Enter SQS Send Message...");
  let sqsQueueURL = await listSqsQueueURL(req, res);
  console.log("Sent RecordNumber: " + recordID);
  //let recordID = recNum;
  //let recordID = await retrieveLastDynamoRecordID(recNum);
  const client = new SQSClient({ region: REGION });
  const input = { // SendMessageRequest
    QueueUrl: sqsQueueURL, // required
    //MessageBody: String(recordID), // required
    MessageBody: String(recordID), // required
  };
  const command = new SendMessageCommand(input);
  try {
    const response = await client.send(command);
    return response;
  } catch (err) {
    console.error(err)
  }
};

////////////////////////////////////////////////////////////////////////////////
// DynamoDB retrieve last inserted record
// Since this is a No-SQL database, this won't work like a Relational Database
//
const retrieveLastDynamoRecordID = async (recNum) => {
  const table = await getDynamoTable();
  const client = new DynamoDBClient({ region: REGION });

  console.log("About to Query for the item of the RecordNumber just passed: " + recNum)
  const command = new QueryCommand({
    TableName: table.TableNames[0],
    "KeyConditionExpression": "RecordNumber = :rNum",
    "ExpressionAttributeValues": { ":rNum": { "S": recNum } },
    // needed to make sure the reads are consistent, the default
    // is eventually consistent and might miss the latest item inserted
    ConsistentRead: true
  });

  // https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/the-response-object.html
  // The V2 documents show to use the .S to access the value of the KV pair
  // https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/the-response-object.html
  console.log("About to issue QueryCommand...")
  const response = await client.send(command);
  console.log("last record: " + JSON.stringify(response.Items));
  console.log("last RecordNumber: " + JSON.stringify(response.Items[0].RecordNumber.S));
  return (String(response.Items[0].RecordNumber.S))
}

////////////////////////////////////////////////////////////////////////////////
// DynamoDB Examples
// https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/javascript_dynamodb_code_examples.html
///////////////////////////////////////////////////////////////////////////////
const getDynamoTable = async () => {

  const client = new DynamoDBClient({ region: REGION });
  const command = new ListTablesCommand({});
  const response = await client.send(command);
  console.log(response.TableNames.join("\n"));
  return response;
};

////////////////////////////////////////////////////////////////////////////////
// DynamoDB query item
// https://docs.aws.amazon.com/sdk-for-javascript/v3/developer-guide/javascript_dynamodb_code_examples.html
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/dynamodb/command/ScanCommand/
///////////////////////////////////////////////////////////////////////////////
const queryAndPrintDynamoRecords = async (req, res) => {

  const table = await getDynamoTable();
  const client = new DynamoDBClient({ region: REGION });

  const command = new ScanCommand({
    TableName: table.TableNames[0]
  });

  const response = await client.send(command);
  console.log(response);
  res.set('Content-Type', 'text/html');
  res.write("Here are the records: " + "\n");
  console.log(JSON.stringify(response.Items));
  res.write(JSON.stringify(response.Items));
  res.end();
  //return response;
};

////////////////////////////////////////////////////////////////////////////////
// DynamoDB put item
// https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/client/dynamodb/command/PutItemCommand/
//
const putDynamoItem = async (req, res) => {
  console.log("About to put item into DynamoDB...")
  /*
  ID INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  RecordNumber VARCHAR(64), -- This is the UUID
  CustomerName VARCHAR(64),
  Email VARCHAR(64),
  Phone VARCHAR(64),
  Stat INT(1) DEFAULT 0, -- Job status, not done is 0, done is 1
  RAWS3URL VARCHAR(200), -- set the returned S3URL here
  FINSIHEDS3URL VARCHAR(200)
  */
  // Will go and query the URL of the just posted image to S3 Raw Bucket
  try {
    let s3URLs = await listObjects(req, res);
    const fname = req.files[0].originalname;
    var s3URL = "URL not generated due to technical issue.";
    console.log("Finding S3URLs...")
    for (let i = 0; i < s3URLs.length; i++) {
      if (s3URLs[i].includes(fname)) {
        s3URL = s3URLs[i];
        break
      }
    }
  }
  catch (err) {
    s3URL = "";
    console.error(err);
  }
  console.log("Listing S3 urls and UUID...")
  // generate a UUID for RecordNumber
  // https://www.npmjs.com/package/uuidv4
  const RecordNumber = uuidv4();
  console.log(RecordNumber);
  const table = await getDynamoTable();
  console.log(table);
  const client = new DynamoDBClient({ region: REGION });
  const input = {
    TableName: table.TableNames[0],
    Item: {
      "Email": {
        "S": String(req.body['email'])
      },
      "RecordNumber": {
        "S": String(RecordNumber)
      },
      "CustomerName": {
        "S": String(req.body['name'])
      },
      "Phone": {
        "S": String(req.body['phone'])
      },
      "Stat": {
        "N": "0"
      },
      "RAWS3URL": {
        "S": String(s3URL)
      },
      "FINSIHEDS3URL": {
        "S": ""
      }
    },
    //"ReturnConsumedCapacity": "TOTAL",

  }
  console.log("About to insert item...")

  const command = new PutItemCommand(input);
  const response = await client.send(command);

  console.log("Finished inserting item...");
  console.log("Returning: " + RecordNumber);
  const getSendMessageToQueue = await sendMessageToQueue(res, req, RecordNumber);

  return (RecordNumber);
};

////////////////////////////////////////////////////////////////////////////////
// Request to index.html or / express will match this route and render this page
//

app.get("/", function (req, res) {
  res.sendFile(__dirname + "/index.html");
});

app.get("/gallery", function (req, res) {
  (async () => { await getImagesFromS3Bucket(req, res); })();
});

app.post("/upload", upload.array("uploadFile", 1), function (req, res, next) {
  (async () => { await getPostedData(req, res); })();
  (async () => { await getListOfSnsTopics(req, res); })();
  (async () => { await getSnsTopicArn() })();
  (async () => { await subscribeEmailToSNSTopic(req, res) })();
  (async () => { await putDynamoItem(req, res) })();
  //(async () => { await sendMessageToQueue(req, res) }) ();
});

app.get("/dynamodb", function (req, res) {
  (async () => { await queryAndPrintDynamoRecords(req, res) })();
});

app.get("/ip", function (req, res) {
  res.set('Content-Type', 'text/html');
  res.write("Here are the records: " + "\n");
  res.write("Network access via: " + ip.address());
  res.end();
});

app.listen(3000, function () {
  console.log("Amazon s3 file upload app listening on port 3000");
});
