import { config } from "dotenv";
import { Storage } from "@google-cloud/storage";
import fs from "fs";
import util from "util";
import axios from "axios";
import Mailgun from "mailgun-js";
import AWS from "aws-sdk";

config();

const mailgun = Mailgun({
  apiKey: process.env.MAILGUN_API_KEY,
  domain: process.env.MAILGUN_DOMAIN,
});

const downloadRelease = async (url, outputFilename, userEmail) => {
  try {
    console.log(url);

    const response = await axios({
      url,
      method: "GET",
      responseType: "arraybuffer",
    });

    const fileBuffer = Buffer.from(response.data);

    console.log(fileBuffer);

    return fileBuffer;
  } catch (error) {
    await sendEmail(
      userEmail,
      "Your submission has failed.",
      "Please review your assignment subission. Submission download was unsucessful."
    );

    console.log("Error downloading the release:", error);
    throw error;
  }
};

const readFile = util.promisify(fs.readFile);

const storeInGCS = async (filePath, email) => {
  const bucketName = process.env.BUCKET_NAME;
  const serviceAccountKey = JSON.parse(
    Buffer.from(process.env.GCP_SERVICE_ACCOUNT_KEY, "utf-8")
  );

  const storage = new Storage({
    credentials: serviceAccountKey,
  });

  const timestamp = new Date().toISOString().replace(/:/g, "-"); 
  const fileName = `${email}-submission-${timestamp}.zip`;

  try {
    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);
    await file.save(filePath);
    const gcsObjectPath = `storage-cloud-google-com/${bucketName}/${fileName}`;

    await sendEmail(
      email,
      "Assignment Submitted Successfully!!",
      `Your submission was sucessfully downloaded and submitted. \n\nGCS Object Path: ${gcsObjectPath}`
    );
  } catch (error) {
    console.log("Error", error);
    throw error;
  }
};


const sendEmail = async (recipient, subject, body) => {
  const data = {
    from: "Aravind Sankar <mailgun@aravindsankar.cloud>",
    to: recipient,
    subject: subject,
    text: body,
  };

  try {
    await mailgun.messages().send(data);
    console.log("Email sent successfully");
  } catch (error) {
    console.error("Error sending email:", error);
    throw error;
  }
};

const dynamoDB = new AWS.DynamoDB.DocumentClient();

const recordEmailSent = async (email, status) => {
  const timestamp = new Date().toISOString();
  const ide = email + timestamp;
  const params = {
    TableName: process.env.TABLE_NAME,
    Item: {
      id: ide,
      email: email,
      status: status,
      timestamp: timestamp,
    },
  };
  try {
    console.log(params);
    await dynamoDB.put(params).promise();
    console.log("Record successfully written to DynamoDB");
  } catch (error) {
    console.log(error);
    throw error;
  }
};

export const handler = async (event, context) => {
  try {
    console.log(
      `Function ${context.functionName} start, execution ${context.awsRequestId}`
    );

    if (Array.isArray(event.Records) && event.Records.length > 0) {
      for (const snsRecord of event.Records) {
        const snsMessage = snsRecord.Sns.Message;
        const parsedMessage = JSON.parse(snsMessage);
        const { userEmail, githubRepo, releaseTag } = parsedMessage;

        try {
          console.log("Starting download...");
          const releaseData = await downloadRelease(
            githubRepo,
            releaseTag,
            userEmail
          );
          console.log("Download completed");

          console.log("Storing in GCS...");
          await storeInGCS(releaseData, userEmail);

          console.log("Sending email...");
          await sendEmail(
            userEmail,
            "Assignment Submission Complete!!",
            "Your Assignment has been sucessfully downloaded and submitted."
          );

          console.log("Recording email sent...");
          await recordEmailSent(userEmail, "success");
        } catch (operationError) {
          console.error("Error in processing record:", operationError);
        }
      }
      console.log("All operations completed successfully");
      return { status: "Success" };
    } else {
      console.log("Event.Records is not an array or is empty:", event.Records);
    }
  } catch (error) {
    console.log("Error:", error);
    throw error;
  }
};