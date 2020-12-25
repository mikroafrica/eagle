import async from "async";
import logger from "../../../../logger";
import mongodb from "mongodb";
import { EmailModel } from "../../commons/model";
import { TransactionServiceClient } from "../../../db";
import { generateAndSendMail } from "../../services/email-template.service";
import moment from "moment";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const MongoClient = mongodb.MongoClient;

//Loops over the activity report, insert missing fields and properly formats the amount values
const parseAndformatData = (activityReport) => {
  let activityReportObject = {};

  for (let typeData of activityReport) {
    activityReportObject[typeData.name] = typeData;
  }

  let activityReportFormatted = {};
  const types = [
    "wallet_top_up",
    "transfer",
    "withdrawal",
    "cable_tv",
    "phcn",
    "airtime",
    "data",
  ];

  for (let type of types) {
    if (type in activityReportObject) {
      activityReportFormatted[`${type}totalcount`] =
        activityReportObject[type]["totalcount"];
      activityReportFormatted[`${type}totalamount`] =
        activityReportObject[type]["totalamount"];
      activityReportFormatted[`${type}serviceamount`] =
        activityReportObject[type]["serviceamount"];
      activityReportFormatted[`${type}mikroamount`] =
        activityReportObject[type]["mikroamount"];
    } else {
      activityReportFormatted[`${type}totalcount`] = 0;
      activityReportFormatted[`${type}totalamount`] = 0;
      activityReportFormatted[`${type}serviceamount`] = 0;
      activityReportFormatted[`${type}mikroamount`] = 0;
    }
  }

  const formatter = new Intl.NumberFormat("en-NG", {
    style: "currency",
    currency: "NGN",
  });

  for (let key in activityReportFormatted) {
    if (key.includes("amount")) {
      activityReportFormatted[key] = formatter.format(
        activityReportFormatted[key]
      );
    }
  }

  return activityReportFormatted;
};

export function getActivityReport(
  dateFrom,
  dateTo,
  schedule,
  sendToEmailCallback
) {
  MongoClient.connect(
    process.env.CONSUMER_SERVICE_MONGO_URI,
    {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    },
    function (err, db) {
      if (err) {
        throw err;
      }
      const dbo = db.db(process.env.CONSUMER_SERVICE_MONGO_DB_NAME);

      const transactionClient = TransactionServiceClient();
      dbo
        .collection("user")
        .find({})
        .toArray(function (err, users) {
          async.forEachOf(
            users,
            async (user, key, callback) => {
              const userId = user._id;
              const query = {
                text:
                  "SELECT tnxType.name, COUNT(CASE WHEN status.name = 'successful' THEN 1 else NULL END) AS totalCount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.amount else 0 END) AS totalAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.service_fee else 0 END) AS serviceAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.mikro_commission else 0 END) AS mikroAmount " +
                  "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                  "JOIN public.transaction_statuses status ON status.id = tnx.transaction_status " +
                  "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3 " +
                  "GROUP BY tnxType.name",

                values: [dateFrom, dateTo, `${userId}`],
              };

              const transactionResults = await transactionClient.query(query);
              let activityReport = parseAndformatData(transactionResults.rows);

              let firstName = user.firstName.toLowerCase();
              firstName =
                firstName.charAt(0).toUpperCase() + firstName.slice(1);

              activityReport = Object.assign(activityReport, {
                schedule,
                firstName,
                currentDate: moment().format("LL"),
              });

              await generateAndSendMail(
                activityReport,
                sendToEmailCallback,
                user.email
              );
            },
            async (err) => {
              logger.info(`Total data for all users found, or err = ${err}`);
              transactionClient.end();
            }
          );
        });
    }
  );
}

export function SendEmailToKafka(report, email) {
  const config: KafkaConfig = {
    hostname: process.env.KAFKA_HOST,
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
    topic: process.env.KAFKA_EMAIL_NOTIFICATION_TOPIC,
    groupId: process.env.KAFKA_EMAIL_GROUP_ID,
  };
  const emailModel: EmailModel = {
    title: process.env.EMAIL_TITLE,
    body: report,
    from: process.env.EMAIL_FROM,
    to: email,
  };

  mikroProducer(config, JSON.stringify(emailModel), function (err, data) {
    if (err) {
      logger.error(
        `error occurred while sending email notification of activity report to user, ERR = [${err}]`
      );
    }
    logger.info(`Sent email notification`);
  });
}
