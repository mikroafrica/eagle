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

const trimData = (activityReport) => {
  for (let key in activityReport) {
    if (activityReport[key] == null) {
      activityReport[key] = "0";
    }
    if (isNaN(activityReport[key])) {
      continue;
    }

    if (key.includes("amount")) {
      let trimmedValue = parseFloat(activityReport[key]).toFixed(2);
      activityReport[key] = trimmedValue;
    }
  }
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
                  "SELECT COUNT(CASE WHEN status.name = 'successful' THEN 1 else NULL END) AS successfulCount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.amount else 0 END) AS successfulAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.vendor_fee else 0 END) AS totalVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.mikro_commission else 0 END) AS totalMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN 1 else NULL END) AS topUpCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.amount else 0 END) AS topUpAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.vendor_fee else 0 END) AS topUpVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.mikro_commission else 0 END) AS topUpMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN 1 else NULL END) AS transferCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.amount else 0 END) AS transferAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.vendor_fee else 0 END) AS transferVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.mikro_commission else 0 END) AS transferMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN 1 else NULL END) AS withdrawalCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.amount else 0 END) AS withdrawalAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.vendor_fee else 0 END) AS withdrawalVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.mikro_commission else 0 END) AS withdrawalMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN 1 else NULL END) AS cableTvCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.amount else 0 END) AS cableTvAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.vendor_fee else 0 END) AS cableTvVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.mikro_commission else 0 END) AS cableTvMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN 1 else NULL END) AS electricityCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.amount else 0 END) AS electricityAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.vendor_fee else 0 END) AS electricityVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.mikro_commission else 0 END) AS electricityMikroAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN 1 else NULL END) AS mobileCount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.amount else 0 END) AS mobileAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.vendor_fee else 0 END) AS mobileVendorAmount, " +
                  "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.mikro_commission else 0 END) AS mobileMikroAmount " +
                  "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                  "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
                  "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                values: [dateFrom, dateTo, `${userId}`],
              };

              const transactionResults = await transactionClient.query(query);
              let activityReport = transactionResults.rows[0];
              let firstName = user.firstName.toLowerCase();
              firstName =
                firstName.charAt(0).toUpperCase() + firstName.slice(1);
              activityReport = Object.assign(activityReport, {
                schedule,
                firstName,
                currentDate: moment().format("LL"),
              });
              trimData(activityReport);
              generateAndSendMail(
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
