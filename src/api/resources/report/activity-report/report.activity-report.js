import async from "async";
import logger from "../../../../logger";
import mongodb from "mongodb";
import { EmailModel } from "../../commons/model";
import { TransactionServiceClient } from "../../../db";
import { uploadMediaFile } from "../../services/file.service";
import { generatePdfFromHtml } from "../../services/email-template.service";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const MongoClient = mongodb.MongoClient;

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

      const client = TransactionServiceClient();

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
                "SUM(CASE WHEN status.name = 'successful' THEN tnx.service_fee else 0 END) AS totalServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN 1 else NULL END) AS topUpCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.amount else 0 END) AS topUpAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.vendor_fee else 0 END) AS topUpVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'wallet_top_up' THEN tnx.service_fee else 0 END) AS topUpServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN 1 else NULL END) AS transferCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.amount else 0 END) AS transferAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.vendor_fee else 0 END) AS transferVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'transfer' THEN tnx.service_fee else 0 END) AS transferServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN 1 else NULL END) AS withdrawalCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.amount else 0 END) AS withdrawalAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.vendor_fee else 0 END) AS withdrawalVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'withdrawal' THEN tnx.service_fee else 0 END) AS withdrawalServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN 1 else NULL END) AS cableTvCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.amount else 0 END) AS cableTvAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.vendor_fee else 0 END) AS cableTvVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'cable_tv' THEN tnx.service_fee else 0 END) AS cableTvServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN 1 else NULL END) AS electricityCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.amount else 0 END) AS electricityAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.vendor_fee else 0 END) AS electricityVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name = 'phcn' THEN tnx.service_fee else 0 END) AS electricityServiceAmount, " +
                "COUNT(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN 1 else NULL END) AS mobileCount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.amount else 0 END) AS mobileAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.vendor_fee else 0 END) AS mobileVendorAmount, " +
                "SUM(CASE WHEN status.name = 'successful' and tnxType.name IN ('airtime', 'data') THEN tnx.service_fee else 0 END) AS mobileServiceAmount " +
                "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
                "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                values: [dateFrom, dateTo, `${userId}`],
              };

              client
                .query(query)
                .then((response) => {
                  const results = response.rows;
                  if (results.length !== 0) {
                    const userTnx = results[0];
                    userTnx = Object.assign(userTnx, { schedule, firstName: user.firstName });
                    console.log(userTnx);
                    generatePdfFromHtml(userTnx, sendToEmailCallback, user.email);
                  }
                  // if (!!callback) callback();
                })
                .catch((error) => {
                  logger.error(
                    `error occurred while computing report for user [${userId}] with error [${error}]`
                  );
                  // if (!!callback) callback();
                });
            },
            async (err) => {
              logger.info(`Total data for retention found`);
              client.end();
            }
          );
        });
    }
  );
}

export function uploadFileToFileServiceAndSendToKafka(report, email) {
  const fileFormat = "pdf";

  uploadMediaFile(report, fileFormat)
    .then((response) => {
      console.log(response);
      const config: KafkaConfig = {
        hostname: process.env.KAFKA_HOST,
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        topic: process.env.KAFKA_EMAIL_NOTIFICATION_TOPIC,
      };
      const model: EmailModel = {
        title: process.env.EMAIL_TITLE,
        message: process.env.EMAIL_MESSAGE,
        from: process.env.EMAIL_FROM,
        to: email,
        fileId: response.data.id.toString(),
        mime: fileFormat,
      };

      mikroProducer(config, JSON.stringify(model), function (err, data) {
        if (err) {
          logger.error(
            `error occurred while sending email notification of activity report to user, ERR = [${err}]`
          );
        }
        logger.info(`Sent email notification`);
      });
    })
    .catch((err) => {
      console.log(err);
    });
}
