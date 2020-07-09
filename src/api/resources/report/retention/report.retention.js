import cron from "cron";
const CronJob = cron.CronJob;

import async from "async";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";
import {
  convertTimeStampToDate,
  previousDayAtNight,
  previousDayInMorning,
} from "../../commons/model";
import { TransactionServiceClient } from "../../../db";
import { retentionReport } from "../../services/file.service";
import type { SlackModel } from "../../commons/model";

const mikroKafkaClient = require("mikro-kafka-client");
const mikroProducer = mikroKafkaClient.mikroProducer;
const KafkaConfig = mikroKafkaClient.kafkaConfig;

const MongoClient = mongodb.MongoClient;

function computeRetentionReport(reportCallback) {
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

      const reportArr = [];

      const pastDayInMorning = previousDayInMorning();
      const pastDayAtNight = previousDayAtNight();

      dbo
        .collection("user")
        .find({})
        .toArray(function (err, users) {
          async.forEachOf(
            users,
            (user, key, callback) => {
              delete user.fcmToken;
              delete user.timeUpdated;
              delete user.profileImageId;
              delete user.isBvnVerified;
              delete user._class;

              user.timeCreated = convertTimeStampToDate(user.timeCreated);

              const userId = user._id;

              // compute all user transaction query by user id
              const query = {
                text:
                  "SELECT  COUNT(tnx.id), SUM(tnx.amount) AS amount," +
                  "SUM(CASE WHEN status.name = 'successful' THEN tnx.amount else 0 END) AS successfulAmount, " +
                  "SUM(CASE WHEN status.name = 'payment successful' " +
                  "or status.name = 'bill purchased failed' or status.name = 'payment pending'" +
                  " or status.name = 'pending' THEN tnx.amount else 0 END) AS pendingAmount, " +
                  "SUM(CASE WHEN status.name = 'payment failed' THEN tnx.amount else 0 END) AS failedAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' THEN 1 else NULL END) AS successful, " +
                  "COUNT(CASE WHEN status.name = 'payment failed' THEN 1 else NULL END) AS failed, " +
                  "COUNT(CASE WHEN status.name = 'payment successful' " +
                  "or status.name = 'bill purchased failed' or status.name = 'payment pending'" +
                  " or status.name = 'pending' THEN 1 else NULL END) AS pending " +
                  "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                  "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
                  "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                values: [pastDayInMorning, pastDayAtNight, userId],
              };
              client
                .query(query)
                .then((response) => {
                  const results = response.rows;
                  if (results.length !== 0) {
                    const userTnx = results[0];
                    reportArr.push(Object.assign(user, userTnx));
                  }
                  callback();
                })
                .catch((error) => {
                  logger.error(
                    `error occurred while computing report for user [${userId}] with error [${error}]`
                  );
                  callback();
                });
            },
            (err) => {
              logger.info(
                `Total data for retention found is ${reportArr.length}`
              );
              client.end();

              const time = convertTimeStampToDate(pastDayAtNight);
              const fileName = `Retention Report - ${time}`;
              reportCallback(reportArr, fileName, time);
            }
          );
        });
    }
  );
}

export const PreviousDayRetentionReportJob = (): CronJob => {
  return new CronJob("* 22 23 * * *", function () {
  // return new CronJob("* 30 03 * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(`::: Retention report @ ${formattedDate} :::`);

    computeRetentionReport(function (data, filename, time) {
      pushReportToSlack(data, filename, time);
    });
  }, undefined, true, "Africa/Lagos");
};

function pushReportToSlack(report, filename, time) {
  const params = { report, filename, type: "csv" };

  retentionReport({ params })
    .then((responseData) => {
      const retentionData = responseData.data;
      const retentionLink = retentionData.data.url;

      const config: KafkaConfig = {
        hostname: process.env.KAFKA_HOST,
        username: process.env.KAFKA_USERNAME,
        password: process.env.KAFKA_PASSWORD,
        topic: process.env.KAFKA_SLACK_NOTIFICATION_TOPIC,
      };

      const model: SlackModel = {
        title: `Retention Report for ${time}`,
        channel: process.env.REPORT_CHANNEL,
        message: "Retention upload can be found here : `" + retentionLink + "`",
      };
      mikroProducer(config, JSON.stringify(model), function (err, data) {
        if (err) {
          logger.error(
            `error occurred while publishing transaction summary to slack [${err}]`
          );
        }
        logger.info(`published retention report`);
      });
    })
    .catch((err) => {
      console.log(err);
      logger.error(
        `failed to push retention report with error ${JSON.stringify(err)}`
      );
    });
}
