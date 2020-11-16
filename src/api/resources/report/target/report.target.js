import cron from "cron";
const CronJob = cron.CronJob;

import async from "async";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";
const { ObjectId } = mongodb;
import {
  convertTimeStampToDate,
  firstDayOfMonth,
  previousDayAtNight,
} from "../../commons/model";
import { TransactionServiceClient } from "../../../db";

const MongoClient = mongodb.MongoClient;

function computeTargetReport() {
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

      const firstDayOfTheMonth = firstDayOfMonth();
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
                  "SELECT SUM(CASE WHEN status.name = 'successful' THEN tnx.amount ELSE 0 END) AS successfulAmount, " +
                  "COUNT(CASE WHEN status.name = 'successful' THEN 1 ELSE NULL END) AS successfulCount " +
                  "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                  "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
                  "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                values: [firstDayOfTheMonth, pastDayAtNight, `${userId}`],
              };
              client
                .query(query)
                .then((response) => {
                  const results = response.rows;

                  if (results.length !== 0) {
                    const userId = ObjectId(user._id);
                    const totalTransactionPerMonth = parseFloat(
                      results[0].successfulamount || 0
                    );
                    const totalTransactionCountPerMonth = parseFloat(
                      results[0].successfulcount
                    );

                    dbo.collection("user").findOneAndUpdate(
                      { _id: userId },
                      {
                        $set: {
                          totalTransactionCountPerMonth,
                          totalTransactionPerMonth,
                        },
                      },
                      { new: true },
                      function (err, doc) {
                        logger.info(
                          `transaction per month info updated for user [${userId}] : count [${totalTransactionCountPerMonth}] value: [${totalTransactionPerMonth}]`
                        );
                        callback();
                      }
                    );
                  } else {
                    callback();
                  }
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
                `Users transaction info updated`
              );
              client.end();
            }
          );
        });
    }
  );
}

// run job at every 1:20 A.M
export const PreviousDayTargetReportJob = (): CronJob => {
  return new CronJob(
    "0 40 2 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Retention report @ ${formattedDate} :::`);
      computeTargetReport();
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};
