import cron from "cron";

const CronJob = cron.CronJob;

import async from "async";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";

const { ObjectId } = mongodb;
import { dateFourWeeksAgo, now } from "../../commons/model";
import { PaymentServiceClient } from "../../../db";

const MongoClient = mongodb.MongoClient;

function TagAgentBasedOnGoalStatus() {
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

      const paymentClient = PaymentServiceClient();

      const fourWeeksBack = dateFourWeeksAgo();
      const currentTime = now();

      dbo
        .collection("user")
        .find({})
        .toArray(function (err, users) {
          async.forEachOf(
            users,
            async (user, key, callback) => {
              const userId = user._id;

              // compute all user transaction query by user id
              const transactionsQuery = {
                text:
                  "SELECT profile.time_updated AS timeMapped, " +
                  "SUM(CASE WHEN tnx.status = 'SUCCESS' THEN tnx.amount else 0 END) AS successfulAmount " +
                  "FROM public.transactions AS tnx JOIN public.terminals profile ON profile.terminal_id = tnx.callback_response -> 'callback_response' ->> 'terminalID' " +
                  "WHERE tnx.type = 'TERMINAL' AND tnx.time_created >= $1 AND tnx.time_created <= $2 AND profile.user_id = $3 " +
                  "GROUP BY profile.time_updated",
                values: [`${fourWeeksBack}`, `${currentTime}`, `${userId}`],
              };

              const results = await paymentClient.query(transactionsQuery);

              if (results.rows.length !== 0) {
                const uniqueUserId = ObjectId(user._id);
                const dateMappedAndAmount = results.rows[0];
                const dateMapped = dateMappedAndAmount.timemapped;
                const totalTransactionWithinFourWeeks = parseFloat(
                  dateMappedAndAmount.successfulamount || 0
                );
                let goal = "ACTIVE";

                //Every agent who was onboarded(got terminal mapped) before the 15th of October are automatically moved to successful(Completed)
                if (
                  dateMapped <
                    moment("10/15/2020 0:00", "M/D/YYYY H:mm").valueOf() ||
                  (dateMapped >= fourWeeksBack &&
                    totalTransactionWithinFourWeeks >= 6000000)
                ) {
                  goal = "COMPLETED";
                } else if (dateMapped < fourWeeksBack) {
                  goal = "PENDING";
                }

                if (user.goal === "ACTIVE") {
                  dbo.collection("user").findOneAndUpdate(
                    { _id: uniqueUserId },
                    {
                      $set: {
                        goal,
                      },
                    },
                    { new: true },
                    function (err, doc) {
                      logger.info(
                        `Goal Status info updated for user [${uniqueUserId}], Error is ${err}`
                      );

                      if (callback !== undefined) {
                        callback();
                      }
                    }
                  );
                }
              }
            },
            (err) => {
              logger.info(
                `Users acquisition status info updated with error = ${err}`
              );
              paymentClient.end();
            }
          );
        });
    }
  );
}

// run job at every 3:00 A.M
export const TagAgentBasedOnGoalStatusJob = (): CronJob => {
  return new CronJob(
    "0 0 3 * * *",
    function () {
      const formattedDate = moment.tz("Africa/Lagos");
      logger.info(`::: Automatic Tagging of agents @ ${formattedDate} :::`);
      TagAgentBasedOnGoalStatus();
    },
    undefined,
    true,
    "Africa/Lagos"
  );
};
