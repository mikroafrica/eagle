import cron from "cron";

const CronJob = cron.CronJob;

import async from "async";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";

const { ObjectId } = mongodb;
import { dateFourWeeksAgo, now } from "../../commons/model";
import { TransactionServiceClient, PaymentServiceClient } from "../../../db";

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

      const transactionClient = TransactionServiceClient();

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

              const terminalQuery = {
                text: "SELECT * FROM terminals tl " + "WHERE tl.user_id = $1",

                values: [`${userId}`],
              };

              const terminalResults = await paymentClient.query(terminalQuery);

              // compute all user transaction query by user id
              const query = {
                text:
                  "SELECT SUM(CASE WHEN status.name = 'successful' THEN tnx.amount ELSE 0 END) AS successfulAmount " +
                  "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                  "JOIN public.transaction_statuses status ON  status.id = tnx.transaction_status " +
                  "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                values: [`${fourWeeksBack}`, `${currentTime}`, `${userId}`],
              };

              const transactionResults = await transactionClient.query(query);

              if (
                terminalResults.rows.length !== 0 &&
                transactionResults.rows.length !== 0
              ) {
                const uniqueUserId = ObjectId(user._id);
                const terminal = terminalResults.rows[0];
                const totalTransactionDetails = transactionResults.rows[0];
                const totalTransactionWithinFourWeeks = parseFloat(
                  totalTransactionDetails.successfulamount || 0
                );

                let goal = "ACTIVE";

                if (
                  terminal.timeUpdated <
                    moment("10/15/2020 0:00", "M/D/YYYY H:mm").valueOf() ||
                  (terminal.timeUpdated >= fourWeeksBack &&
                    totalTransactionWithinFourWeeks >= 6000000)
                ) {
                  goal = "COMPLETED";
                } else if (terminal.timeUpdated < fourWeeksBack) {
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
              transactionClient.end();
              paymentClient.end();
            }
          );
        });
    }
  );
}

// run job at every 1:00 A.M
export const TagAgentBasedOnGoalStatusJob = (): CronJob => {
  return new CronJob(
    "0 0 1 * * *",
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
