import cron from "cron";

const CronJob = cron.CronJob;

import async from "async";
import logger from "../../../../logger";
import moment from "moment";
import mongodb from "mongodb";

const { ObjectId } = mongodb;
import { dateFourWeeksAgo, now } from "../../commons/model";
import { PaymentServiceClient, TransactionServiceClient } from "../../../db";

const MongoClient = mongodb.MongoClient;

const getMonthBoundaries = (monthsBack) => {
  const month = moment().subtract(monthsBack, "months");

  const firstDay = month.startOf("month").format("YYYY-MM-DD");
  const firstDayInMilli = moment(`${firstDay} 00:00:00`)
    .tz("Africa/Lagos")
    .format("x");

  const lastDay = month.endOf("month").format("YYYY-MM-DD");
  const lastDayInMilli = moment(`${lastDay} 00:00:00`)
    .tz("Africa/Lagos")
    .format("x");

  return [firstDayInMilli, lastDayInMilli];
};

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
      const transactionClient = TransactionServiceClient();

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

              let goal = "ACTIVE";

              if (terminalResults.rows.length !== 0) {
                const time_onboarded = terminalResults.rows[0].time_updated;

                let i = 0;
                let month_start = Number.POSITIVE_INFINITY;
                let month_end = Number.POSITIVE_INFINITY;

                //Perform this background check for each user. Would be removed after the users have been updated.
                while (time_onboarded <= month_start) {
                  const monthBoundaries = getMonthBoundaries(i);
                  month_start = monthBoundaries[0];
                  month_end = monthBoundaries[1];
                  i += 1;

                  const query = {
                    text:
                      "SELECT SUM(CASE WHEN status.name = 'successful' THEN tnx.amount ELSE 0 END) AS successfulAmount " +
                      "FROM public.transactions AS tnx JOIN public.transaction_types AS tnxType ON tnx.transaction_type = tnxType.id " +
                      "JOIN public.transaction_statuses status ON status.id = tnx.transaction_status " +
                      "WHERE tnx.time_created >= $1 AND tnx.time_created <= $2 AND tnx.user_id = $3",

                    values: [`${month_start}`, `${month_end}`, `${userId}`],
                  };
                  const transactionResults = await transactionClient.query(
                    query
                  );

                  if (transactionResults.rows.length !== 0) {
                    const totalTransactionPerMonth = parseFloat(
                      transactionResults.rows[0].successfulamount || 0
                    );

                    //If the user attained a total transaction of 6 Million during any month, assign a goal of COMPLETED and break out
                    if (totalTransactionPerMonth >= 6000000) {
                      goal = "COMPLETED";
                      break;
                    }
                  }
                }
              }

              const uniqueUserId = ObjectId(user._id);
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
            },
            (err) => {
              logger.info(
                `Users acquisition status info updated with error = ${err}`
              );
              paymentClient.end();
              transactionClient.end();
            }
          );
        });
    }
  );
}

// run job once at 11:00 P.M
export const TagAgentBasedOnGoalStatusJob = (): CronJob => {
  return new CronJob(
    "0 0 23 * * *",
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
