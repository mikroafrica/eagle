import { PaymentServiceClient } from "./db";
import async from "async";
import logger from "../logger";
import mongodb from "mongodb";

const MongoClient = mongodb.MongoClient;

export const runUpdate = () => {
  console.log("terminal update running");
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

      const client = PaymentServiceClient();

      dbo
        .collection("user-address")
        .find({})
        .toArray(function (err, userAddresses) {
          async.forEachOf(
            userAddresses,
            (userAddress, key, callback) => {
              const userId = userAddress.userId;

              // compute all user transaction query by user id
              // update terminals set state = 'lagos', lga = 'ikeja' where user_id = '4564546456stry3456'
              const query = {
                text:
                  "update public.terminals set state = $1, lga = $2 where user_id = $3",

                values: [
                  `${userAddress.state}`,
                  `${userAddress.lga}`,
                  `${userId}`,
                ],
              };
              client
                .query(query)
                .then((response) => {
                  const results = response.rows;
                  logger.info(results);

                  logger.info(`user with user id ${userId} updated`);
                })
                .catch((error) => {
                  logger.error(
                    `error occurred while updating for user [${userId}] with error [${error}]`
                  );
                  callback();
                });
            },
            (err) => {
              logger.info(`Users state and lga updated`);
              client.end();
            }
          );
        });
    }
  );
};
