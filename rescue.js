const async = require("async");
const pg = require("pg");
const Client = pg.Client;

const client = new Client({
  user: "postgres",
  host: "payment-production.c5mzaain2n7m.us-east-1.rds.amazonaws.com",
  database: "postgres",
  password: "JTMBvY5B7rtkavI4lSB6",
  port: 5432,
  // user: "postgres",
  // host: "127.0.0.1",
  // database: "postgres",
  // password: "root",
  // port: 5432,
});

client.connect();

const transactions = require("./rescue-value").transactions;

let count = 0;
// async.forEachOf(transactions, async (transaction, key, callback) => {
//     const transactionReference = `${transaction.terminalId}${transaction.retrievalReferenceNumber}${transaction.stan}`;
//
//     // const query = {
//     //   text: "select * from transactions where payment_reference = $1",
//     //
//     //   values: [transactionReference],
//     // };
//     // const results = await client.query(query);
//     // count += 1;
//     // if (results && results.rows.length > 0) {
//     //   console.log(count);
//     //   console.log(`transaction reference exist ${transactionReference}`);
//     // } else {
//
//   },
//   (err) => {
//     console.log(err);
//     console.log(amount);
//     console.log("done");
//   }
// );

transactions.forEach(function (transaction) {
  const transactionReference = `${transaction.terminalId}${transaction.retrievalReferenceNumber}${transaction.stan}`;
  console.log(transactionReference);
  const callbackResponse = { callback_response: transaction };
  let status = "";
  if (
    transaction.messageReason === "Approved" &&
    transaction.responseCode === 0
  ) {
    status = "SUCCESS";
  } else {
    status = "FAILED";
  }
  count += 1;

  const time = 1611694799000;
  client.query(
    "insert into transactions (amount, type, status, handshake_status, vendor, " +
      "payment_reference, callback_response, time_created, time_updated, customer_biller_id)" +
      " VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
    [
      transaction.amount,
      "TERMINAL",
      status,
      "INIT",
      "ITEX",
      transactionReference,
      callbackResponse,
      time,
      time,
      transaction.terminalId,
    ],
    (err, results) => {
      console.error(err);
      console.log(count);
    }
  );
});
