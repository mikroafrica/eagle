import logger from "./logger";
import server from "./server";
import { RetryTransferJob } from "./api/resources/transaction-service/requery/transfer";
import { RetryWalletTopUpJob } from "./api/resources/transaction-service/requery/walletop-up";

// handle all uncaught errors
// process.on("uncaughtException", function (err) {
//   logger.error(`uncaught error has been fired with Error: ${err}`);
// });

RetryTransferJob().start();
RetryWalletTopUpJob().start();

const port = process.env.PORT || 3000;
server.listen(port, function () {
  logger.info(`App running @ port ${port}`);
});
