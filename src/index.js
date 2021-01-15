import logger from "./logger";
import server from "./server";
import { RetryTransferJob } from "./api/resources/transaction-service/requery/transfer";
import { RetryWalletTopUpJob } from "./api/resources/transaction-service/requery/walletop-up";
import { RetryWithdrawalJob } from "./api/resources/transaction-service/requery/withdrawal/requery.withdrawal";
import {
  RetryPaymentTerminalJob,
  RetryPaymentWalletTopUpJob,
} from "./api/resources/payment-service/requery.payment";
import { RetryBillsJob } from "./api/resources/transaction-service/requery/bills";

// handle all uncaught errors
process.on("uncaughtException", function (err) {
  logger.error(`uncaught error has been fired with Error: ${err}`);
});

RetryTransferJob().start();
RetryWalletTopUpJob().start();
RetryWithdrawalJob().start();

RetryPaymentWalletTopUpJob().start();
RetryPaymentTerminalJob().start();
RetryBillsJob().start();

const port = process.env.PORT || 9000;
server.listen(port, function () {
  logger.info(`App running @ port ${port}`);
});
