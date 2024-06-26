import logger from "./logger";
import server from "./server";
import { RetryTransferJob } from "./api/resources/transaction-service/requery/transfer";
import { RetryWalletTopUpJob } from "./api/resources/transaction-service/requery/walletop-up";
import { RetryWithdrawalJob } from "./api/resources/transaction-service/requery/withdrawal/requery.withdrawal";
import {
  RetryPaymentTerminalJob,
  RetryPaymentWalletTopAndUSSDJob,
} from "./api/resources/payment-service/requery.payment";
import { RetryBillsJob } from "./api/resources/transaction-service/requery/bills";
import { RetryReportJob } from "./api/resources/transaction-service/requery/report/transaction.report";

// handle all uncaught errors
process.on("uncaughtException", function (err) {
  logger.error(`uncaught error has been fired with Error: ${err}`);
});

// RetryTransferJob().start();
// RetryWalletTopUpJob().start();

// RetryPaymentWalletTopAndUSSDJob().start();
// RetryPaymentTerminalJob().start();
// RetryBillsJob().start();
// RetryWithdrawalJob().start();
RetryReportJob().start();

const port = process.env.PORT || 80;
server.listen(port, function () {
  logger.info(`App running @ port ${port}`);
});
