import cron from "cron";
const CronJob = cron.CronJob;

import moment from "moment";
import logger from "../../../../../logger";
import {
  handleListOfHits,
  ReportIndex,
  search,
} from "../../../../modules/report-service";
import {
  pastMinutes,
  previousDayInMorning,
  TransactionMessagingType,
  TransactionStatus,
} from "../../../commons/model";
import { PaymentType } from "../transfer/retry.transfer";
import reQueryTransferEvent, {
  REQUERY_TRANSACTION_EMITTER,
} from "../transfer/requery.transfer.event";
import reQueryBillEvent, {
  RE_QUERY_BILL_EMITTER,
} from "../bills/requery.bills.event";
import reQueryWithdrawalEmitter, {
  REQUERY_WITHDRAWAL_EMITTER,
} from "../withdrawal/requery.withdrawal-event";

const ReportTransactionType = {
  Transfer: "TRANSFER",
  Data: "DATA",
  CableTv: "CABLE_TV",
  Phcn: "PHCN",
  Airtime: "AIRTIME",
  Withdrawal: "WITHDRAWAL",
  Payout: "PAYOUT",
};

export const ReportTransactionStatus = {
  Pending: "PENDING",
  PaymentSuccessful: "PAYMENT SUCCESSFUL",
  BillPurchasedFailed: "BILL PURCHASED FAILED",
  PendingReversal: "PAYMENT REVERSED PEND",
};

const query = ({ startTime, endTime }) => {
  const must = [];

  must.push({
    range: {
      timeCreated: {
        lte: endTime,
      },
    },
  });

  must.push({
    range: {
      timeCreated: {
        gte: startTime,
      },
    },
  });

  must.push({
    bool: {
      should: [
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Phcn,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Airtime,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Data,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.CableTv,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Transfer,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Withdrawal,
          },
        },
        {
          match: {
            "transactionType.keyword": ReportTransactionType.Payout,
          },
        },
      ],
      minimum_should_match: 1,
    },
  });
  must.push({
    bool: {
      should: [
        {
          match: {
            "transactionStatus.keyword":
              ReportTransactionStatus.BillPurchasedFailed,
          },
        },
        {
          match: {
            "transactionStatus.keyword": ReportTransactionStatus.Pending,
          },
        },
        {
          match: {
            "transactionStatus.keyword":
              ReportTransactionStatus.PaymentSuccessful,
          },
        },
      ],
      minimum_should_match: 1,
    },
  });

  const query = {
    index: ReportIndex.TRANSACTION,
    from: 0,
    size: 150,
    _source: [
      "meta",
      "reference",
      "vendor",
      "userId",
      "amount",
      "product",
      "userdata",
      "product",
      "timeUpdated",
      "transactionType",
      "transactionStatus",
      "customerBillerId",
      "destinationWalletId",
      "uniqueIdentifier",
    ],
    body: {
      query: {
        bool: {
          must,
        },
      },
      sort: {
        timeCreated: { order: "desc" },
      },
    },
  };

  logger.info(
    `::: Transaction query request is [${JSON.stringify(query)}] :::`
  );
  return query;
};

const QueryPendingTransactions = async () => {
  try {
    const previousDay = previousDayInMorning();
    const pastThreeMinutes = pastMinutes(2);
    const startTime = new Date(previousDay);
    const endTime = new Date(pastThreeMinutes);
    const queryResponse = await search(query({ startTime, endTime }));
    const { data: queryResponseData } = queryResponse.data;
    const { list: transactionList, total } =
      handleListOfHits(queryResponseData);

    logger.info(`::: Total transaction report list is [${total}] :::`);

    const paymentDtoList = [];
    const billingDtoList = [];
    const withdrawalList = [];

    for (let i = 0; i < transactionList.length; i++) {
      const transaction = transactionList[i];

      const amount = parseFloat(transaction.amount);

      if (transaction.transactionType === ReportTransactionType.Transfer) {
        const paymentDto = {
          userId: transaction.userId,
          amount: amount,
          bankCode: transaction.product,
          customerName: transaction.userdata.name,
          remarks: transaction.userdata.remarks,
          transactionRef: transaction.reference,
          accountNumber: transaction.customerBillerId,
          type: PaymentType.BANK_TRANSFER_REQUERY,
          paymentDate: new Date(transaction.timeUpdated).getTime(),
          terminalId: transaction.userdata?.meta?.terminalId,
          serialNumber: transaction.userdata?.meta?.serialNumber,
          vendor: transaction?.userdata?.meta?.dedicatedVendorName,
          isDirectPayment: transaction?.userdata?.meta?.isVendorProcessInstant,
          cashInUrl: transaction?.userdata?.meta?.cashInUrl,
          cashInSecretKey: transaction?.userdata?.meta?.cashInSecretKey,
          shouldTransferUseNibssCode:
            transaction?.userdata?.meta?.shouldTransferUseNibssCode,
        };
        paymentDtoList.push(paymentDto);
      }

      if (transaction.transactionType === ReportTransactionType.Withdrawal) {
        let status = TransactionStatus.PENDING;
        if (
          transaction.transactionStatus ===
            ReportTransactionStatus.PaymentSuccessful ||
          transaction.transactionStatus ===
            ReportTransactionStatus.BillPurchasedFailed
        ) {
          status = TransactionStatus.SUCCESS;
        }

        if (
          transaction.transactionStatus ===
          ReportTransactionStatus.PendingReversal
        ) {
          status = TransactionStatus.REVERSAL;
        }

        const withdrawalDto = {
          paymentReference: transaction.uniqueIdentifier,
          amount: amount,
          vendor: transaction.vendor,
          paymentStatus: status,
          type: TransactionMessagingType.TERMINAL,
          terminalId: transaction.customerBillerId,
          userId: transaction.userId,
          walletId: transaction.destinationWalletId,
          timeCreated: new Date(transaction.timeUpdated).getTime(),
        };
        withdrawalList.push(withdrawalDto);
      }

      if (
        transaction.transactionType === ReportTransactionType.Phcn ||
        transaction.transactionType === ReportTransactionType.Data ||
        transaction.transactionType === ReportTransactionType.CableTv ||
        transaction.transactionType === ReportTransactionType.Airtime
      ) {
        const billingDto = {
          transactionReference: transaction.reference,
          vendor: transaction.vendor
            ? transaction.vendor.toLocaleLowerCase()
            : transaction?.userdata?.meta?.dedicatedVendorName || "",
          phoneNumber:
            transaction.meta.data.customerPhoneNumber ||
            transaction.customerBillerId,
          amount: amount,
          productId: transaction.meta.data.productId,
          meterNumber: transaction.customerBillerId,
          type: transaction.transactionType.toLocaleLowerCase(),
          smartCardNumber: transaction.customerBillerId,
          category: transaction.transactionType.toLocaleLowerCase(),
          terminalId: transaction.userdata?.meta?.terminalId,
          serialNumber: transaction.userdata?.meta?.serialNumber,
          isDirectPayment: transaction?.userdata?.meta?.isVendorProcessInstant,
          cashInUrl: transaction?.userdata?.meta?.cashInUrl,
          cashInSecretKey: transaction?.userdata?.meta?.cashInSecretKey,
        };
        billingDtoList.push(billingDto);
      }
    }

    if (paymentDtoList.length > 0) {
      reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
    }

    if (billingDtoList.length > 0) {
      reQueryBillEvent.emit(RE_QUERY_BILL_EMITTER, billingDtoList);
    }

    if (withdrawalList.length > 0) {
      reQueryWithdrawalEmitter.emit(REQUERY_WITHDRAWAL_EMITTER, withdrawalList);
    }
  } catch (err) {
    logger.error(
      `::: failed to fetch report transactions with error [${JSON.stringify(
        err
      )}] :::`
    );
  }
};

// run job every one minutes
export const RetryReportJob = (): CronJob => {
  return new CronJob("0 */3 * * * *", function () {
    const formattedDate = moment.tz("Africa/Lagos");
    logger.info(
      `::: reQuery for pending transaction reports started ${formattedDate} :::`
    );

    QueryPendingTransactions()
      .then((paymentDtoList) => {
        // reQueryTransferEvent.emit(REQUERY_TRANSACTION_EMITTER, paymentDtoList);
      })
      .catch((err) => {
        logger.error(
          `error occurred while publishing transfer result: ${err} `
        );
      });
  });
};
