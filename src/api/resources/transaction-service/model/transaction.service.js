import { Transaction } from "./transaction.model";

export const findByTransactionReference = (transactionReference: string) => {
  return Transaction.findOne({ transactionReference });
};

export const saveTransaction = (transactionReference: string) => {
  return Transaction.create({ transactionReference, retryCount: 0 });
};

export const updateByTransactionReference = (
  transactionReference: string,
  retryCount: number,
  reProcessCount: number
) => {
  return Transaction.findOneAndUpdate(
    { transactionReference },
    { $set: { retryCount, reProcessCount } },
    { new: true }
  ).exec();
};
