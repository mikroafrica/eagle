import mongoose from "mongoose";

const schema = {
  transactionReference: {
    type: String,
    unique: true,
  },
  retryCount: {
    type: Number,
  },

  reProcessCount: {
    type: Number
  },
};

const transactionSchema = new mongoose.Schema(schema, { timestamps: true });

transactionSchema.index({ transactionReference: 1 });

export const Transaction = mongoose.model("transaction", transactionSchema);
