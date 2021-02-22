import pg from "pg";

const Client = pg.Client;

import mongoose from "mongoose";

mongoose.Promise = global.Promise;

import logger from "../logger";

export const TransactionServiceClient = () => {
  const client = new Client({
    user: process.env.TRANSACTION_POSTGRES_USERNAME,
    host: process.env.TRANSACTION_POSTGRES_ENDPOINT,
    database: process.env.TRANSACTION_POSTGRESS_DATABASE_NAME,
    password: process.env.TRANSACTION_POSTGRES_PASSWORD,
    port: process.env.TRANSACTION_POSTGRES_PORT,
  });
  logger.info("connecting to transaction service database");

  client.connect();

  return client;
};

export const PaymentServiceClient = () => {
  const client = new Client({
    user: process.env.PAYMENT_POSTGRES_USERNAME,
    host: process.env.PAYMENT_POSTGRES_ENDPOINT,
    database: process.env.PAYMENT_POSTGRESS_DATABASE_NAME,
    password: process.env.PAYMENT_POSTGRES_PASSWORD,
    port: process.env.PAYMENT_POSTGRES_PORT,
  });
  logger.info("connecting to payment service database");

  client.connect();

  return client;
};

export const connect = () =>
  mongoose
    .connect(process.env.MONGODB_URI, {
      useNewUrlParser: true,
      useFindAndModify: false,
      useCreateIndex: true,
      useUnifiedTopology: true,
    })
    .then(() => logger.info("Database connected successfully"))
    .catch((e) => logger.error(`Failed to connect with error ${e}`));
