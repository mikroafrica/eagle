import restify from "restify-clients";
import { get, post } from "./request.js";

const request = () => {
  const client = restify.createJSONClient({
    url: process.env.REPORT_SERVICE_URL,
    version: "*",
  });

  client.basicAuth(
    process.env.REPORT_SERVICE_USERNAME,
    process.env.REPORT_SERVICE_PASSWORD
  );
  return client;
};

export const ReportIndex = {
  WALLET_TRANSACTION: "mikro-wallet-transaction",
  TRANSACTION: "mikro-transaction-report",
  User: "mikro-user",
  TICKET: "mikro-user-ticket",
  NUBAN_TRANSACTION: "mikro-nuban-transactions",
};

export const search = (query) =>
  post({ client: request, params: query, path: "/report/raw" });

export type HitResponse = {
  total: any,
  max_score: number,
  hits: Hit[],
};

export type Hit = {
  _index: string,
  _type: string,
  _id: string,
  _score: string,
  _source: any,
};

export type Aggregation = {
  label: string,
  field: string,
  operator: string,
};

export const buildAggregation = (aggregation: Aggregation[]) => {
  let builder = {};
  for (const object: Aggregation of aggregation) {
    const { label, operator, field } = object;
    const queryString = `{"${label}":{"${operator}":{"field":"${field}"}}}`;

    builder = Object.assign(builder, JSON.parse(queryString));
  }
  return { ...builder };
};

export const handleListOfHits = (response) => {
  const hitResponse: HitResponse = response.hits;
  const docs = transformDoc(hitResponse.hits);
  return {
    list: docs,
    total: hitResponse.total.value,
  };
};

const transformDoc = (hits: any) => {
  if (Array.isArray(hits)) {
    return hits.map((docs) => {
      const { _source } = docs;
      return _source;
    });
  }
  return hits._source;
};
