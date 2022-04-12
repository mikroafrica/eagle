import request from "request-promise";
import { UN_AVAILABLE } from "./status";
import logger from "../../logger";

export const post = ({ client, path, params }) => {
  const clientRequest = client();
  return new Promise((resolve, reject) => {
    clientRequest.post(path, params, (err, req, res, data) => {
      if (err) {
        console.log(err);
        reject({
          message: err.message
            ? err.body
              ? err.body.message
              : err.message
            : err,
          statusCode: res ? res.statusCode : UN_AVAILABLE,
        });
      } else {
        resolve({ data, statusCode: res.statusCode });
      }
    });
  });
};

export const put = ({ client, path, params }) => {
  const clientRequest = client();
  return new Promise((resolve, reject) => {
    clientRequest.put(path, params, (err, req, res, data) => {
      if (err) {
        console.log(err);
        reject({
          message: err.message
            ? err.body
              ? err.body.message
              : err.message
            : err,
          statusCode: res ? res.statusCode : UN_AVAILABLE,
        });
      } else {
        resolve({ data, statusCode: res.statusCode });
      }
    });
  });
};

export const get = ({ client, path }) => {
  const clientRequest = client();
  return new Promise((resolve, reject) => {
    clientRequest.get(path, (err, req, res, data) => {
      if (err) {
        console.log(err);
        reject({
          message: err.message
            ? err.body
              ? err.body.message
              : err.message
            : err,
          statusCode: res ? res.statusCode : UN_AVAILABLE,
        });
      } else {
        resolve({ data, statusCode: res.statusCode });
      }
    });
  });
};

export const del = ({ client, path }) => {
  const clientRequest = client();
  return new Promise((resolve, reject) => {
    clientRequest.del(path, (err, req, res, data) => {
      if (err) {
        console.log(err);
        reject({
          message: err.message
            ? err.body
              ? err.body.message
              : err.message
            : err,
          statusCode: res ? res.statusCode : UN_AVAILABLE,
        });
      } else {
        resolve({ data, statusCode: res.statusCode });
      }
    });
  });
};

/**
 *
 * @param uri
 * @param method  : http methods, POST, PUT, etc
 * @param headers : request header
 * @param payLoad : request body
 * @param retries : number of times the request should be retried
 * @param backOff : time taken to waite before retrying again
 * @returns {Promise<unknown>}
 *
 * adopted from: https://blog.bearer.sh/add-retry-to-api-calls-javascript-node/
 */
export const requestBody = async (
  uri: string,
  method: string,
  headers: any = {},
  payLoad: any = {},
  retries: number = 3,
  backOff: number = 300
) => {
  const options = {
    method,
    uri,
    headers: {
      ...headers,
      "User-Agent": new Date().getTime(),
    },
    body: payLoad,
    json: true,
  };

  return new Promise(async (resolve, reject) => {
    try {
      const response = await request(options);
      resolve(response);
    } catch (e) {
      console.error(e);
      logger.error(`::: Request failed with error [${JSON.stringify(e)}] :::`);

      if (retries > 0) {
        setTimeout(
          () =>
            requestBody(
              uri,
              method,
              headers,
              payLoad,
              retries - 1,
              backOff * 3
            ),
          backOff
        );
      } else {
        reject(e);
      }
    }
  });
};
