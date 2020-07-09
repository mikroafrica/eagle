import { UN_AVAILABLE } from "../../modules/status";

export const post = ({ client, path, params }) => {
  const clientRequest = client();
  return new Promise((resolve, reject) => {
    clientRequest.post(path, params, function (err, req, res, data) {
      if (err) {
        reject({
          message: err.message
            ? err.body.message
              ? err.body.message
              : err.message
            : err,
          statusCode: res.statusCode || UN_AVAILABLE,
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
    clientRequest.put(path, params, function (err, req, res, data) {
      if (err) {
        reject({
          message: err.message
            ? err.body.message
              ? err.body.message
              : err.message
            : err,
          statusCode: res.statusCode || UN_AVAILABLE,
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
    clientRequest.get(path, function (err, req, res, data) {
      if (err) {
        reject({
          message: err.message
            ? err.body.message
              ? err.body.message
              : err.message
            : err,
          statusCode: res.statusCode || UN_AVAILABLE,
        });
      } else {
        resolve({ data, statusCode: res.statusCode });
      }
    });
  });
};
