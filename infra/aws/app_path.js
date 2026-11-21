// CloudFront viewer-request function for the desk mobile app (infra/aws/app.tf).
// /<secret>/... -> app/... in the bucket; /<secret> -> redirect to /<secret>/; anything else -> 404.
function handler(event) {
  var request = event.request;
  var base = "/__APP_PATH__";
  if (request.uri === base) {
    return { statusCode: 302, statusDescription: "Found", headers: { location: { value: base + "/" } } };
  }
  if (request.uri.indexOf(base + "/") !== 0) {
    return { statusCode: 404, statusDescription: "Not Found" };
  }
  var rest = request.uri.substring(base.length);
  if (rest === "/") {
    rest = "/index.html";
  }
  request.uri = "/app" + rest;
  return request;
}
