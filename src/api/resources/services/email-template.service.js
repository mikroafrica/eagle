import handlebars from "handlebars";
import fs from "fs";
import path from "path";

function populateHtml(filename, data) {
  var source = fs.readFileSync(filename, "utf8").toString();
  var template = handlebars.compile(source);
  var output = template(data);
  return output;
}

export async function generateAndSendMail(
  transactionData,
  sendToEmailCallback,
  email
) {
  const filePath = path.resolve(
    "./src/api/resources/email-templates/activity-report.html"
  );
  var populatedHtml = populateHtml(filePath, transactionData);
  sendToEmailCallback(populatedHtml, email);
}
