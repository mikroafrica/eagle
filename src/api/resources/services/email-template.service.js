import handlebars from "handlebars";
import fs from "fs";
import path from "path";
import htmlPdf from "html-pdf";

function populateHtml(filename, data) {
  var source   = fs.readFileSync(filename, 'utf8').toString();
  var template = handlebars.compile(source);
  var output = template(data);
  return output;
}

export async function generatePdfFromHtml(transactionData, sendToEmailCallback) {
    const filePath = path.resolve("./src/api/resources/email-templates/activity-report.html")
    var populatedHtml = populateHtml(filePath, transactionData);
    const htmlOptions = {
        type: "PDF", format: 'Letter', timeout: '100000'
    };
    return await htmlPdf.create(populatedHtml, htmlOptions)
        .toStream((err, result) => {
            if (err) return console.log(err);
            sendToEmailCallback(result, "test.pdf")
        })
    
}
