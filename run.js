const express = require('express');
const parse = require('csv-parse');
const request = require('request');
const config = require('./config');

const not_available_column = "";

var app = express();

let urlToColumns = {};
config.columns.forEach((column, resultIndex) => {
  if (urlToColumns[column.url] === undefined) {
    urlToColumns[column.url] = [];
  }
  urlToColumns[column.url].push({proxyIndex: column.column, resultIndex: resultIndex});
});

app.get('/', function (req, res) {

  let ipToColumns = [];
  let readyCount = 0;
  Object.keys(urlToColumns).forEach(url => {
    request.get(url, {}, function (error, response, body)  {
      let columns = urlToColumns[url];
      var parser = parse({delimiter: ','});

      parser.on('readable', function() {
        while(record = parser.read()){
          //console.log(record);
          let ip = record[0];
          if (ipToColumns[ip] === undefined) {
            ipToColumns[ip] = [];
          }
          columns.forEach(column => {
            if (ipToColumns[ip][column.resultIndex] === undefined) {
              ipToColumns[ip][column.resultIndex] = [];
            }
            ipToColumns[ip][column.resultIndex] = record[column.proxyIndex];
          });
        }
      });

      parser.on('error', function(error) {
        console.log("error", error);
      });

      parser.on('end', function() {
        readyCount++;
        let allUrlsProcessed = readyCount === Object.keys(urlToColumns).length;
        if (allUrlsProcessed) {//all urls processed
          let result = Object.keys(ipToColumns).map(ip => {
            let row = [ip];
            for (let i = 0; i < config.columns.length; i++) {
              let column = ipToColumns[ip][i] === undefined ? not_available_column : ipToColumns[ip][i];
              row.push(column);
            }
            return row.map(column => "\""+column.toString().replace(/\"/g, "\"\"")+"\"").join(",");
          }).join("\n");
          res.set('Content-Type', 'text/csv');
          res.send(result);
        }
      });

      parser.write(body);
      parser.end();
    });
  });

  
});


app.listen(config.listen_port);
