var express = require('express');
var bodyParser = require('body-parser');
var amqp = require('amqplib/callback_api');
const CLOUDAMQP_URL = 'amqp://vykiekcg:dLrVvI_hTTiqrv8lKpp5yuoS_NJc8dZn@wasp.rmq.cloudamqp.com/vykiekcg'

// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
function start() {
  amqp.connect(CLOUDAMQP_URL + "?heartbeat=60", function(err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function(err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function() {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    startPublisher();
  });
}

var pubChannel = null;
var offlinePubQueue = [];
function startPublisher() {
  amqpConn.createConfirmChannel(function(err, ch) {
    if (closeOnErr(err)) return;
      ch.on("error", function(err) {
      console.error("[AMQP] channel error", err.message);
    });
    ch.on("close", function() {
      console.log("[AMQP] channel closed");
    });

    pubChannel = ch;
    while (true) {
      var m = offlinePubQueue.shift();
      if (!m) break;
      publish(m[0], m[1], m[2]);
    }
  });
}

function publish(exchange, routingKey, content) {
  try {
    pubChannel.publish(exchange, routingKey, content, { persistent: true },
                      function(err, ok) {
                        if (err) {
                          console.error("[AMQP] publish", err);
                          offlinePubQueue.push([exchange, routingKey, content]);
                          pubChannel.connection.close();
                        }
                      });
  } catch (e) {
    console.error("[AMQP] publish", e.message);
    offlinePubQueue.push([exchange, routingKey, content]);
  }
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

var app = express();

const port = process.env.PORT || 3000;

app.use(bodyParser.json());
// app.use(bodyParser.urlencoded({extended : false}));

app.post('/sendToQueue', (req, res) => {
    // console.log(req.body);
    var newMessage = {
        exchange: req.body.exchange,
        routingKey: req.body.routingKey,
        content: req.body.content
    }

    console.log('Message received', newMessage);

    publish(newMessage.exchange, newMessage.routingKey, new Buffer.from(JSON.stringify(newMessage.content)));

    res.status(200).send();
})

app.listen(port, () => {
    console.log(`Server is running on port ${port}`)
    start()
});



