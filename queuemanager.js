var amqp = require('amqplib/callback_api');
const axios = require('axios');
const CLOUDAMQP_URL = 'amqp://vykiekcg:dLrVvI_hTTiqrv8lKpp5yuoS_NJc8dZn@wasp.rmq.cloudamqp.com/vykiekcg'
const queueURL = 'http://localhost:3000/sendToQueue'
// if the connection is closed or fails to be established at all, we will reconnect
var amqpConn = null;
function start() {
  amqp.connect(CLOUDAMQP_URL + "?heartbeat=60", function (err, conn) {
    if (err) {
      console.error("[AMQP]", err.message);
      return setTimeout(start, 1000);
    }
    conn.on("error", function (err) {
      if (err.message !== "Connection closing") {
        console.error("[AMQP] conn error", err.message);
      }
    });
    conn.on("close", function () {
      console.error("[AMQP] reconnecting");
      return setTimeout(start, 1000);
    });
    console.log("[AMQP] connected");
    amqpConn = conn;
    whenConnected();
  });
}

function whenConnected() {
  startWorker();
}

var pubChannel = null;
var offlinePubQueue = [];
// A worker that acks messages only if processed succesfully
function startWorker() {
  amqpConn.createChannel(function (err, ch) {
    if (closeOnErr(err)) return;
    ch.on("error", function (err) {
      console.error("[AMQP] channel error", err.message);
    });

    ch.on("close", function () {
      console.log("[AMQP] channel closed");
    });

    ch.prefetch(10);
    ch.assertQueue("scanman-upload", { durable: true }, function (err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("scanman-upload", processMsg, { noAck: false });
      console.log("Ready and picking messages from upload queue");
    });

    ch.assertQueue("scanman-approvalprocess", { durable: true }, function (err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("scanman-approvalprocess", processMsg, { noAck: false });
      console.log("Ready and picking messages from Approval queue");
    });

    ch.assertQueue("scanman-voucherprocess", { durable: true }, function (err, _ok) {
      if (closeOnErr(err)) return;
      ch.consume("scanman-voucherprocess", processMsg, { noAck: false });
      console.log("Ready and picking messages from Voucher queue");
    });

    function processMsg(msg) {
      work(msg, function (ok) {
        try {
          if (ok)
            ch.ack(msg);
          else
            ch.reject(msg, true);
        } catch (e) {
          closeOnErr(e);
        }
      });
    }
  });
}

function work(msg, cb) {
  console.log("Message from queue ", msg.fields.routingKey)
  console.log("Got msg ", msg.content.toString());
  if (msg.fields.routingKey === 'scanman-upload') {
    // Assume some upload processing is done, and we now queue this up to voucher queue
    var queueData = {
      exchange: "",
      routingKey: "scanman-voucherprocess",
      content: {
        ContentHeading: "From Queue Manager",
        ContentMessage: "PDF uploaded. Ready for voucher process"
      }
    }
    axios.post(queueURL, queueData).then((response) => { });
  } else if (msg.fields.routingKey === 'scanman-voucherprocess') {
    // Assume some voucher processing is done, and we now queue this up to approval queue
    var queueData = {
      exchange: "",
      routingKey: "scanman-approvalprocess",
      content: {
        ContentHeading: "From Queue Manager",
        ContentMessage: "Voucher Processed. Ready for Approval process"
      }
    }
    axios.post(queueURL, queueData).then((response) => { });
  }
  cb(true);
}

function closeOnErr(err) {
  if (!err) return false;
  console.error("[AMQP] error", err);
  amqpConn.close();
  return true;
}

start();