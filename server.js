'use strict';

// Module imports
var express = require('express')
  , http = require('http')
  , https = require('https')
  , bodyParser = require('body-parser')
  , kafka = require('kafka-node')
  , async = require('async')
  , _ = require('lodash')
  , QUEUE = require('block-queue')
  , log = require('npmlog-ts')
  , cors = require('cors')
  , commandLineArgs = require('command-line-args')
  , getUsage = require('command-line-usage')
  , util = require('util')
;

// Instantiate classes & servers
const restURI      = '/kafka/send/:topic'
    , CONNECTED    = "CONNECTED"
    , DISCONNECTED = "DISCONNECTED"
    , RESTPORT     = 10200
;

const options = {
  cert: fs.readFileSync("/u01/ssl/certificate.fullchain.crt").toString(),
  key: fs.readFileSync("/u01/ssl/certificate.key").toString()
};

var restapp        = express()
//  , restserver     = http.createServer(restapp)
  , restserver     = https.createServer(options, restapp)
  , Producer       = kafka.Producer
  , kafkaClient    = _.noop()
  , kafkaProducer  = _.noop()
  , kafkaCnxStatus = DISCONNECTED;
;

// ************************************************************************
// Main code STARTS HERE !!
// ************************************************************************

log.stream = process.stdout;
log.timestamp = true;

// Main handlers registration - BEGIN
// Main error handler
process.on('uncaughtException', function (err) {
  log.info("","Uncaught Exception: " + err);
  log.info("","Uncaught Exception: " + err.stack);
});
// Detect CTRL-C
process.on('SIGINT', function() {
  log.info("","Caught interrupt signal");
  log.info("","Exiting gracefully");
  process.exit(2);
});
// Main handlers registration - END

// Initialize input arguments
const optionDefinitions = [
  { name: 'zookeeperhost', alias: 'z', type: String },
  { name: 'help', alias: 'h', type: Boolean },
  { name: 'verbose', alias: 'v', type: Boolean, defaultOption: false }
];

const sections = [
  {
    header: 'Kafka Wrapper',
    content: 'Kafka wrapper to send messages through REST API'
  },
  {
    header: 'Options',
    optionList: [
      {
        name: 'zookeeperhost',
        typeLabel: '{underline ipaddress:port}',
        alias: 'z',
        type: String,
        description: 'Zookeeper address for Kafka messaging'
      },
      {
        name: 'verbose',
        alias: 'v',
        description: 'Enable verbose logging.'
      },
      {
        name: 'help',
        alias: 'h',
        description: 'Print this usage guide.'
      }
    ]
  }
]
const options = commandLineArgs(optionDefinitions);

const valid =
  options.help ||
  (
    options.zookeeperhost
  );

if (!valid) {
  console.log(getUsage(sections));
  process.exit(-1);
}

log.level = (options.verbose) ? 'verbose' : 'info';

// REST engine initial setup
restapp.use(bodyParser.urlencoded({ extended: true }));
restapp.use(bodyParser.json());
restapp.use(cors());

var servers = [];

// Initializing QUEUE variables BEGIN
var inboundQueue  = []
  , queueConcurrency = 1
;
// Initializing QUEUE variables END

function startKafka(cb) {
  kafkaClient = new kafka.Client(options.zookeeperhost, "RETAIL", {sessionTimeout: 1000});
  kafkaClient.zk.client.on('connected', () => {
    kafkaCnxStatus = CONNECTED;
    log.verbose("", "[Kafka] Server connected!");
  });
  kafkaClient.zk.client.on('disconnected', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.verbose("", "[Kafka] Server disconnected!");
  });
  kafkaClient.zk.client.on('expired', () => {
    kafkaCnxStatus = DISCONNECTED;
    log.verbose("", "[Kafka] Server disconnected!");
  });
  kafkaProducer = new Producer(kafkaClient);
  kafkaProducer.on('ready', () => {
    log.info("", "[Kafka] Producer ready");
    if (inboundQueue.length > 0) {
      // Sent pending messages
      log.info("", "[Kafka] Sending %d pending messages...", inboundQueue.length);

      async.reject(inboundQueue, (msg, callback) => {
        kafkaProducer.send([{ topic: msg.topic, messages: JSON.stringify(msg.payload), partition: 0 }], (err, data) => {
          if (err) {
            log.error("", err);
            // Abort resending
            callback(err, true);
          } else {
            log.verbose("", "[Kafka] Message sent to topic %s, partition %s and id %d", Object.keys(data)[0], Object.keys(Object.keys(data)[0])[0], data[Object.keys(data)[0]][Object.keys(Object.keys(data)[0])[0]]);
            callback(err, false);
          }
        });
      }, (err, results) => {
        if (err) {
          log.error(err)
        } else {
          log.info("", "Done");
        }
      });
    }
  });
  kafkaProducer.on('error', (err) => {
    log.error("", "Error initializing KAFKA producer: " + err.message);
  });
  if (typeof(cb) == 'function') cb(null);
}

function stopKafka(cb) {
  if (kafkaClient) {
    kafkaClient.close(() => {
      cb();
    });
  } else {
    cb();
  }
}

async.series([
    function(next) {
      // Initialize KAFKA producer
      log.verbose("", "[Kafka] Connecting to Zookeper host at %s...", options.zookeeperhost);
      startKafka(next);
    },
    function(next) {
      // Start REST server
      restserver.listen(RESTPORT, function() {
        log.info("","REST server running on http://localhost:" + RESTPORT + restURI);
        next(null);
      });
    }
], function(err, results) {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});

restapp.post(restURI, function(req,res) {
  res.status(204).end();
  log.verbose("","Incoming publish request to '%s' topic with payload: '%j'", req.params.topic, req.body);

  var message = {
    topic: req.params.topic,
    payload: req.body
  }

  if (kafkaCnxStatus !== CONNECTED || !kafkaProducer) {
    // Zookeeper connection lost, let's try to reconnect before giving up
    log.verbose("","[Kafka] Server not available. Enqueueing message");
    inboundQueue.push(message);
    log.verbose("","[Kafka] Trying to reconnect to Kafka server...");
    stopKafka(() => {
      log.verbose("","[Kafka] Kafka Object closed");
      startKafka();
    });
  } else {
    kafkaProducer.send([{ topic: message.topic, messages: JSON.stringify(message.payload), partition: 0 }], (err, data) => {
      if (err) {
        log.error("", err);
        log.verbose("","[Kafka] Server not available. Enqueueing message");
        inboundQueue.push(message);
      } else {
        log.verbose("", "[Kafka] Message sent to topic %s, partition %s and id %d", Object.keys(data)[0], Object.keys(Object.keys(data)[0])[0], data[Object.keys(data)[0]][Object.keys(Object.keys(data)[0])[0]]);
      }
    });
  }
});
