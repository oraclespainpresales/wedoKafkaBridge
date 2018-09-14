'use strict';

// Module imports
var kafka = require('kafka-node')
  , _ = require('lodash')
  , async = require('async')
  , log = require('npmlog-ts')
  , commandLineArgs = require('command-line-args')
  , getUsage = require('command-line-usage')
  , util = require('util')
;

// Instantiate classes & servers
const CONNECTED    = "CONNECTED"
    , DISCONNECTED = "DISCONNECTED"
    , RESTPORT     = 22222
;
var Consumer       = kafka.Consumer
  , kafkaClient    = _.noop()
  , kafkaConsumer  = _.noop()
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
  { name: 'kafkatopic', alias: 't', type: String },
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
        name: 'kafkatopic',
        typeLabel: '{underline topic}',
        alias: 't',
        type: String,
        description: 'Kafka topic to subscribe to'
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
var options = undefined;

try {
  options = commandLineArgs(optionDefinitions);
} catch (e) {
  console.log(getUsage(sections));
  console.log(e.message);
  process.exit(-1);
}

if (!options.zookeeperhost) {
  console.log(getUsage(sections));
  process.exit(-1);
}

if (options.help) {
  console.log(getUsage(sections));
  process.exit(0);
}

log.level = (options.verbose) ? 'verbose' : 'info';

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

  kafkaConsumer = new Consumer(
    kafkaClient, [ { topic: options.kafkatopic, partition: 0 } ], { autoCommit: true }
  );

  kafkaConsumer.on('message', (data) => {
    log.verbose("", "Incoming message on topic '%s', payload: %s", data.topic, data.value);
  });

  kafkaConsumer.on('ready', function () {
    console.log("kafkaConsumer ready");
  });

  kafkaConsumer.on('error', (err) => {
    log.error("", "Error initializing KAFKA consumer: " + err.message);
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
    }
], function(err, results) {
  if (err) {
    log.error("", err.message);
    process.exit(2);
  }
});
