var crypto = require('crypto');
var async = require('async');
var winston = require('winston');
var moment = require('moment');
var util = require('util');

var SYNC_LOGGER = 'SYNC';

exports.init = function(dataset_id, options, cb) {
  initDataset(dataset_id, options, cb);
};

exports.invoke = function(dataset_id, params, callback) {
  return doInvoke(dataset_id, params, callback);
};

exports.stop = function(dataset_id, callback) {
  return stopDatasetSync(dataset_id, callback);
};

exports.stopAll = function(dataset_id, callback) {
  return stopAllDatasetSync(dataset_id, callback);
};

exports.handleList = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.listHandler = fn;
    }
  });
};

exports.handleCreate = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.createHandler = fn;
    }
  });
};

exports.handleRead = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.readHandler = fn;
    }
  });
};

exports.handleUpdate = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.updateHandler = fn;
    }
  });
};

exports.handleDelete = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.deleteHandler = fn;
    }
  });
};

exports.handleCollision = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.collisionHandler = fn;
    }
  });
};

exports.listCollisions = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.collisionLister = fn;
    }
  });
};

exports.removeCollision = function(dataset_id, fn) {
  getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.collisionRemover = fn;
    }
  });
};

/* ======================================================= */
/* ================== PRIVATE FUNCTIONS ================== */
/* ======================================================= */

function initDataset(dataset_id, options, cb) {
  doLog(SYNC_LOGGER, 'info', 'initDataset - ' + dataset_id);
  var datasetConfig = JSON.parse(JSON.stringify(defaults));
  for (var i in options) {
    datasetConfig[i] = options[i];
  }

  setLogger(dataset_id, datasetConfig);

  createDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err, null);
    }
    dataset.config = datasetConfig;
    cb(null, {});
  });
}

function stopDatasetSync(dataset_id, cb) {
  doLog(dataset_id, 'info', 'stopDatasetSync');
  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err);
    }
    if( dataset.timeouts ) {
      for( i in dataset.timeouts ) {
        clearTimeout(dataset.timeouts[i]);
      }
    }

    removeDataset(dataset_id, cb);
  });
}

function stopAllDatasetSync(cb) {
  doLog(SYNC_LOGGER, 'info', 'stopAllDatasetSync');
  var datasets = [];
  async.forEachSeries(datasets, function(dataset, itemCallback) {
      datasets.push(dataset.id);
      stopDatasetSync(dataset.id, itemCallback);
    },
    function(err) {
      cb(err, datasets);
    });
}

function doInvoke(dataset_id, params, callback) {

  // Verify that fn param has been passed
  if( ! params || ! params.fn ) {
    doLog(dataset_id, 'warn', 'no fn parameter provided :: ' + util.inspect(params), params);
    return callback("no_fn", null);
  }

  var fn = params.fn;

  // Verify that fn param is valid
  var fnHandler = invokeFunctions[fn];
  if( ! fnHandler ) {
    return callback("unknown_fn : " + fn, null);
  }

  return fnHandler(dataset_id, params, callback);
}

function doListCollisions(dataset_id, params, cb) {
  getDataset(dataset_id, function(err, dataset) {
    if( err ) return cb(err);

    if( ! dataset.collisionLister ) {
      return cb("no_collisionLister", null);
    }

    dataset.collisionLister(dataset_id, cb);
  });
}

function doRemoveCollision(dataset_id, params, cb) {
  getDataset(dataset_id, function(err, dataset) {
    if( err ) return cb(err);

    if( ! dataset.collisionRemover ) {
      return cb("no_collisionRemover", null);
    }

    dataset.collisionRemover(dataset_id, params.hash, cb);
  });
}

function doSetLogLevel(dataset_id, params, cb) {
  if( params && params.logLevel) {
    doLog(dataset_id, 'info', 'Setting logLevel to "' + params.logLevel +'"');
    setLogger(dataset_id, params);
    cb(null, {"status":"ok"});
  }
  else {
    cb('logLevel parameter required');
  }
}

function doClientSync(dataset_id, params, callback) {

  // Verify that query_param have been passed
  if( ! params || ! params.query_params ) {
    return callback("no_query_params", null);
  }

  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return callback(err, null);
    }
    if( ! dataset.listHandler ) {
      return callback("no_listHandler", null);
    }

    if( params.pending && params.pending.length > 0) {
      doLog(dataset_id, 'info', 'Found pending records... processing', params);

      // Process Pending Params then re-sync data
      processPending(dataset_id, dataset, params, function(pendingRes) {
        doLog(dataset_id, 'info', 'back from processPending - res = \n' + util.inspect(pendingRes), params);
        // Changes have been submitted from client, redo the list operation on back end system,
        redoSyncList(dataset_id, params.query_params, function(err, res) {
          if( res ) {
            res.updates = pendingRes;
          }
          callback(err, res);
        });
      });
    }
    else {
      // No pending updates, just sync client dataset
      var queryHash = generateHash(params.query_params);
      if( dataset.syncLists[queryHash] ) {
        doLog(dataset_id, 'verbose', 'doClientSync - No pending - Hash (Client :: Cloud) = ' + params.dataset_hash + ' :: ' + dataset.syncLists[queryHash].hash, params);
        if( ! params.dataset_hash || params.dataset_hash == '' ) {
          // No client Hash = No client data - return full dataset
          redoSyncList(dataset_id, params.query_params, function(err, res) {
            callback(err, res);
          });
        }
        else {
          if( params.dataset_hash != dataset.syncLists[queryHash].hash ) {
            // Hashes don't match - return the dataset.
            callback(null, dataset.syncLists[queryHash]);
          }
          else {
            // Hashes match - just return our hash
            callback(null, {"hash": dataset.syncLists[queryHash].hash});
          }
        }
      } else {
        doLog(dataset_id, 'verbose', 'No pending records... No data set - invoking list on back end system', params);
        redoSyncList(dataset_id, params.query_params, function(err, res) {
          if( res ) {
            res.updates = {};
          }
          callback(err, res);
        });
      }
    }
  });
}

function processPending(dataset_id, dataset, params, cb) {
  var pending = params.pending;

  var applied = {};
  var failed = {};
  var collisions = {};

  var itemCallback = function() {
    doLog(dataset_id, 'verbose', 'itemCallback :: arguments = ' + util.inspect(arguments), params);
  }

  doLog(dataset_id, 'verbose', 'processPending :: starting async.forEachSeries');
  async.forEachSeries(pending, function(pendingObj, itemCallback) {
      //var pendingObj = pending[i];
      doLog(dataset_id, 'silly', 'processPending :: item = ' + util.inspect(pendingObj), params);
      var action = pendingObj.action;
      var uid = pendingObj.uid;
      var pre = pendingObj.pre;
      var post = pendingObj.post;
      var hash = pendingObj.hash;
      var timestamp = pendingObj.timestamp;

      if( "create" === action ) {
        doLog(dataset_id, 'info', 'CREATE Start', params);
        dataset.createHandler(dataset_id, post, function(err, data) {
          if( err ) {
            doLog(dataset_id, 'warn', 'CREATE Failed - uid=' + uid + ' : err = ' + err, params);
            failed[hash] = {action:"create", "uid":uid, "msg": err};
            return itemCallback();
          }
          doLog(dataset_id, 'info', 'CREATE Success - uid=' + data.uid + ' : hash = ' + hash, params);
          applied[hash]  = {action:"create", "uid":data.uid};
          itemCallback();
        });
      }
      else if ( "update" === action ) {
        doLog(dataset_id, 'info', 'UPDATE Start', params);
        dataset.readHandler(dataset_id, uid, function(err, data) {
          if( err ) {
            doLog(dataset_id, 'warn', 'READ for UPDATE Failed - uid=' + uid + ' : err = ' + err, params);
            failed[hash] = {action:"update", "uid":uid, "msg": err};
            return itemCallback();
          }
          doLog(dataset_id, 'info', ' READ for UPDATE Success', params);
          doLog(dataset_id, 'silly', ' READ for UPDATE Data : \n' + util.inspect(data), params);

          var preHash = generateHash(pre);
          var dataHash = generateHash(data);

          doLog(dataset_id, 'info', 'UPDATE Hash Check ' + uid + ' (client :: dataStore) = ' + preHash + ' :: ' + dataHash, params);

          if( preHash === dataHash ) {
            dataset.updateHandler(dataset_id, uid, post, function(err, data) {
              if( err ) {
                doLog(dataset_id, 'warn', 'UPDATE Failed - uid=' + uid + ' : err = ' + err, params);
                failed[hash] = {action:"update", "uid":uid, "msg": err};
                return itemCallback();
              }
              doLog(dataset_id, 'info', 'UPDATE Success - uid=' + uid + ' : hash = ' + hash, params);
              applied[hash]  = {action:"update", "uid":uid};
              return itemCallback();
            });
          } else {
            var postHash = generatehash(post);
            if( posthash = dataHash ) {
              // Update has already been applied
              doLog(dataset_id, 'info', 'UPDATE Already Applied - uid=' + uid + ' : hash = ' + hash, params);
              applied[hash]  = {action:"update", "uid":uid};
              itemCallback();
            }
            else {
              doLog(dataset_id, 'warn', 'UPDATE COLLISION \n Pre record from client:\n' + util.inspect(sortObject(pre)) + '\n Current record from data store:\n' + util.inspect(sortObject(data)), params);
              dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post);
              collisions[hash]  = {action:"update", "uid":uid};
              return itemCallback();
            }
          }
        });
      }
      else if ( "delete" === action ) {
        doLog(dataset_id, 'info', 'DELETE Start', params);
        dataset.readHandler(dataset_id, uid, function(err, data) {
          if( err ) {
            doLog(dataset_id, 'warn', 'READ for DELETE Failed - uid=' + uid + ' : err = ' + err, params);
            failed[hash] = {action:"delete", "uid":uid, "msg": err};
            return itemCallback();
          }
          doLog(dataset_id, 'info', ' READ for DELETE Success', params);
          doLog(dataset_id, 'silly', ' READ for DELETE Data : \n' + util.inspect(data), params);

          var preHash = generateHash(pre);
          var dataHash = generateHash(data);

          doLog(dataset_id, 'info', 'DELETE Hash Check ' + uid + ' (client :: dataStore) = ' + preHash + ' :: ' + dataHash, params);

          if( dataHash == null ) {
            //record has already been deleted
            doLog(dataset_id, 'info', 'DELETE Already performed - uid=' + uid + ' : hash = ' + hash, params);
            applied[hash]  = {action:"delete", uid:uid};
            itemCallback();
          }
          else {
            if( preHash === dataHash ) {
              dataset.deleteHandler(dataset_id, uid, function(err, data) {
                if( err ) {
                  doLog(dataset_id, 'warn', 'DELETE Failed - uid=' + uid + ' : err = ' + err, params);
                  failed[hash] = {action:"delete", "uid":uid, "msg": err};
                  return itemCallback();
                }
                doLog(dataset_id, 'info', 'DELETE Success - uid=' + uid + ' : hash = ' + hash, params);
                applied[hash]  = {action:"delete", "uid":uid};
                itemCallback();
              });
            } else {
              doLog(dataset_id, 'warn', 'DELETE COLLISION \n Pre record from client:\n' + util.inspect(sortObject(pre)) + '\n Current record from data store:\n' + util.inspect(sortObject(data)), params);
              dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post);
              collisions[hash]  = {action:"delete", "uid":uid};
              itemCallback();
            }
          }
        });
      }
      else {
        doLog(dataset_id, 'warn', 'unknown action : ' + action, params);
        itemCallback();
      }
    },
    function(err) {
      //console.log("processPending :: async callback - err " + err);
      return cb({
        "applied": applied,
        "failed": failed,
        "collisions": collisions
      });
    });
}

function redoSyncList(dataset_id, query_params, cb) {
  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err, null);
    }
    // Clear any existing timeouts so sync does not run multiple times
    var queryHash = generateHash(query_params);
    if( dataset && dataset.timeouts && dataset.timeouts[queryHash]) {
      doLog(dataset_id, 'info', 'redoSyncList :: Clearing timeout for dataset sync loop - queryParams : ' + util.inspect(query_params));
      clearTimeout(dataset.timeouts[queryHash]);
    }
    // Invoke the sync List;
    doSyncList(dataset_id, query_params, cb);
  });
}

function doSyncList(dataset_id, query_params, cb) {
  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      // doSyncList is recursively called with no callback. This means we must
      // check if cb exists before passing the error to it.
      if( cb ) {
        return cb(err, null);
      }
      else {
        doLog(dataset_id, 'error', 'Error getting dataset in doSyncList : ' + err);
        return;
      }
    }
    if( ! dataset.listHandler ) {
      return cb("no_listHandler", null);
    }

    dataset.listHandler(dataset_id, query_params, function(err, records) {
      if( err ) {
        if( cb ) {
          cb(err);
        }
        return;
      }

      var hashes = [];
      var shasum;
      var recOut = {};
      for(var i in records ) {
        var rec = {};
        var recData = records[i];
        var hash = generateHash(recData);
        hashes.push(hash);
        //console.log("recData hash :: ", hash);
        rec.data = recData;
        rec.hash = hash;
        recOut[i] = rec;
      }
      var globalHash = generateHash(hashes);
      //console.log("globalHash :: ", globalHash);

      var queryHash = generateHash(query_params);
      //console.log("queryHash :: ", queryHash);

      var previousHash = (dataset.syncLists[queryHash] && dataset.syncLists[queryHash].hash) ? dataset.syncLists[queryHash].hash : '<undefined>';
      doLog(dataset_id, 'verbose', 'doSyncList cb ' + ( cb != undefined) + ' - Global Hash (prev :: cur) = ' + previousHash + ' ::  ' + globalHash);

      dataset.syncLists[queryHash] = {"records" : recOut, "hash": globalHash};
      dataset.timeouts[queryHash] = setTimeout(function() {
        doSyncList(dataset_id, query_params);
      }, dataset.config.sync_frequency * 1000);
      if( cb ) {
        //console.log('dataset.syncLists[',queryHash, ']', dataset.syncLists[queryHash]);
        cb(null, dataset.syncLists[queryHash]);
      }
    });
  });
}


/* Synchronise the individual records for a dataset */
function doSyncRecords(dataset_id, params, callback) {
  doLog(dataset_id, 'verbose', 'doSyncRecords', params);
  // Verify that query_param have been passed
  if( ! params || ! params.query_params ) {
    return callback("no_query_params", null);
  }

  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return callback(err, null);
    }
    var queryHash = generateHash(params.query_params);
    if( dataset.syncLists[queryHash] && dataset.syncLists[queryHash].records) {
      // We have a data set for this dataset_id and query hash - compare the uid and hashe values of
      // our records with the record received

      var creates = {};
      var updates = {};
      var deletes = {};
      var i;

      var serverRecs = dataset.syncLists[queryHash].records;
      //console.log("serverRecs : ", serverRecs);

      var clientRecs = {};
      if( params && params.clientRecs) {
        clientRecs = params.clientRecs;
      }

      for( i in serverRecs ) {
        var serverRec = serverRecs[i];
        //console.log("processing server record ", i , " :: ", serverRec);
        var serverRecUid = i;
        var serverRecHash = serverRec.hash;

        if( clientRecs[serverRecUid] ) {
          if( clientRecs[serverRecUid] !== serverRecHash ) {
            doLog(dataset_id, 'info', '"Updating client record ' + serverRecUid + " client hash=" + clientRecs[serverRecUid], params);
            updates[serverRecUid] = serverRec;
          }
        } else {
          doLog(dataset_id, 'info', 'Creating client record ' + serverRecUid, params);
          creates[serverRecUid] = serverRec;
        }
      }

      // Itterate over each of the client records. If there is no corresponding server record then mark the client
      // record for deletion
      for( i in clientRecs ) {
        if( ! serverRecs[i] ) {
          deletes[i] = {};
        }
      }

      var res = {"create": creates, "update": updates, "delete":deletes, "hash":dataset.syncLists[queryHash].hash};
      callback(null, res);
    } else {
      // No data set invoke the list operation on back end system,
      redoSyncList(dataset_id, params.query_params, function(err, res) {
        callback(err, res);
      });
    }
  });
}

function getDataset(dataset_id, cb) {

  // TODO - Persist data sets - in memory or more permanently ($fh.db())
  if( deleted_datasets[dataset_id] ) {
    return cb("unknown_dataset", null);
  }
  else {
    var dataset = datasets[dataset_id];
    if( ! dataset ) {
      return cb("unknown_dataset", null);
    }
    else {
      return cb(null, dataset);
    }
  }
}

function createDataset(dataset_id, cb) {
  delete deleted_datasets[dataset_id];

  var dataset = datasets[dataset_id];
  if( ! dataset ) {
    dataset = {};
    dataset.id = dataset_id;
    dataset.created = new Date().getTime();
    dataset.syncLists = {};
    dataset.timeouts = {};
    datasets[dataset_id]= dataset;
  }
  cb(null, dataset);
}

function removeDataset(dataset_id, cb) {

  // TODO - Persist data sets - in memory or more permanently ($fh.db())
  deleted_datasets[dataset_id] = new Date().getTime();

  delete datasets[dataset_id];

  cb(null, {});
}

function generateHash(plainText) {
  var hash;
  if( plainText ) {
    if ('string' !== typeof plainText) {
      plainText = sortedStringify(plainText);
    }
    var shasum = crypto.createHash('sha1');
    shasum.update(plainText);
    hash = shasum.digest('hex');
  }
  return hash;
}

function sortObject(object) {
  if (typeof object !== "object" || object === null) {
    return object;
  }

  var result = [];

  Object.keys(object).sort().forEach(function(key) {
    result.push({
      key: key,
      value: sortObject(object[key])
    });
  });

  return result;
}


function sortedStringify(obj) {

  var str = '';

  try {
    var soretdObject = sortObject(obj);
    if(obj) {
      str = JSON.stringify(sortObject(obj));
    }
  } catch (e) {
    doLog(SYNC_LOGGER, 'error', 'Error stringifying sorted object:' + e);
    throw e;
  }

  return str;
}

function setLogger(dataset_id, options) {
  var level = options.logLevel;
  var logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: level, debugStdout: true })
    ]
  });
  loggers[dataset_id] = logger;
}

function doLog(dataset_id, level, msg, params) {

  var logger = loggers[dataset_id] || loggers[SYNC_LOGGER];
  if( logger ) {
    var logMsg = moment().format('YYYY-MM-DD HH:mm:ss') + ' [' + dataset_id + '] ';
    if( params && params.__fh && params.__fh.cuid ) {
      logMsg += '(' + params.__fh.cuid + ') ';
    }
    logMsg = logMsg + ': ' +msg;

    logger.log(level, logMsg);
  }
}

/* ======================================================= */
/* ================== PRIVATE VARIABLES ================== */
/* ======================================================= */

var loggers = {};

var datasets = {};

var deleted_datasets = {};

// CONFIG
var defaults = {
  "sync_frequency": 10,
  "logLevel" : "info"
};

// Functions which can be invoked through sync.doInvoke
var invokeFunctions = {
  "sync" : doClientSync,
  "syncRecords" : doSyncRecords,
  "listCollisions": doListCollisions,
  "removeCollision": doRemoveCollision,
  "setLogLevel" : doSetLogLevel
};


/* ======================================================= */
/* =================== INITIALISATION ==================== */
/* ======================================================= */
setLogger(SYNC_LOGGER, {logLevel : defaults.logLevel});
