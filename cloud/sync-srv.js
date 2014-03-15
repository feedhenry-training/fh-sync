var crypto = require('crypto');
var async = require('async');
var winston = require('winston');
var moment = require('moment');
var util = require('util');
var events = require('events');

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

exports.stopAll = function(callback) {
  return stopAllDatasetSync(callback);
};

exports.handleList = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.listHandler = fn;
    }
  });
};

exports.handleCreate = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.createHandler = fn;
    }
  });
};

exports.handleRead = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.readHandler = fn;
    }
  });
};

exports.handleUpdate = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.updateHandler = fn;
    }
  });
};

exports.handleDelete = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.deleteHandler = fn;
    }
  });
};

exports.handleCollision = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.collisionHandler = fn;
    }
  });
};

exports.listCollisions = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( ! err ) {
      dataset.collisionLister = fn;
    }
  });
};

exports.removeCollision = function(dataset_id, fn) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
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

  DataSetModel.createDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err, null);
    }
    dataset.config = datasetConfig;
    cb(null, {});
  });
}

function stopDatasetSync(dataset_id, cb) {
  DataSetModel.stopDatasetSync(dataset_id, cb);
}

function stopAllDatasetSync(cb) {
  DataSetModel.stopAllDatasetSync(cb);
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
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( err ) return cb(err);

    if( ! dataset.collisionLister ) {
      return cb("no_collisionLister", null);
    }

    dataset.collisionLister(dataset_id, cb);
  });
}

function doRemoveCollision(dataset_id, params, cb) {
  DataSetModel.getDataset(dataset_id, function(err, dataset) {
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

  DataSetModel.getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return callback(err, null);
    }
    if( ! dataset.listHandler ) {
      return callback("no_listHandler", null);
    }

    DataSetModel.getOrCreateDatasetClient(dataset_id, params.query_params, params.meta_data, function(err, datasetClient) {
      if(err) return cb(err);

      //Deal with any Acknowledgement of updates from the client
      acknowledgeUpdates(dataset_id, params, function(err) {

        if( params.pending && params.pending.length > 0) {
          doLog(dataset_id, 'info', 'Found ' + params.pending.length + ' pending records. processing', params);

          // Process Pending Params then re-sync data
          processPending(dataset_id, dataset, params, function() {
            doLog(dataset_id, 'verbose', 'back from processPending', params);
            // Changes have been submitted from client, redo the list operation on back end system.
            DataSetModel.syncDatasetClientObj(datasetClient, function(err, res) {
              returnUpdates(dataset_id, params, res, callback);
            });
          });
        }
        else {
          if( datasetClient.data.hash ) {
            // No pending updates, just sync client dataset
            //doLog(dataset_id, 'verbose', 'doClientSync - No pending - Hash (Client :: Cloud) = ' + params.dataset_hash + ' :: ' + datasetClient.data.hash, params);

            if( datasetClient.data.hash === params.dataset_hash) {
              doLog(dataset_id, 'verbose', 'doClientSync - No pending - Hashes match. Just return hash', params);
              var res = {"hash": datasetClient.hash};
              returnUpdates(dataset_id, params, res, callback);
            }
            else {
              doLog(dataset_id, 'info', 'doClientSync - No pending - Hashes NO NOT match (Client :: Cloud) = ' + params.dataset_hash + ' :: ' + datasetClient.data.hash + ' - return full dataset', params);
              // TODO - Partial Sync
              var res = datasetClient.data;
              returnUpdates(dataset_id, params, res, callback);
            }
          } else {
            doLog(dataset_id, 'verbose', 'No pending records. No cloud data set - invoking list on back end system', params);
            DataSetModel.syncDatasetClientObj(datasetClient, function(err, res) {
              if( err ) callback(err);
              returnUpdates(dataset_id, params, res, callback);
            });
          }
        }
      });
    });
  });
}

function processPending(dataset_id, dataset, params, cb) {
  var pending = params.pending;
  var meta_data = params.meta_data;

  var cuid = getCuid(params);

  var itemCallback = function(err, update) {
    doLog(dataset_id, 'verbose', 'itemCallback :: err=' + err + " :: storedPendingUpdate = " + util.inspect(update), params);
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

    function addUpdate(type, action, hash, uid, msg, cb) {

      var update = {
        cuid: cuid,
        type: type,
        action: action,
        hash: hash,
        uid : uid,
        msg: util.inspect(msg)
      }
      $fh.db({
        "act": "create",
        "type": dataset_id + "-updates",
        "fields": update
      }, function(err, res) {
        cb(err, res);
      });
    }

    if( "create" === action ) {
      doLog(dataset_id, 'info', 'CREATE Start', params);
      dataset.createHandler(dataset_id, post, function(err, data) {
        if( err ) {
          doLog(dataset_id, 'warn', 'CREATE Failed - uid=' + uid + ' : err = ' + err, params);
          return addUpdate("failed", "create", hash, uid, err, itemCallback);
        }
        doLog(dataset_id, 'info', 'CREATE Success - uid=' + data.uid + ' : hash = ' + hash, params);
        return addUpdate("applied", "create", hash, data.uid, '', itemCallback);
      }, meta_data);
    }
    else if ( "update" === action ) {
      doLog(dataset_id, 'info', 'UPDATE Start', params);
      dataset.readHandler(dataset_id, uid, function(err, data) {
        if( err ) {
          doLog(dataset_id, 'warn', 'READ for UPDATE Failed - uid=' + uid + ' : err = ' + err, params);
          return addUpdate("failed", "update", hash, uid, err, itemCallback);
        }
        doLog(dataset_id, 'info', ' READ for UPDATE Success', params);
        doLog(dataset_id, 'silly', 'READ for UPDATE Data : \n' + util.inspect(data), params);

        var preHash = generateHash(pre);
        var dataHash = generateHash(data);

        doLog(dataset_id, 'info', 'UPDATE Hash Check ' + uid + ' (client :: dataStore) = ' + preHash + ' :: ' + dataHash, params);

        if( preHash === dataHash ) {
          dataset.updateHandler(dataset_id, uid, post, function(err, data) {
            if( err ) {
              doLog(dataset_id, 'warn', 'UPDATE Failed - uid=' + uid + ' : err = ' + err, params);
              return addUpdate("failed", "update", hash, uid, err, itemCallback);
            }
            doLog(dataset_id, 'info', 'UPDATE Success - uid=' + uid + ' : hash = ' + hash, params);
            return addUpdate("applied", "update", hash, uid, '', itemCallback);
          }, meta_data);
        } else {
          var postHash = generateHash(post);
          if( postHash === dataHash ) {
            // Update has already been applied
            doLog(dataset_id, 'info', 'UPDATE Already Applied - uid=' + uid + ' : hash = ' + hash, params);
            return addUpdate("applied", "update", hash, uid, '', itemCallback);
          }
          else {
            doLog(dataset_id, 'warn', 'UPDATE COLLISION \n Pre record from client:\n' + util.inspect(sortObject(pre)) + '\n Current record from data store:\n' + util.inspect(sortObject(data)), params);
            dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post, meta_data);
            return addUpdate("collisions", "update", hash, uid, '', itemCallback);
          }
        }
      }, meta_data);
    }
    else if ( "delete" === action ) {
      doLog(dataset_id, 'info', 'DELETE Start', params);
      dataset.readHandler(dataset_id, uid, function(err, data) {
        if( err ) {
          doLog(dataset_id, 'warn', 'READ for DELETE Failed - uid=' + uid + ' : err = ' + err, params);
          return addUpdate("failed", "delete", hash, uid, err, itemCallback);
        }
        doLog(dataset_id, 'info', ' READ for DELETE Success', params);
        doLog(dataset_id, 'silly', ' READ for DELETE Data : \n' + util.inspect(data), params);

        var preHash = generateHash(pre);
        var dataHash = generateHash(data);

        doLog(dataset_id, 'info', 'DELETE Hash Check ' + uid + ' (client :: dataStore) = ' + preHash + ' :: ' + dataHash, params);

        if( dataHash == null ) {
          //record has already been deleted
          doLog(dataset_id, 'info', 'DELETE Already performed - uid=' + uid + ' : hash = ' + hash, params);
          return addUpdate("applied", "delete", hash, uid, '', itemCallback);
        }
        else {
          if( preHash === dataHash ) {
            dataset.deleteHandler(dataset_id, uid, function(err, data) {
              if( err ) {
                doLog(dataset_id, 'warn', 'DELETE Failed - uid=' + uid + ' : err = ' + err, params);
                return addUpdate("failed", "delete", hash, uid, err, itemCallback);
              }
              doLog(dataset_id, 'info', 'DELETE Success - uid=' + uid + ' : hash = ' + hash, params);
              return addUpdate("applied", "delete", hash, uid, '', itemCallback);
            }, meta_data);
          } else {
            doLog(dataset_id, 'warn', 'DELETE COLLISION \n Pre record from client:\n' + util.inspect(sortObject(pre)) + '\n Current record from data store:\n' + util.inspect(sortObject(data)), params);
            dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post, meta_data);
            return addUpdate("collisions", "delete", hash, uid, '', itemCallback);
          }
        }
      }, meta_data);
    }
    else {
      doLog(dataset_id, 'warn', 'unknown action : ' + action, params);
      itemCallback();
    }
  },
  function(err) {
    return cb();
  });
}

function returnUpdates(dataset_id, params, resIn, cb) {
  //doLog(dataset_id, 'verbose', 'START returnUpdates', params);
  doLog(dataset_id, 'silly', 'returnUpdates - existing res = ' + util.inspect(resIn), params);
  var cuid = getCuid(params);
  $fh.db({
    "act": "list",
    "type": dataset_id + "-updates",
    "eq": {
      "cuid": cuid
    }
  }, function(err, res) {
    if (err) return cb(err);

    var updates = {};

    doLog(dataset_id, 'silly', 'returnUpdates - found ' + res.list.length + ' updates', params);

    for (var di = 0, dl = res.list.length; di < dl; di += 1) {
      var rec = res.list[di].fields;
      if ( !updates.hashes ) {
        updates.hashes = {};
      }
      updates.hashes[rec.hash] = rec;

      if( !updates[rec.type] ) {
        updates[rec.type] = {};
      }
      updates[rec.type][rec.hash] = rec;

      doLog(dataset_id, 'verbose', 'returning update ' + util.inspect(rec), params);
    }

    if( ! resIn ) {
      doLog(dataset_id, 'silly', 'returnUpdates - initialising res', params);
      resIn = {};
    }
    resIn.updates = updates;
    doLog(dataset_id, 'silly', 'returnUpdates - final res = ' + util.inspect(resIn), params);
    if( res.list.length > 0 ) {
      doLog(dataset_id, 'info', 'returnUpdates :: ' + util.inspect(updates.hashes), params);
    }
    return cb(null, resIn);
  });
}

function acknowledgeUpdates(dataset_id, params, cb) {

  var updates = params.acknowledgements;
  var cuid = getCuid(params);

  var itemCallback = function(err, update) {
    doLog(dataset_id, 'verbose', 'acknowledgeUpdates :: err=' + err + ' :: update=' + util.inspect(update), params);
  }

  if( updates && updates.length > 0) {
    doLog(dataset_id, 'info', 'acknowledgeUpdates :: ' + util.inspect(updates), params);

    async.forEachSeries(updates, function(update, itemCallback) {
      doLog(dataset_id, 'verbose', 'acknowledgeUpdates :: processing update ' + util.inspect(update), params);
      $fh.db({
        "act": "list",
        "type": dataset_id + "-updates",
        "eq": {
          "cuid": cuid,
          "hash": update.hash
        }
      }, function(err, res) {
        if (err) return itemCallback(err, update);

        if( res && res.list && res.list.length > 0 ) {
          var rec = res.list[0];
          var uid = rec.guid;
          $fh.db({
            "act": "delete",
            "type": dataset_id + "-updates",
            "guid": uid
          }, function(err, res) {
            if (err) return itemCallback(err, update);

            return itemCallback(null, update);
          });
        }
        else {
          return itemCallback(null, update);
        }
      });
    },
    function(err) {
      if( err ) {
        doLog(dataset_id, 'info', 'END acknowledgeUpdates - err=' + err, params);
      }
      cb(err);
    });
  }
  else {
    cb();
  }
}

/* Synchronise the individual records for a dataset */
function doSyncRecords(dataset_id, params, callback) {
  doLog(dataset_id, 'verbose', 'doSyncRecords', params);
  // Verify that query_param have been passed
  if( ! params || ! params.query_params ) {
    return callback("no_query_params", null);
  }

  DataSetModel.getOrCreateDatasetClient(dataset_id, params.query_params, params.meta_data, function(err, datasetClient) {
    if( err ) {
      return callback(err, null);
    }


    if( datasetClient.data.hash) {
      // We have a data set for this dataset_id and query hash - compare the uid and hashe values of
      // our records with the record received

      var creates = {};
      var updates = {};
      var deletes = {};
      var i;

      var serverRecs = datasetClient.data.records;

      var clientRecs = {};
      if( params && params.clientRecs) {
        clientRecs = params.clientRecs;
      }

      for( i in serverRecs ) {
        var serverRec = serverRecs[i];
        var serverRecUid = i;
        var serverRecHash = serverRec.hash;

        if( clientRecs[serverRecUid] ) {
          if( clientRecs[serverRecUid] !== serverRecHash ) {
            doLog(dataset_id, 'info', 'Updating client record ' + serverRecUid + ' client hash=' + clientRecs[serverRecUid], params);
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
      DataSetModel.syncDatasetClientObj(datasetClient, callback);
    }
  });
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
    logMsg += '(' + getCuid(params)  + ')';
    logMsg = logMsg + ': ' +msg;

    logger.log(level, logMsg);
  }
}

function getCuid(params) {
  var cuid = '';
  if( params && params.__fh && params.__fh.cuid ) {
    cuid = params.__fh.cuid;
  }
  return cuid;
}

/* ======================================================= */
/* ================== PRIVATE VARIABLES ================== */
/* ======================================================= */

var loggers = {};

// CONFIG
var defaults = {
  "sync_frequency": 10,
  "logLevel" : "verbose"
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


/* ======================================================= */
/* =================== DataSets Object ==================== */
/* ======================================================= */

var DataSetModel = (function() {

  var self = {

    defaults : {
      "syncFrequency": 10
    },

    datasets : {},
    deletedDatasets : {},


    getDataset : function(dataset_id, cb) {

      // TODO - Persist data sets - in memory or more permanently ($fh.db())
      if( self.deletedDatasets[dataset_id] ) {
        return cb("unknown_dataset - " + dataset_id, null);
      }
      else {
        var dataset = self.datasets[dataset_id];
        if( ! dataset ) {
          return cb("unknown_dataset - " + dataset_id, null);
        }
        else {
          return cb(null, dataset);
        }
      }
    },

    createDataset : function(dataset_id, cb) {
      delete self.deletedDatasets[dataset_id];

      var dataset = self.datasets[dataset_id];
      if( ! dataset ) {
        dataset = {
          id : dataset_id,
          created : new Date().getTime(),
          clients : {}
      }
        self.datasets[dataset_id]= dataset;
      }
      cb(null, dataset);
    },

    removeDataset : function(dataset_id, cb) {

      // TODO - Persist data sets - in memory or more permanently ($fh.db())
      self.deletedDatasets[dataset_id] = new Date().getTime();

      delete self.datasets[dataset_id];

      cb(null, {});
    },

    stopDatasetSync : function(dataset_id, cb) {
      doLog(dataset_id, 'info', 'stopDatasetSync');
      self.getDataset(dataset_id, function(err, dataset) {
        if( err ) {
          return cb(err);
        }

        self.removeDataset(dataset_id, cb);
      });
    },

    stopAllDatasetSync : function(cb) {
      doLog(SYNC_LOGGER, 'info', 'stopAllDatasetSync');

      var stoppingDatasets = [];
      for( var dsId in self.datasets ) {
        if( self.datasets.hasOwnProperty(dsId) ) {
          stoppingDatasets.push(self.datasets[dsId]);
        }
      }

      var stoppedDatasets = [];

      async.forEachSeries(stoppingDatasets, function(dataset, itemCallback) {
        stoppedDatasets.push(dataset.id);
        self.stopDatasetSync(dataset.id, itemCallback);
      },
      function(err) {
        cb(err, stoppedDatasets);
      });
    },

    getOrCreateDatasetClient : function(dataset_id, query_params, meta_data, cb) {
      self.getDataset(dataset_id, function(err, dataset) {
        if( err ) return err;
        var clientHash = self.getClientHash(query_params, meta_data);
        var datasetClient = dataset.clients[clientHash];
        if( ! datasetClient ) {
          return self.createDatasetClient(dataset_id, query_params, meta_data, cb);
        }
        else {
          return cb(null, datasetClient);
        }
      })
    },

    getDatasetClientByObj : function(datasetClient, cb) {
      if( datasetClient && datasetClient.id && datasetClient.datasetId) {
        self.getDataset(datasetClient.datasetId, function(err, dataset) {
          if(err) return cb(err);
          var dsc = dataset.clients[datasetClient.id];

          if( ! dsc ) return cb('Unknown datasetClient');
          return cb(null, dsc);
        });
      }
      else {
        return cb('Unknown datasetClient');
      }
    },

    getDatasetClient : function(dataset_id, query_params, meta_data, cb) {
      self.getDataset(dataset_id, function(err, dataset) {
      if( err ) return err;
        var clientHash = self.getClientHash(query_params, meta_data);
        var datasetClient = dataset.clients[clientHash];
        if( ! datasetClient ) return cb('Unknown dataset client for dataset_id ' + dataset_id);
        return cb(null, datasetClient);
      });
    },

    createDatasetClient : function(dataset_id, query_params, meta_data, cb) {
      self.getDataset(dataset_id, function(err, dataset) {
        if( err ) return err;
        var clientHash = self.getClientHash(query_params, meta_data);
        var datasetClient = dataset.clients[clientHash];
        if( ! datasetClient ) {
          datasetClient = {
            id : clientHash,
            datasetId : dataset_id,
            created : new Date().getTime(),
            queryParams : query_params,
            metaData : meta_data,
            syncRunning : false,
            syncPending : true,
            syncActive : true,
            pendingCallbacks : [],
            data : {}
          };
          dataset.clients[clientHash] = datasetClient;
        }
        doLog(dataset_id, 'verbose', 'createDatasetClient :: ' + util.inspect(datasetClient));
        return cb(null, datasetClient);
      });
    },

    removeDatasetClient : function(datasetClient, cb) {
      if( datasetClient && datasetClient.id ) {
        delete dataset.clients[datasetClient.id];
      }
      cb();
    },

    syncDatasetClient : function(dataset_id, query_params, meta_data, cb) {
      self.getDatasetClient(dataset_id, query_params, meta_data, function(err, dsc) {
        if(err) return cb(err);
        dsc.syncPending = true;
        dsc.pendingCallbacks.push(cb);
      });
    },

    syncDatasetClientObj : function(datasetClient, cb) {
      self.getDatasetClientByObj(datasetClient, function(err, dsc) {
        if(err) return cb(err);
        dsc.syncPending = true;
        dsc.pendingCallbacks.push(cb);
      });
    },

    getClientHash: function(query_params, meta_data) {
      var queryParamsHash = generateHash(query_params);
      var metaDataHash = generateHash(meta_data);

      return queryParamsHash + '-' + metaDataHash;
    },

    doSyncList : function(dataset, datasetClient, cb) {
      datasetClient.syncPending = false;
      datasetClient.syncRunning = true;
      datasetClient.syncLoopStart = new Date().getTime();

      if( ! dataset.listHandler ) {
        return cb("no_listHandler", null);
      }

      dataset.listHandler(dataset.id, datasetClient.query_params, function(err, records) {
        if( err ) return cb(err);


        var hashes = [];
        var recOut = {};
        for(var i in records ) {
          var rec = {};
          var recData = records[i];
          var hash = generateHash(recData);
          hashes.push(hash);
          rec.data = recData;
          rec.hash = hash;
          recOut[i] = rec;
        }
        var globalHash = generateHash(hashes);

        var previousHash = datasetClient.data.hash ? datasetClient.data.hash : '<undefined>';
        doLog(dataset.id, 'verbose', 'doSyncList cb ' + ( cb != undefined) + ' - Global Hash (prev :: cur) = ' + previousHash + ' ::  ' + globalHash);

        datasetClient.data = {"records" : recOut, "hash": globalHash};

        datasetClient.syncRunning = false;
        datasetClient.syncLoopEnd = new Date().getTime();
        if( cb ) {
          cb(null, datasetClient.data);
        }
      }, datasetClient.metaData);
    },

    doSyncLoop : function() {
      for( var dataset_id in self.datasets ) {
        if( self.datasets.hasOwnProperty(dataset_id) ) {
          var dataset = self.datasets[dataset_id];
          for( var datasetClientId in dataset.clients ) {
            if( dataset.clients.hasOwnProperty(datasetClientId) ) {
              var datasetClient = dataset.clients[datasetClientId];
              if( !datasetClient.syncRunning && datasetClient.syncActive) {
                // Check to see if it is time for the sync loop to run again
                var lastSyncStart = datasetClient.syncLoopStart;
                var lastSyncCmp = datasetClient.syncLoopEnd;
                if( lastSyncStart == null ) {
                  doLog(dataset_id, 'verbose', 'Performing initial sync');
                  // Dataset has never been synced before - do initial sync
                  datasetClient.syncPending = true;
                } else if (lastSyncCmp != null) {
                  var timeSinceLastSync = new Date().getTime() - lastSyncCmp;
                  var syncFrequency = dataset.config.sync_frequency * 1000;
                  if( timeSinceLastSync > syncFrequency ) {
                    // Time between sync loops has passed - do another sync
                    datasetClient.syncPending = true;
                  }
                }

                if( datasetClient.syncPending ) {
                  doLog(dataset_id, 'verbose', 'running sync for client ' + datasetClient.id);
                  // If the dataset requres syncing, run the sync loop. This may be because the sync interval has passed
                  // or because the sync_frequency has been changed or because the syncPending flag was deliberately set
                  self.doSyncList(dataset, datasetClient, function(err, res) {

                    // Check if there are aby pending callbacks for this sync Client;
                    var pendingCallbacks = datasetClient.pendingCallbacks;
                    datasetClient.pendingCallbacks = [];
                    for( var i = 0; i < pendingCallbacks.length; i++) {
                      var cb = pendingCallbacks[i];

                      // Use process.nextTick so we can complete the syncLoop before all the callbacks start to fire
                      function invokeCb() {
                        cb(err, res);
                      };
                      process.nextTick(invokeCb);
                    }
                  });
                }
              }
            }
          }
        }
      }
    },

    datasetMonitor : function() {
      self.doSyncLoop();

      // Re-execute datasetMonitor every 500ms so we keep invoking doSyncLoop();
      setTimeout(function() {
        self.datasetMonitor();
      }, 500);
    }
  };

  var init = function() {
    doLog('', 'info', 'DataSetModel Init');

    self.datasetMonitor();

  };

  init();

  return {
    stopDatasetSync : self.stopDatasetSync,
    stopAllDatasetSync : self.stopAllDatasetSync,
    getOrCreateDatasetClient: self.getOrCreateDatasetClient,
    getDataset : self.getDataset,
    createDataset : self.createDataset,
    removeDataset : self.removeDataset,
    getDatasetClient: self.getDatasetClient,
    createDatasetClient : self.createDatasetClient,
    removeDatasetClient : self.removeDatasetClient,
    syncDatasetClient : self.syncDatasetClient,
    syncDatasetClientObj : self.syncDatasetClientObj,
    getClientHash : self.getClientHash
  }
})();
