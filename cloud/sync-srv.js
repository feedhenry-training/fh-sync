var crypto = require('crypto');
var async = require('async');

exports.init = function(dataset_id, options, cb) {
  initDataset(dataset_id, options, cb);
};

exports.invoke = function(dataset_id, params, callback) {
  return doInvoke(dataset_id, params, callback);
};

exports.stop = function(dataset_id, callback) {
  return stopDatasetSync(dataset_id, callback);
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
  console.log('!!!!!!!!!!!!!!!listCollisions - ', fn);
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
  var datasetConfig = JSON.parse(JSON.stringify(defaults));
  for (var i in options) {
    datasetConfig[i] = options[i];
  }

  createDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err, null);
    }
    dataset.config = datasetConfig;
    cb(null, {});
  });
}

function stopDatasetSync(dataset_id, cb) {
  console.log('stopDatasetSync :: dataset_id : ', dataset_id);
  getDataset(dataset_id, function(err, dataset) {
    if( err ) {
      return cb(err);
    }
    if( dataset.timeouts ) {
      console.log('stopDatasetSync :: Clearing timeouts for dataset : ', dataset_id);
      for( i in dataset.timeouts ) {
        clearTimeout(dataset.timeouts[i]);
      }
    }

    removeDataset(dataset_id, cb);
  });
}

function doInvoke(dataset_id, params, callback) {

  // Verify that fn param has been passed
  if( ! params || ! params.fn ) {
    console.log("no_fn :: ", params);
    return callback("no_fn", null);
  }

  var fn = params.fn;

  // Verify that fn param is valid
  var fnHandler = invokeFunctions[fn];
  if( ! fnHandler ) {
    return callback("unknown_fn", null);
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
      doLog("Found pending records... processing", params);
      // Process Pending Params then re-sync data
      processPending(dataset_id, dataset, params, function(pendingRes) {
        console.log("back from processPending - ", pendingRes);
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
        doLog("No pending records... Hash (Request :: cloud) = " + params.dataset_hash + " :: " + dataset.syncLists[queryHash].hash, params);
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
        doLog("No pending records... No data set - invoking the list operation on back end system", params);
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
    console.log("itemCallback :: arguments = ", arguments);
  }

  console.log("processPending :: starting async.forEachSeries");
  async.forEachSeries(pending, function(pendingObj, itemCallback) {
      //var pendingObj = pending[i];
      console.log(new Date(), " - processPending :: in async.forEachSeries :: ", pendingObj);
      var action = pendingObj.action;
      var uid = pendingObj.uid;
      var pre = pendingObj.pre;
      var post = pendingObj.post;
      var hash = pendingObj.hash;
      var timestamp = pendingObj.timestamp;

      if( "create" === action ) {
        dataset.createHandler(dataset_id, post, function(uid, data) {
          applied[hash]  = {"uid":data.uid};
          itemCallback();
        }, function(msg) {
          failed[hash] = {"uid":uid, "msg": msg};
          itemCallback();
        });
      }
      else if ( "update" === action ) {
        console.log("update action");
        dataset.readHandler(dataset_id, uid, function(err, data) {
          if( err ) {
            failed[hash] = {"uid":uid, "msg": err};
            return itemCallback();
          }
          var preHash = generateHash(pre);
          var dataHash = generateHash(data);
          console.log(new Date(), 'UPDATE ', uid, ' pre hash:', preHash, ' post hash', dataHash);

          if( preHash === dataHash ) {
            dataset.updateHandler(dataset_id, uid, post, function(err, data) {
              if( err ) {
                failed[hash] = {"uid":uid, "msg": err};
                return itemCallback();
              }
              applied[hash]  = {"uid":uid};
              return itemCallback();
            });
          } else {
            console.log("CALLING COLLISION HANDLER");
            dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post);
            collisions[hash]  = {"uid":uid};
            return itemCallback();
          }
        });
      }
      else if ( "delete" === action ) {
        dataset.readHandler(dataset_id, uid, function(err, data) {
          if( err ) {
            failed[hash] = {"uid":uid, "msg": err};
            return itemCallback();
          }

          var preHash = generateHash(pre);
          var dataHash = generateHash(data);

          console.log(new Date(), 'DELETE ', uid, ' pre hash:', preHash, ' post hash', dataHash);

          if( preHash === dataHash ) {
            dataset.deleteHandler(dataset_id, uid, function(err, data) {
              if( err ) {
                failed[hash] = {"uid":uid, "msg": err};
                return itemCallback();
              }
              applied[hash]  = {"uid":uid};
              itemCallback();
            });
          } else {
            dataset.collisionHandler(dataset_id, hash, timestamp, uid, pre, post);
            collisions[hash]  = {"uid":uid};
            itemCallback();
          }
        });
      }
      else {
        console.log("unknown action");
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
      console.log('redoSyncList :: Clearing timeout for dataset sync loop - queryParams : ', query_params);
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
        console.error("Error getting dataset in doSyncList : ", err);
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
      console.log(new Date() + " : doSyncList (" + dataset_id + ") cb " + ( cb != undefined) + " - Global Hash (prev :: cur) = " + previousHash + " ::  " + globalHash);

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
  console.log("doSyncRecords");
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
            console.log("Updating client record ", serverRecUid, " :: client hash=", clientRecs[serverRecUid]);
            updates[serverRecUid] = serverRec;
          }
        } else {
          console.log("Creating client record ", serverRecUid);
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

  cb();
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

function sortedStringify(obj) {

  function sort(object) {
    if (typeof object !== "object" || object === null) {
      return object;
    }

    var result = [];

    Object.keys(object).sort().forEach(function(key) {
      result.push({
        key: key,
        value: sort(object[key])
      });
    });

    return result;
  }

  var str = '';

  try {
    str = JSON.stringify(sort(obj));
  } catch (e) {
    console.error('Error stringifying sorted object:', e);
    throw e;
  }

  return str;
}

function doLog(msg, params) {
  var logMsg = new Date() + ': ';
  if( params && params.__fh && params.__fh.cuid ) {
    logMsg += '(' + params.__fh.cuid + ') : ';
  }
  logMsg += msg;
  console.log(logMsg);
}

/* ======================================================= */
/* ================== PRIVATE VARIABLES ================== */
/* ======================================================= */

var datasets = {};

var deleted_datasets = {};

// CONFIG
var defaults = {
  "sync_frequency": 10
};

// Functions which can be invoked through sync.doInvoke
var invokeFunctions = {
  "sync" : doClientSync,
  "syncRecords" : doSyncRecords,
  "listCollisions": doListCollisions,
  "removeCollision": doRemoveCollision
};