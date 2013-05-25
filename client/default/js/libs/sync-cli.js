$fh = $fh || {};
console.log('HERE');
$fh.sync = (function() {

  var self = {

    // CONFIG
    defaults: {
      "sync_frequency": 10,
      // How often to synchronise data with the cloud in seconds.
      "auto_sync_local_updates": true,
      // Should local chages be syned to the cloud immediately, or should they wait for the next sync interval
      "notify_client_storage_failed": false,
      // Should a notification event be triggered when loading/saving to client storage fails
      "notify_sync_started": false,
      // Should a notification event be triggered when a sync cycle with the server has been started
      "notify_sync_complete": true,
      // Should a notification event be triggered when a sync cycle with the server has been completed
      "notify_offline_update": false,
      // Should a notification event be triggered when an attempt was made to update a record while offline
      "notify_collision_detected": false,
      // Should a notification event be triggered when an update failed due to data collision
      "notify_remote_update_failed": false,
      // Should a notification event be triggered when an update failed for a reason other than data collision
      "notify_local_update_applied": false,
      // Should a notification event be triggered when an update was applied to the local data store
      "notify_remote_update_applied": false,
      // Should a notification event be triggered when an update was applied to the remote data store
      "notify_delta_received": false,
      // Should a notification event be triggered when a delta was received from the remote data store (dataset or record - depending on whether uid is set)
      "notify_sync_failed": false,
      // Should a notification event be triggered when the sync loop failed to complete
      "do_console_log": true
      // Should log statements be written to console.log
    },

    notifications: {
      "CLIENT_STORAGE_FAILED": "client_storage_failed",
      // loading/saving to client storage failed
      "SYNC_STARTED": "sync_started",
      // A sync cycle with the server has been started
      "SYNC_COMPLETE": "sync_complete",
      // A sync cycle with the server has been completed
      "OFFLINE_UPDATE": "offline_update",
      // An attempt was made to update a record while offline
      "COLLISION_DETECTED": "collision_detected",
      //Update Failed due to data collision
      "REMOTE_UPDATE_FAILED": "remote_update_failed",
      // Update Failed for a reason other than data collision
      "REMOTE_UPDATE_APPLIED": "remote_update_applied",
      // An update was applied to the remote data store
      "LOCAL_UPDATE_APPLIED": "local_update_applied",
      // An update was applied to the local data store
      "DELTA_RECEIVED": "delta_received",
      // A delta was received from the remote data store (dataset or record - depending on whether uid is set)
      "SYNC_FAILED": "sync_failed"
      // Sync loop failed to complete
    },

    datasets: {},

    // Initialise config to default values;
    config: undefined,

    notify_callback: undefined,

    // PUBLIC FUNCTION IMPLEMENTATIONS
    init: function(options) {
      console.log('sync - init called');
      self.config = JSON.parse(JSON.stringify(self.defaults));
      for (var i in options) {
        self.config[i] = options[i];
      }
      self.datasetMonitor();
    },

    notify: function(callback) {
      self.notify_callback = callback;
    },

    manage: function(dataset_id, options, query_params) {
      var doManage = function(dataset) {
        self.consoleLog('doManage dataset :: initialised = ' + dataset.initialised + " :: " + dataset_id + ' :: ' + JSON.stringify(options));

        // Make sure config is initialised
        if( ! self.config ) {
          self.config = JSON.parse(JSON.stringify(self.defaults));
        }

        var datasetConfig = JSON.parse(JSON.stringify(self.config));
        for (var k in options) {
          datasetConfig[k] = options[k];
        }

        dataset.query_params = query_params || {};
        dataset.config = datasetConfig;
        dataset.syncPending = true;
        dataset.initialised = true;
        self.saveDataSet(dataset_id);
      };

      // Check if the dataset is already loaded
      self.getDataSet(dataset_id, function(dataset) {
        doManage(dataset);
      }, function(err) {

        // Not already loaded, try to load from local storage
        self.loadDataSet(dataset_id, function(dataset) {
            // Loading from local storage worked

            // Fire the local update event to indicate that dataset was loaded from local storage
            self.doNotify(dataset_id, null, self.notifications.LOCAL_UPDATE_APPLIED, "load");

            // Put the dataet under the management of the sync service
            doManage(dataset);
          },
          function(err) {
            // No dataset in memory or local storage - create a new one and put it in memory
            self.consoleLog('Creating new dataset for id ' + dataset_id);
            var dataset = {};
            dataset.pending = {};
            self.datasets[dataset_id] = dataset;
            doManage(dataset);
          });
      });
    },

    list: function(dataset_id, success, failure) {
      self.getDataSet(dataset_id, function(dataset) {
        if (dataset) {
          // Return a copy of the dataset so updates will not automatically make it back into the dataset
          var res = JSON.parse(JSON.stringify(dataset.data));
          success(res);
        }
      }, function(code, msg) {
        failure(code, msg);
      });
    },

    create: function(dataset_id, data, success, failure) {
      self.addPendingObj(dataset_id, null, data, "create", success, failure);
    },

    read: function(dataset_id, uid, success, failure) {
        self.getDataSet(dataset_id, function(dataset) {
        var rec = dataset.data[uid];
        if (!rec) {
          failure("unknown_uid");
        } else {
          // Return a copy of the record so updates will not automatically make it back into the dataset
          var res = JSON.parse(JSON.stringify(rec));
          success(res);
        }
      }, function(code, msg) {
        failure(code, msg);
      });
    },

    update: function(dataset_id, uid, data, success, failure) {
      self.addPendingObj(dataset_id, uid, data, "update", success, failure);
    },

    'delete': function(dataset_id, uid, success, failure) {
      self.addPendingObj(dataset_id, uid, null, "delete", success, failure);
    },

    getPending: function(dataset_id) {
      console.log('getPending');
      self.getDataSet(dataset_id, function(dataset) {
        var res;
        if( dataset ) {
          res = dataset.pending;
        }
        console.log(JSON.stringify(res, null, 2));
        return res;
      });
    },

    clearPending: function(dataset_id) {
      self.getDataSet(dataset_id, function(dataset) {
        dataset.pending = {};
        self.saveDataSet(dataset_id);
      });
    },

    listCollisions : function(dataset_id, success, failure){
      $fh.act({
        "act": dataset_id,
        "req": {
          "fn": "listCollisions"
        }
      }, success, failure);
    },

    removeCollision: function(dataset_id, colissionHash, success, failure) {
      $fh.act({
        "act": dataset_id,
        "req": {
          "fn": "removeCollision",
          "hash": colissionHash
        }
      }, success, failure);
    },


    // PRIVATE FUNCTIONS
    isOnline: function(callback) {
      var online = true;

      // first, check if navigator.online is available
      if(typeof navigator.onLine !== "undefined"){
        online = navigator.onLine;
      }

      // second, check if Phonegap is available and has online info
      if(online){
        //use phonegap to determin if the network is available
        if(typeof navigator.network !== "undefined" && typeof navigator.network.connection !== "undefined"){
          var networkType = navigator.network.connection.type;
          if(networkType === "none" || networkType === null) {
            online = false;
          }
        }
      }

      return callback(online);
    },

    doNotify: function(dataset_id, uid, code, message) {

      if( self.notify_callback ) {
        if ( self.config['notify_' + code] ) {
          var notification = {
            "dataset_id" : dataset_id,
            "uid" : uid,
            "code" : code,
            "message" : message
          };
          // make sure user doesn't block
          setTimeout(function () {
            self.notify_callback(notification);
          }, 0);
        }
      }
    },

    getDataSet: function(dataset_id, success, failure) {
      var dataset = self.datasets[dataset_id];

      if (dataset) {
        success(dataset);
      } else {
        failure('unknown_dataset' + dataset_id, dataset_id);
      }
    },

    sortObject : function(object) {
      if (typeof object !== "object" || object === null) {
        return object;
      }

      var result = [];

      Object.keys(object).sort().forEach(function(key) {
        result.push({
          key: key,
          value: self.sortObject(object[key])
        });
      });

      return result;
    },

    sortedStringify : function(obj) {

      var str = '';

      try {
        str = JSON.stringify(self.sortObject(obj));
      } catch (e) {
        console.error('Error stringifying sorted object:' + e);
        throw e;
      }

      return str;
    },

    generateHash: function(object) {
      var hash = CryptoJS.SHA1(self.sortedStringify(object));
      return hash.toString();
    },

    addPendingObj: function(dataset_id, uid, data, action, success, failure) {
      self.isOnline(function (online) {
        if (!online) {
          self.doNotify(dataset_id, uid, self.notifications.OFFLINE_UPDATE, action);
        }
      });

      function storePendingObject(obj) {
        obj.hash = self.generateHash(obj);

        self.consoleLog("storePendingObj :: " + JSON.stringify( obj ));

        self.getDataSet(dataset_id, function(dataset) {

          dataset.pending[obj.hash] = obj;

          self.updateDatasetFromLocal(dataset, obj);

          if(self.config.auto_sync_local_updates) {
            dataset.syncPending = true;
          }
          self.saveDataSet(dataset_id);
          self.doNotify(dataset_id, uid, self.notifications.LOCAL_UPDATE_APPLIED, action);

          success(obj);
        }, function(code, msg) {
          failure(code, msg);
        });
      }

      var pendingObj = {};
      pendingObj.action = action;
      pendingObj.post = data;
      pendingObj.postHash = self.generateHash(pendingObj.post);
      pendingObj.timestamp = new Date().getTime();
      if( "create" === action ) {
        pendingObj.uid = pendingObj.postHash;
        storePendingObject(pendingObj);
      } else {
        self.read(dataset_id, uid, function(rec) {
          pendingObj.uid = uid;
          pendingObj.pre = rec.data;
          pendingObj.preHash = self.generateHash(rec.data);
          storePendingObject(pendingObj);
        }, function(code, msg) {
          failure(code, msg);
        });
      }
    },

    syncLoop: function(dataset_id) {
      self.getDataSet(dataset_id, function(dataSet) {
        // The sync loop is currently active
        dataSet.syncPending = false;
        dataSet.syncRunning = true;
        dataSet.syncLoopStart = new Date().getTime();
        self.doNotify(dataset_id, null, self.notifications.SYNC_STARTED, null);

        self.isOnline(function(online) {
          if (!online) {
            self.syncComplete(dataset_id, "offline");
          } else {
            var syncLoopParams = {};
            syncLoopParams.fn = 'sync';
            syncLoopParams.dataset_id = dataset_id;
            syncLoopParams.query_params = dataSet.query_params;
            syncLoopParams.dataset_hash = dataSet.hash;

            var pending = dataSet.pending;
            var pendingArray = [];
            for(var i in pending ) {
              pendingArray.push(pending[i]);
            }
            syncLoopParams.pending = pendingArray;

            // TODO - Move pending data to in-flight storage - keyed on new timestamp

            self.consoleLog('Starting sync loop - global hash = ' + dataSet.hash + ' :: pending = ' + JSON.stringify(pendingArray));

            $fh.act({
              'act': dataset_id,
              'req': syncLoopParams
            }, function(res) {
              self.consoleLog("Back from Sync Loop : full Dataset = " + (res.records ? " Y" : "N"));
              var rec;

              function processUpdates(updates, notification) {
                if( updates ) {
                  for (var up in updates) {
                    rec = updates[up];
                    delete dataSet.pending[up];
                    self.doNotify(dataset_id, rec.uid, notification, rec);
                  }
                }
              }

              if (res.updates) {
                processUpdates(res.updates.applied, self.notifications.REMOTE_UPDATE_APPLIED);
                processUpdates(res.updates.failed, self.notifications.REMOTE_UPDATE_FAILED);
                processUpdates(res.updates.collisions, self.notifications.COLLISION_DETECTED);
              }

              if (res.records) {
                // Full Dataset returned
                dataSet.data = res.records;

                // TODO - Merge local pending and in-flight data

                dataSet.hash = res.hash;
                self.doNotify(dataset_id, res.hash, self.notifications.DELTA_RECEIVED, 'full dataset');
                self.consoleLog("Full Dataset returned");
                self.syncComplete(dataset_id,  "online");

              }
              else if (res.hash && res.hash !== dataSet.hash) {
                self.consoleLog("Local dataset stale - syncing records :: local hash= " + dataSet.hash + " - remoteHash=" + res.hash);
                // Different hash value returned - Sync individual records
                self.syncRecords(dataset_id);
              } else {
                self.consoleLog("Local dataset up to date");
                self.syncComplete(dataset_id,  "online");
              }
            }, function(msg, err) {
              self.consoleLog("syncLoop failed : msg=" + msg + " :: err = " + err);
              self.doNotify(dataset_id, null, self.notifications.SYNC_FAILED, msg);
              self.syncComplete(dataset_id,  msg);
            });
          }
        });
      });
    },

    syncRecords: function(dataset_id) {

      self.getDataSet(dataset_id, function(dataSet) {

        var localDataSet = dataSet.data || {};

        var clientRecs = {};
        for (var i in localDataSet) {
          var uid = i;
          var hash = localDataSet[i].hash;
          clientRecs[uid] = hash;
        }

        var syncRecParams = {};

        syncRecParams.fn = 'syncRecords';
        syncRecParams.dataset_id = dataset_id;
        syncRecParams.query_params = dataSet.query_params;
        syncRecParams.clientRecs = clientRecs;

        self.consoleLog("syncRecParams :: " + JSON.stringify(syncRecParams));

        $fh.act({
          'act': dataset_id,
          'req': syncRecParams
        }, function(res) {
          var i;

          if (res.create) {
            for (i in res.create) {
              localDataSet[i] = {"hash" : res.create[i].hash, "data" : res.create[i].data};
              self.doNotify(dataset_id, i, self.notifications.DELTA_RECEIVED, "create");
            }
          }
          if (res.update) {
            for (i in res.update) {
              localDataSet[i].hash = res.update[i].hash;
              localDataSet[i].data = res.update[i].data;
              self.doNotify(dataset_id, i, self.notifications.DELTA_RECEIVED, "update");
            }
          }
          if (res['delete']) {
            for (i in res['delete']) {
              delete localDataSet[i];
              self.doNotify(dataset_id, i, self.notifications.DELTA_RECEIVED, "delete");
            }
          }

          dataSet.data = localDataSet;
          if(res.hash) {
            dataSet.hash = res.hash;
          }
          self.syncComplete(dataset_id, "online");
        }, function(msg, err) {
          self.consoleLog("syncRecords failed : msg=" + msg + " :: err=" + err);
          self.syncComplete(dataset_id, msg);
        });
      });
    },

    syncComplete: function(dataset_id, status) {
      //self.consoleLog('syncComplete');

      self.getDataSet(dataset_id, function(dataset) {
        dataset.syncRunning = false;
        dataset.syncLoopEnd = new Date().getTime();
        self.saveDataSet(dataset_id);
        self.doNotify(dataset_id, dataset.hash, self.notifications.SYNC_COMPLETE, status);
      });
    },

    checkDatasets: function() {
      for( var dataset_id in self.datasets ) {
        if( self.datasets.hasOwnProperty(dataset_id) ) {
          var dataset = self.datasets[dataset_id];

          if( !dataset.syncRunning ) {
            // Check to see if it is time for the sync loop to run again
            var lastSyncStart = dataset.syncLoopStart;
            var lastSyncCmp = dataset.syncLoopEnd;
            if( lastSyncStart == null ) {
              console.log(dataset_id +' - Performing initial sync');
              // Dataset has never been synced before - do initial sync
              dataset.syncPending = true;
            } else if (lastSyncCmp != null) {
              var timeSinceLastSync = new Date().getTime() - lastSyncCmp;
              var syncFrequency = dataset.config.sync_frequency * 1000;
              //console.log(dataset_id + ' - timeSinceLastSync = ' + timeSinceLastSync);
              //console.log(dataset_id + ' - syncFrequency = ' + syncFrequency);
              if( timeSinceLastSync > syncFrequency ) {
                //console.log(dataset_id + ' - Sync Loop time expired, starting new sync');
                // Time between sync loops has passed - do another sync
                dataset.syncPending = true;
              }
            }

            if( dataset.syncPending ) {
              // If the dataset requres syncing, run the sync loop. This may be because the sync interval has passed
              // or because the sync_frequency has been changed or because a change was made to the dataset and the
              // immediate_sync flag set to true
             self.syncLoop(dataset_id);
            }
          }
        }
      }
    },

    datasetMonitor: function() {
      // Re-execute datasetMonitor every 500ms so we keep invoking checkDatasets();
      setTimeout(function() {
        self.datasetMonitor();
      }, 500);
      self.checkDatasets();
    },

    saveDataSet: function (dataset_id) {
      var onFail =  function(msg, err) {
        // save failed
        var errMsg = 'save to local storage failed  msg:' + msg + ' err:' + err;
        self.doNotify(dataset_id, null, self.notifications.CLIENT_STORAGE_FAILED, errMsg);
        self.consoleLog(errMsg);
      };
      self.getDataSet(dataset_id, function(dataset) {
        // save dataset to local storage
        Lawnchair({fail:onFail}, function (){
             this.save({key:"dataset_" + dataset_id,val:JSON.stringify(dataset)}, function(){
               //save success
             });
        });
      });
    },

    loadDataSet: function (dataset_id, success, failure) {
      // load dataset from local storage
      var onFail = function(msg, err) {
        // load failed
        var errMsg = 'load from local storage failed  msg:' + msg;
        self.doNotify(dataset_id, null, self.notifications.CLIENT_STORAGE_FAILED, errMsg);
        self.consoleLog(errMsg);
      };

      Lawnchair({fail:onFail},function (){
         this.get( "dataset_" + dataset_id, function (data){
           if (data && data.val !== null) {
              var dataset = JSON.parse(data.val);
              // Datasets should not be auto initialised when loaded - the mange function should be called for each dataset
              // the user wants sync
              dataset.initialised = false;
              self.datasets[dataset_id] = dataset; // TODO: do we need to handle binary data?
              self.consoleLog('load from local storage success for dataset_id :' + dataset_id);
              return success(dataset);
            } else {
                // no data yet, probably first time. failure calback should handle this
                return failure();
            }
         });
      });
    },


    updateDatasetFromLocal: function(dataset, pendingRec) {
      var pending = dataset.pending;

      self.consoleLog('updateDatasetFromLocal - START');
      self.consoleLog('pending item = ' + JSON.stringify(pendingRec));

      var uid = pendingRec.uid;
      self.consoleLog('updating local dataset for uid ' + uid + ' - action = ' + pendingRec.action);

      // Creating a new record
      if( pendingRec.action === "create" ) {
        if( dataset.data[uid] ) {
          self.consoleLog('dataset already exists for uid in create :: ' + JSON.stringify(dataset.data[uid]));

          // We are trying to do a create using a uid which already exists
          if (dataset.data[uid].fromPending) {
            // We are trying to create on top of an existing pending record
            // Remove the previous pending record and use this one instead
            var previousPendingUid = dataset.data[uid].pendingUid;
            delete pending[previousPendingUid];
          }
        }
        dataset.data[uid] = {};
      }

      if( pendingRec.action === "update" ) {
        if( dataset.data[uid] ) {
          if (dataset.data[uid].fromPending) {
            self.consoleLog('updating an existing pending record for dataset :: ' + JSON.stringify(dataset.data[uid]));
            // We are trying to update an existing pending record
            var previousPendingUid = dataset.data[uid].pendingUid;
            var previousPending = pending[previousPendingUid];
            if( previousPending ) {
              self.consoleLog('existing pending record = ' + JSON.stringify(previousPending));
              if( previousPending.action == "create" ) {
                // We are trying to perform an update on an existing pending create
                // Use this update as the new create and delete the old pending create.
                pendingRec.action = "create";
                delete pending[previousPendingUid];
              }
            }
          }
        }
      }

      if( pendingRec.action === "delete" ) {
        if( dataset.data[uid] ) {
          if (dataset.data[uid].fromPending) {
            self.consoleLog('Deleting an existing pending record for dataset :: ' + JSON.stringify(dataset.data[uid]));
            // We are trying to delete an existing pending record
            var previousPendingUid = dataset.data[uid].pendingUid;
            var previousPending = pending[previousPendingUid];
            if( previousPending ) {
              self.consoleLog('existing pending record = ' + JSON.stringify(previousPending));
              if( previousPending.action == "create" ) {
                // We are trying to perform a delete on an existing pending create
                // These cancel each other out so remove them both
                delete pending[pendingRec.hash];
                delete pending[previousPendingUid];
              }
              if( previousPending.action == "update" ) {
                // We are trying to perform a delete on an existing pending update
                // Use the pre value from the pending update for the delete and
                // get rid of the pending update
                pendingRec.pre = previousPending.pre;
                pendingRec.preHash = previousPending.preHash;
                delete pending[previousPendingUid];
              }
            }
          }
          delete dataset.data[uid];
        }
      }

      if( dataset.data[uid] ) {
        self.consoleLog('Updating dataset record for uid ' + uid);
        self.consoleLog('Updating dataset record FROM ' + JSON.stringify(dataset.data[uid]));
        dataset.data[uid].data = pendingRec.post;
        dataset.data[uid].hash = pendingRec.postHash;
        dataset.data[uid].fromPending = true;
        dataset.data[uid].pendingUid = pendingRec.hash;
        self.consoleLog('Updating dataset record TO ' + JSON.stringify(dataset.data[uid]));
      }


      self.consoleLog('updateDatasetFromLocal - END');
      self.consoleLog('pending = ' + JSON.stringify(pending));
    },

    consoleLog: function(msg) {
      if( self.config.do_console_log ) {
        console.log(msg);
      }
    }
  };

  (function() {
    self.config = self.defaults;
  })();

  return {
    init: self.init,
    manage: self.manage,
    notify: self.notify,
    doList: self.list,
    doCreate: self.create,
    doRead: self.read,
    doUpdate: self.update,
    doDelete: self['delete'],
    listCollisions: self.listCollisions,
    removeCollision: self.removeCollision,
    getPending : self.getPending,
    clearPending : self.clearPending
  };
})();