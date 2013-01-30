var sync = require('./sync-srv.js');
var dataHandler = require('./dataHandler.js');

/* main.js
 * All calls here are publicly exposed as REST API endpoints.
 * - all parameters must be passed in a single JSON paramater.
 * - the return 'callback' method signature is 'callback (error, data)', where 'data' is a JSON object.
 */

/* 'helloWorld' server side REST API method.
 * Where it always begins. Not related to Data Sync... just a simple hellow world function which can be used to ensure
 * cloud code is running.
 */
exports.helloWorld = function(params, callback) {
    console.log("In helloWorld() call");
    return callback(null, {"hello": "world"});
};


/* dataset_id = The namespace for the dataset. This allows multiple different datasets to be managed
by a single applications.
 */
var dataset_id = "myShoppingList";

/*
 * The Data Sync Framework manages syncing data between the App Cloud and the App Client (i.e. mobile device).
 * The app developer must provide the functions for handling data synchronisation between the back end data store
 * and the App Cloud. In this sample app, the "back end data store" is a simple Cloud Database which is implemented
 * using the $fh.db() API. See dataHandler.js for the implementation of the various functions defined below.
 */
sync.init(dataset_id, {}, function() {
  sync.handleList(dataset_id, dataHandler.doList);
  sync.handleCreate(dataset_id, dataHandler.doCreate);
  sync.handleRead(dataset_id, dataHandler.doRead);
  sync.handleUpdate(dataset_id, dataHandler.doUpdate);
  sync.handleDelete(dataset_id, dataHandler.doDelete);
  sync.handleCollision(dataset_id, dataHandler.doCollision);
  sync.listCollisions(dataset_id, dataHandler.listCollisions);
  sync.removeCollision(dataset_id, dataHandler.removeCollision);
});

exports.myShoppingList = function (params, callback) {
  return sync.invoke('myShoppingList', params, callback);
};