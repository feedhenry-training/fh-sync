var dataHandler = require('./dataHandler.js');
console.log(process.env);

/* main.js
 * All calls here are publicly exposed as REST API endpoints.
 * - all parameters must be passed in a single JSON paramater.
 * - the return 'callback' method signature is 'callback (error, data)', where 'data' is a JSON object.
 */


/* dataset_id = The namespace for the dataset. This allows multiple different datasets to be managed
 by a single applications.
 */
var dataset_id = "myShoppingList";

/* To allow the sync client to interact with the cloud dataset, a function is required in main.js who's
 * name is the same as the dataset_id - in this case "myShoppingList". The implmentation for this function
 * is alwyas a call to sync.invoke() - passing the dataset_id, the request parameters and the callback.
 *
 */
exports.myShoppingList = function(params, callback) {
  return $fh.sync.invoke(dataset_id, params, callback);
};

/* Public function to support stoping syncronisation of an individual dataset */
exports.stopSync = function(params, callback) {
  return $fh.sync.stop(dataset_id, callback);
};

/* Public function to support stoping syncronisation of all datasets (Since there is only 1 dataset active in this
 * example, the stopAllSync() function is somewhat redundent */
exports.stopAllSync = function(params, callback) {
  return $fh.sync.stopAll(callback);
};

/*
 * The Data Sync Framework manages syncing data between the App Cloud and the App Client (i.e. mobile device).
 * The app developer must provide the functions for handling data synchronisation between the back end data store
 * and the App Cloud. In this sample app, the "back end data store" is a simple Cloud Database which is implemented
 * using the $fh.db() API. See dataHandler.js for the implementation of the various functions defined below.
 */
$fh.sync.init(dataset_id, {}, function() {
  $fh.sync.handleList(dataset_id, dataHandler.doList);
  $fh.sync.handleCreate(dataset_id, dataHandler.doCreate);
  $fh.sync.handleRead(dataset_id, dataHandler.doRead);
  $fh.sync.handleUpdate(dataset_id, dataHandler.doUpdate);
  $fh.sync.handleDelete(dataset_id, dataHandler.doDelete);
  $fh.sync.handleCollision(dataset_id, dataHandler.doCollision);
  $fh.sync.listCollisions(dataset_id, dataHandler.listCollisions);
  $fh.sync.removeCollision(dataset_id, dataHandler.removeCollision);
});
