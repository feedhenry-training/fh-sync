var datasetId = 'myShoppingList';
var datasetHash;

$(document).ready(function() {
  initSync();
});

function initSync() {
  // Initialise the Sync Service. See XXXX for details on initialisation options
  sync.init({
    "sync_frequency": 5,
    "auto_sync_local_updates": true,
    "notify_client_storage_failed": false,
    "notify_sync_started": false,
    "notify_sync_complete": true,
    "notify_offline_update": false,
    "notify_collision_detected": true,
    "notify_update_failed": false,
    "notify_update_applied": false,
    "notify_delta_received": false
  });

  // Provide handler function for receiving notifications from sync service - e.g. data changed
  sync.notify(handleSyncNotifications);

  // Get the Sync service to manage the dataset called "myShoppingList"
  sync.manage(datasetId, {});

  // Request the initial dataset from the sync service
  sync.list(datasetId, handleListSuccess, handleListFailure);

  //sync.create(datasetId, JSON.parse(document.getElementById('data').value), handleSuccess, handleFailure);" value="Create"/>
  //sync.read(datasetId, document.getElementById('uid').value, handleSuccess, handleFailure);" value="Read"/>
  //sync.update(datasetId, document.getElementById('uid').value, JSON.parse(document.getElementById('data').value), handleSuccess, handleFailure);" value="Update"/>
  //sync.delete(datasetId, document.getElementById('uid').value, handleSuccess, handleFailure);" value="Delete"/>

}

function handleSyncNotifications(notification) {
  console.log('############ handleSyncNotifications :: notification = ', notification);
  if( 'sync_complete' == notification.code ) {
    // We are interetsed in sync_complete notifications as there may be changes to the dataset
    if( datasetHash != notification.uid ) {
      // The dataset hash received in the uid parameter is different to the one we have stored.
      // This means that there has been a change in the dataset, so we should invoke the list operation.
      datasetHash = notification.uid;
      sync.list(datasetId, handleListSuccess, handleListFailure);
    }
  }
}

function handleListSuccess(res) {
  console.log('handleListSuccess :: ', arguments);
  var tableData = [];
  // Iterate over the dataset to create a record structure which is suitable for the jQuery Data table
  // we are using to display the data (i.e a 2d array)
  for( i in res ) {
    var row = [];
    var rec = res[i];
    row.push(i);
    row.push(rec.data.name);
    row.push(new Date(rec.data.created));

    tableData.push(row);
  }
  reloadTable(tableData);
}

function handleListFailure(code, msg) {
  alert('An error occured while listing data : (' + code + ') ' + msg);
}

function handleCreateSuccess() {
  console.log('handleCreateSuccess :: ', arguments);
}

function handleCreateFailure() {
  console.log('handleCreateFailure :: ', arguments);
}

function addItem() {
  console.log('addItem Called...');
  var name = $('#itemIn').val();
  var created = new Date().getTime();
  var dataItem = {
    "name" : name,
    "created" : created
  };
  sync.create(datasetId, dataItem, handleCreateSuccess, handleCreateFailure);
  $('#itemIn').val('');
}

function updateItem() {
  console.log('addItem Called...');
  var name = $('#itemIn').val();
  var created = new Date().getTime();
  var dataItem = {
    "name" : name,
    "created" : created
  };
  sync.create(datasetId, dataItem, handleCreateSuccess, handleCreateFailure);
  $('#itemIn').val('');
}

function reloadTable(contents) {
  console.log('reloadTable :: ', contents);
  // Create a table to store the Sync Data
  $('#table').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="shoppingList"></table>' );

  $('#shoppingList').dataTable( {
    "bDestroy":true,
    "aaData": contents,
    "aoColumns": [
      { "sTitle": "UID" },
      { "sTitle": "Food Item" },
      { "sTitle": "Date Created" },
      { "sTitle": "Controls", "bSortable": false, "sClass": "controls" }
    ]
  });
}