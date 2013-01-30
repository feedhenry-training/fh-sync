$(document).ready(function() {

  // Create a table to store the Sync Data
  $('#table').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="shoppingList"></table>' );

  initSync();
});

function initSync() {
  // Initialise the Sync Service. See XXXX for details on initialisation options
  sync.init({});

  // Provide handler function for receiving notifications from sync service - e.g. data changed
  sync.notify(handleSyncNotifications);

  // Get the Sync service to manage the dataset called "myShoppingList"
  sync.manage("myShoppingList", {});
}

function handleSyncNotifications() {
  console.log('handleSyncNotifications :: ', arguments);
}

function reloadTable(contents) {
  $('#shoppingList').dataTable( {
    "bDestroy":true,
    "aaData": [
      contents
    ],
    "aoColumns": [
      { "sTitle": "Food Item" }
    ]
  });
}