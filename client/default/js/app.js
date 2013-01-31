var datasetId = 'myShoppingList';
var datasetHash;
var table;

$(document).ready(function() {

  $('#updateBtn').attr('disabled', 'disabled');
  initSync();
});

function initSync() {
  // Initialise the Sync Service. See XXXX for details on initialisation options
  sync.init({
    "sync_frequency": 5,
    "auto_sync_local_updates": true,
    "notify_client_storage_failed": true,
    "notify_sync_started": true,
    "notify_sync_complete": true,
    "notify_offline_update": true,
    "notify_collision_detected": true,
    "notify_update_failed": true,
    "notify_update_applied": true,
    "notify_delta_received": true
  });

  // Provide handler function for receiving notifications from sync service - e.g. data changed
  sync.notify(handleSyncNotifications);

  // Get the Sync service to manage the dataset called "myShoppingList"
  sync.manage(datasetId, {});

  // Request the initial dataset from the sync service
  sync.list(datasetId, handleListSuccess, handleListFailure);
}

function handleSyncNotifications(notification) {
  var msg = new Date() + ' : ' + notification.code + ' (uid:' + notification.uid + ', msg:' + notification.message + ')\n';
  $('#notifications').val(msg + $('#notifications').val());

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

function clearNotifications() {
  $('#notifications').val('');
}

function handleListSuccess(res) {
  var tableData = [];
  // Iterate over the dataset to create a record structure which is suitable for the jQuery Data table
  // we are using to display the data (i.e a 2d array)

  var controls = [];
  controls.push('<button class="btn edit">Edit</button>&nbsp;');
  controls.push('<button class="btn delete">Delete</button>&nbsp;');

  for( i in res ) {
    var row = [];
    var rec = res[i];
    row.push(i);
    row.push(rec.data.name);
    row.push(new Date(rec.data.created));
    row.push(controls.join(""));
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
  // Read the data from the fields
  var name = $('#itemUp').val();
  var uid = $('#itemUpId').val();

  // Reset fields and disable update button
  $('#itemUp').val('');
  $('#itemUpId').val('');
  $('#updateBtn').attr('disabled', 'disabled');

  // Read the full record from the sync service
  sync.read(datasetId, uid, function(res) {
    var data = res.data;
    // Update the name field with the updated value from the text box
    data.name = name;

    // Send the update to the sync service
    sync.update(datasetId, uid, data, function(res) {
      //Update completed successfully, nothing to do here
    },
    function(code, msg) {
      alert('Unable to update row : (' + code + ') ' + msg);
    });
  }, function(code, msg) {
    alert('Unable to read row for updating : (' + code + ') ' + msg);
  });


}

function reloadTable(contents) {
  console.log('reloadTable :: ', contents);
  // Create a table to store the Sync Data
  $('#table').html( '<table cellpadding="0" cellspacing="0" border="0" class="table table-striped table-bordered" id="shoppingList"></table>' );

  table = $('#shoppingList').dataTable( {
    "bDestroy":true,
    "aaData": contents,
    "aoColumns": [
      { "sTitle": "UID" },
      { "sTitle": "Food Item" },
      { "sTitle": "Date Created" },
      { "sTitle": "Controls", "bSortable": false, "sClass": "controls" }
    ]
  });

  $('tr td .edit, tr td .delete, tr td:not(.controls,.dataTables_empty)').unbind().click(function() {
    var row = $(this).parent().parent();
    var data = table.fnGetData($(this).closest('tr').get(0));

    if($(this).hasClass('edit')) {
      doEditRow(data);
    }
    else if( $(this).hasClass('delete')) {
      doDeleteRow(data);
    }
    return false;
  });
}

function doEditRow(row) {
  sync.read(datasetId, row[0], function(res) {
    console.log('read ', res);
    $('#itemUp').val(res.data.name);
    $('#itemUpId').val(row[0]);
    $('#updateBtn').removeAttr('disabled');
  },
  function(code, msg) {
    alert('Unable to read row for editing : (' + code + ') ' + msg);
  });
}

function doDeleteRow(row) {
  sync.read(datasetId, row[0], function(res) {
    var doDelete = confirm('Are you sure you wish to delete this row')
    if( doDelete ) {
      sync.delete(datasetId, row[0], function(res) {
        // Successfully deleted - no need to do anything
      },
      function(code, msg) {
        alert('Unable to delete row : (' + code + ') ' + msg);
      });
    }
  },
  function(code, msg) {
    alert('Unable to read row for deleting : (' + code + ') ' + msg);
  });
}