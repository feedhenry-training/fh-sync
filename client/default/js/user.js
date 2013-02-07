var syncUser = (function() {

  var self = {

    syncTable: undefined,

    init: function() {
      // Ensure UI is set up correctly
      $('#updateBtn').attr('disabled', 'disabled');

      $('#isOnlineChk').unbind().click(self.setOnline);
      $('#updateBtn').unbind().click(self.updateItem);
      $('#addBtn').unbind().click(self.addItem);
      $('#clearNotificationsBtn').unbind().click(self.clearNotifications);

      // Initialise the Sync Service. See XXXX for details on initialisation options
      $fh.sync.init({
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
      $fh.sync.notify(self.handleSyncNotifications);

      // Get the Sync service to manage the dataset called "myShoppingList"
      $fh.sync.manage(datasetId, {});

      // Request the initial dataset from the sync service
      $fh.sync.doList(datasetId, self.handleListSuccess, self.handleListFailure);
    },

    handleSyncNotifications: function(notification) {
      var msg = new Date() + ' : ' + notification.code + ' (uid:' + notification.uid + ', msg:' + notification.message + ')\n';
      $('#notifications').val(msg + $('#notifications').val());

      if( 'sync_complete' == notification.code ) {
        // We are interetsed in sync_complete notifications as there may be changes to the dataset
        if( datasetHash != notification.uid ) {
          // The dataset hash received in the uid parameter is different to the one we have stored.
          // This means that there has been a change in the dataset, so we should invoke the list operation.
          datasetHash = notification.uid;
          $fh.sync.doList(datasetId, self.handleListSuccess, self.handleListFailure);
        }
      }
    },

    clearNotifications: function() {
      $('#notifications').val('');
    },

    handleListSuccess: function(res) {
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

      self.reloadTable(tableData);
    },

    handleListFailure: function(code, msg) {
      alert('An error occured while listing data : (' + code + ') ' + msg);
    },

    addItem: function() {
      console.log('addItem Called...');
      var name = $('#itemIn').val();
      var created = new Date().getTime();
      var dataItem = {
        "name" : name,
        "created" : created
      };
      $fh.sync.doCreate(datasetId, dataItem, function(res) {
        console.log('Create item success');
      }, function(code, msg) {
        alert('An error occured while creating data : (' + code + ') ' + msg);
      });

      // Clear the add item text box
      $('#itemIn').val('');
    },

    updateItem: function() {
      // Read the data from the fields
      var name = $('#itemUp').val();
      var uid = $('#itemUpId').val();

      // Reset fields and disable update button
      $('#itemUp').val('');
      $('#itemUpId').val('');
      $('#updateBtn').attr('disabled', 'disabled');

      // Read the full record from the sync service
      $fh.sync.doRead(datasetId, uid, function(res) {
        var data = res.data;
        // Update the name field with the updated value from the text box
        data.name = name;

        // Send the update to the sync service
        $fh.sync.doUpdate(datasetId, uid, data, function(res) {
          console.log('Update item success');
        },
        function(code, msg) {
          alert('Unable to update row : (' + code + ') ' + msg);
        });
      }, function(code, msg) {
        alert('Unable to read row for updating : (' + code + ') ' + msg);
      });
    },

    reloadTable: function(contents) {
      console.log('reloadTable :: ', contents);
      if( contents.length == 0 ) {
        $('#nosyncdata').show();
        $('#table').hide();
        return;
      }

      // show the table & hide the no data message
      $('#nosyncdata').hide();
      $('#table').show();

      self.syncTable = $('#shoppingList').dataTable( {
        "bDestroy":true,
        "bLengthChange": false,
        "bFilter": false,
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
        var data = self.syncTable.fnGetData($(this).closest('tr').get(0));

        if($(this).hasClass('edit')) {
          self.doEditRow(data);
        }
        else if( $(this).hasClass('delete')) {
          self.doDeleteRow(data);
        }
        return false;
      });
    },

    doEditRow: function(row) {
      $fh.sync.doRead(datasetId, row[0], function(res) {
        console.log('read ', res);
        $('#itemUp').val(res.data.name);
        $('#itemUpId').val(row[0]);
        $('#updateBtn').removeAttr('disabled');
      },
      function(code, msg) {
        alert('Unable to read row for editing : (' + code + ') ' + msg);
      });
    },

    doDeleteRow: function(row) {
      $fh.sync.doRead(datasetId, row[0], function(res) {
        var doDelete = confirm('Are you sure you wish to delete this row')
        if( doDelete ) {
          $fh.sync["delete"](datasetId, row[0], function(res) {
            console.log('Delete item success');
          },
          function(code, msg) {
            alert('Unable to delete row : (' + code + ') ' + msg);
          });
        }
      },
      function(code, msg) {
        alert('Unable to read row for deleting : (' + code + ') ' + msg);
      });
    },

    setOnline: function() {
      var isOnline = $('#isOnlineChk').is(":checked")
      console.log('isOnline = ' + isOnline);
      //navigator.network.connection.type
      navigator.network = navigator.network || {};
      navigator.network.connection = navigator.network.connection || {}
      navigator.network.connection.type = isOnline ? 'WiFi' : 'none';
    }
  };

  return {
    init: self.init
  }
})();