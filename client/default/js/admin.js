var syncAdmin = (function() {

  var self = {

    /* JavaScript for managing administrator data collisions view */
    collisionsTable: undefined,
    collisionRecord: undefined,
    activeCollision: undefined,

    jsonEditorPre: undefined,
    jsonEditorPost: undefined,
    jsonEditorCurrent: undefined,

    init: function() {
      // Ensure the UI is set up correctly
      $('#collisions_manager').show();
      $('#collisions_editor').hide();

      // Map the jsonEdior variables to their corresponding HTML elements
      self.jsoneditorCurrent = $('#jsoneditorCurrent');
      self.jsoneditorPre = $('#jsoneditorPre');
      self.jsoneditorPost = $('#jsoneditorPost');

      // Bind UI buttons
      $('#btnReloadCollisions').unbind().click(self.getCollisions);

      $('#back_button').unbind().click(function() {
        console.log('back button click ', arguments);
        $('#collisions_manager').show();
        $('#collisions_editor').hide();
        return false;
      });

      // allow time for the dataset to be initialised.
      setTimeout(function() { self.getCollisions(); }, 1000);

    },


    reloadCollisions: function() {
      if( $('#collisions_manager').is(':visible ') ) {
        self.getCollisions();
      }
    },

    getCollisions: function() {
      $fh.sync.listCollisions(datasetId, self.showColissions, self.handleCollisionListFailure);
    },

    showColissions: function(res) {
      //console.log('showCollisions : ', res);

      // Store the collision data for use later
      self.collisionRecord = res;
      var tableData = [];

      // Iterate over the collisions to create a record structure which is suitable for the jQuery Data table
      // we are using to display the data (i.e a 2d array)

      var controls = [];
      controls.push('<button class="btn manage btn-small"><i class="icon-pencil"></i> Manage</button>&nbsp;');

      for( i in res ) {
        var row = [];
        var rec = res[i];
        row.push(i);
        row.push(rec.uid);
        row.push(moment(rec.timestamp).format('YYYY-MM-DD HH:mm:ss'));
        row.push(controls.join(""));
        tableData.push(row);
      }
      self.reloadColissionsTable(tableData);
    },

    reloadColissionsTable: function(contents) {
      $('#collisions_manager').show();
      $('#collisions_editor').hide();

      if( contents.length == 0 ) {
        $('#no_collisions').show();
        $('#collisions_list').hide();
        return;
      }

      // show the table & hide the no data message
      $('#no_collisions').hide();
      $('#collisions_list').show();

      self.collisionsTable = $('#collisionsTable').dataTable( {
        "bDestroy":true,
        "aaData": contents,
        "bLengthChange": false,
        "bFilter": false,
        "aoColumns": [
          { "sTitle": "Collision Hash" },
          { "sTitle": "Record UID" },
          { "sTitle": "Collision Date" },
          { "sTitle": "Controls", "bSortable": false, "sClass": "controls" }
        ]
      });

      self.collisionsTable.fnSetColumnVis( 0, false );

      $('tr td .manage, tr td .discard, tr td:not(.controls,.dataTables_empty)').unbind().click(function() {
        var row = $(this).parent().parent();
        var data = self.collisionsTable.fnGetData($(this).closest('tr').get(0));

        if($(this).hasClass('manage')) {
          self.doManageCollision(data);
        }
        else if( $(this).hasClass('discard')) {
          self.doDiscardCollision(data[0], false);
        }
        return false;
      });
    },

    handleCollisionListFailure: function(code, msg) {
      alert('An error occured while listing collisions : (' + code + ') ' + msg);
    },

    doManageCollision: function(data) {
      console.log(data);
      var collisionHash = data[0];
      var recordUid = data[1];
      // Bind the discard button
      $('.discard_collision_button').unbind().click(function() {
        self.doDiscardCollision(collisionHash, true);
      });

      $('.save_collision_button').unbind().click(function() {
        self.doSaveCollision(collisionHash, recordUid);
      });
      console.log(recordUid);
      //Read the current version of the record
      sync.doRead(datasetId, recordUid, function(record) {
        var collisionRec = self.collisionRecord[collisionHash];
        self.activeCollision = {
          "pre" : collisionRec.pre,
          "post" : collisionRec.post,
          "current" : record.data
        }

        // Sort all the records to ensure their fields are listed in the same order for comparision
        self.activeCollision.current = self.sortObject(self.activeCollision.current);
        self.activeCollision.pre = self.sortObject(self.activeCollision.pre);
        self.activeCollision.post = self.sortObject(self.activeCollision.post);

        // Initialise the JSON editoris with the collision data
        self.jsoneditorPre.jsonEditor(self.activeCollision.pre).find('input').attr('readonly', 'true');
        self.jsoneditorPost.jsonEditor(self.activeCollision.post).find('input').attr('readonly', 'true');
        self.jsoneditorCurrent.jsonEditor(self.activeCollision.current, {change: self.collisionChangeHandler}).find('input').attr('readonly', 'true');

        self.initCollisionUI();

        // Show the collision manager UI
        $('#collisions_manager').hide();
        $('#collisions_editor').show();
      }, function(code, msg) {
        alert('An error occured while reading the latest version of the record for collision management : (' + code + ') ' + msg);
      });
    },

    initCollisionUI: function() {
      // Set UI layout for collision lists
      $('.json-editor').find('input.property').addClass('span5');
      $('.json-editor').find('input.value').addClass('span7 pull-right');

      // Function to invoke when copying collision changes from pre or post records
      var copyClickHandler = function(e) {
        e.preventDefault();
        var el = $(this);

        var propName = el.parent().find('input.property').attr('title');
        var propVal = el.parent().find('input.value').val();

        var target = self.jsoneditorCurrent.find('input.property[title="' + propName + '"]').parent();
        target.find('input.value').val(propVal).trigger('change');
      }

      // highlight conflict data and add 'copy changes' button for each change
      for (var key in self.activeCollision.current) {
        // pre or post different?

        if (
          (self.activeCollision.pre && self.activeCollision.pre[key] !== self.activeCollision.current[key]) ||
          (self.activeCollision.post && self.activeCollision.post[key] !== self.activeCollision.current[key])
        ) {
          if (self.activeCollision.pre){
            // JSON Table stringifies all keys, so we need to do the same in order to find UI elements
            var stringifiedKey = JSON.stringify(self.activeCollision.pre[key]);
            var inputProperty = self.jsoneditorPre.find('input.property[title="' + key + '"]');
            var inputRow = inputProperty.parent();

            // Highlight the data as a collision
            inputRow.addClass('collision');

            // Create and initialise a button for copying the pre data as the conflict resolution
            var copyPreBtn = $('<div>');
            copyPreBtn.css({
              "float": "right",
              "width": 0,
              "height": 0
            });
            copyPreBtn.append($('<button>', {
              "class": "btn btn-inverse btnCopy btnCopyPre",
              "text": ">"
            }));
            copyPreBtn.on('click', copyClickHandler);

            // Add the copy button to the UI
            inputProperty.after(copyPreBtn);
          }

          if (self.activeCollision.post){
            var stringifiedKey = JSON.stringify(self.activeCollision.post[key]);
            var inputProperty = self.jsoneditorPost.find('input.property[title="' + key + '"]');
            var inputRow = inputProperty.parent();

            // Highlight the data as a collision
            inputRow.addClass('collision');

            // Create and initialise a button for copying the post data as the conflict resolution
            var copyPostBtn = $('<div>');
            copyPostBtn.css({
              "position": "absolute"
            });
            copyPostBtn.append($('<button>', {
              "class": "btn btn-inverse btnCopy btnCopyPost",
              "text": "< "
            }));
            copyPostBtn.on('click', copyClickHandler);

            inputProperty.before(copyPostBtn);
          }

          // Highlight the row in current version of the conflict record
          self.jsoneditorCurrent.find('input.property[title="' + key + '"]').parent().addClass('collision')
        }
      }
    },

    collisionChangeHandler: function() {
      console.log('collisionChangeHandler : ', self.activeCollision.current);
    },

    doDiscardCollision: function(collisionHash, hideEditor) {
      sync.removeCollision(datasetId, collisionHash, function() {
        //Colission successfully removed. Hide colission editor and reload table
        if( hideEditor ) {
          $('#collisions_editor').hide();
        }
        self.getCollisions();
      }, function(code, msg) {
        alert('An error occured while removing the collision record : (' + code + ') ' + msg);
      });
    },

    doSaveCollision: function(collisionHash, recordUid) {
      console.log('currentRec = ', self.activeCollision.current);
      sync.doUpdate(datasetId, recordUid, self.activeCollision.current, function(res) {
        alert('Collision successfully resolved');
        self.doDiscardCollision(collisionHash, true);
      }, function(code, msg) {
        alert('An error occured while udating the collision record : (' + code + ') ' + msg);
      });
    },

    sortObject: function(o) {
      var sorted = {},
        key, a = [];

      for (key in o) {
        if (o.hasOwnProperty(key)) {
          a.push(key);
        }
      }

      a.sort();

      for (key = 0; key < a.length; key++) {
        sorted[a[key]] = o[a[key]];
      }
      return sorted;
    }
  };

  return {
    init: self.init,
    reloadCollisions: self.reloadCollisions
  }
})();
