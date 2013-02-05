/* JavaScript for managing administrator data collisions view */
var collisionsTable;
var collisionRecord;
var activeCollision;

var jsonEditorPre, jsonEditorPost, jsonEditorCurrent;

function getCollisions() {
  sync.listCollisions(datasetId, showColissions, handleCollisionListFailure);
}

function showColissions(res) {
  // Store the collision data for use later
  collisionRecord = res;
  var tableData = [];
  // Iterate over the dataset to create a record structure which is suitable for the jQuery Data table
  // we are using to display the data (i.e a 2d array)

  console.log(res);
  
  var controls = [];
  controls.push('<button class="btn manage">Manage</button>&nbsp;');
  controls.push('<button class="btn discard">Discard</button>&nbsp;');

  for( i in res ) {
    var row = [];
    var rec = res[i];
    row.push(i);
    row.push(rec.uid);
    row.push(rec.timestamp);
    row.push(controls.join(""));
    tableData.push(row);
  }
  reloadColissionsTable(tableData);

}

function reloadColissionsTable(contents) {
  console.log('reloadTable :: ', contents);

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

  collisionsTable = $('#collisionsTable').dataTable( {
    "bDestroy":true,
    "aaData": contents,
    "aoColumns": [
      { "sTitle": "Collision Hash" },
      { "sTitle": "Record UID" },
      { "sTitle": "Collision Date" },
      { "sTitle": "Controls", "bSortable": false, "sClass": "controls" }
    ]
  });

  $('tr td .manage, tr td .discard, tr td:not(.controls,.dataTables_empty)').unbind().click(function() {
    var row = $(this).parent().parent();
    var data = collisionsTable.fnGetData($(this).closest('tr').get(0));

    if($(this).hasClass('manage')) {
      doManageCollision(data);
    }
    else if( $(this).hasClass('discard')) {
      doDiscardCollision(data[0], false);
    }
    return false;
  });
}

function handleCollisionListFailure(code, msg) {
  alert('An error occured while listing collisions : (' + code + ') ' + msg);
}

function doManageCollision(data) {
  var collisionHash = data[0];
  var recordUid = data[1];
  // Bind the discard button
  $('.discard_collision_button').unbind().click(function() {
    doDiscardCollision(collisionHash, true);
  });

  $('.save_collision_button').unbind().click(function() {
    doSaveCollision(collisionHash, recordUid);
  });

  $('#back_button').unbind().click(function() {
    console.log('back button click ', arguments);
    $('#collisions_manager').show();
    $('#collisions_editor').hide();
    return false;
  });

  //Read the current version of the record
  sync.read(datasetId, recordUid, function(record) {
    var collisionRec = collisionRecord[collisionHash];
    activeCollision = {
      "pre" : collisionRec.pre,
      "post" : collisionRec.post,
      "current" : record.data
    }

    // Sort all the records to ensure their fields are listed in the same order for comparision
    activeCollision.current = sortObject(activeCollision.current);
    activeCollision.pre = sortObject(activeCollision.pre);
    activeCollision.post = sortObject(activeCollision.post);

    jsoneditorPre.jsonEditor(activeCollision.pre).find('input').attr('readonly', 'true');
    jsoneditorPost.jsonEditor(activeCollision.post).find('input').attr('readonly', 'true');
    jsoneditorCurrent.jsonEditor(activeCollision.current, {change: collisionChangeHandler}).find('input').attr('readonly', 'true');

    $('.json-editor').find('input.property').addClass('span5').end()
      .find('input.value').addClass('span7 pull-right');

    // flat diff of json objects and highlight modified data,
    // adding 'copy changes' button for each change

    for (var key in activeCollision.current) {
      // pre or post different?
      console.log(key);
      if ((activeCollision.pre && activeCollision.pre[key] !== activeCollision.current[key]) || (activeCollision.post && activeCollision.post[key] !== activeCollision.current[key])) {
        if (activeCollision.pre){
          var stringifiedKey = JSON.stringify(activeCollision.pre[key]);
          var inputProperty = jsoneditorPre.find('input.property[title="' + key + '"]');
          var inputRow = inputProperty.parent();
          inputRow.addClass('activeCollision');
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
          copyPreBtn.on('click', function (e) {
            e.preventDefault();
            var el = $(this);
            var propName = el.prev().val();
            jsoneditorCurrent.find('input.property[title="' + propName + '"]').next().val(el.next().val()).trigger('change');
          });

          inputProperty.after(copyPreBtn);
        }

        if (activeCollision.post){
          jsoneditorPost.find('input.property[title="' + key + '"]').parent().addClass('activeCollision')
            .find('input.property').before($('<div>').css({
            "position": "absolute"
          }).append($('<button>', {
            "class": "btn btn-inverse btnCopy btnCopyPost",
            "text": "<"
          })).on('click', function (e) {
              e.preventDefault();
              var el = $(this);
              var propName = el.next().val();
              jsoneditorCurrent.find('input.property[title="' + propName + '"]').next().val(el.next().next().val()).trigger('change');
            }));
        }

        jsoneditorCurrent.find('input.property[title="' + key + '"]').parent().addClass('activeCollision')
          .find('input.value').removeAttr('readonly');
      }
    }

    // Show the collision manager UI
    $('#collisions_manager').hide();
    $('#collisions_editor').show();
  }, function(code, msg) {
    alert('An error occured while reading the latest version of the record for collision management : (' + code + ') ' + msg);
  });
}

function collisionChangeHandler() {
  console.log('collisionChangeHandler : ', activeCollision.current);
}

function doDiscardCollision(collisionHash, hideEditor) {
  sync.removeCollision(datasetId, collisionHash, function() {
    //Colission successfully removed. Hide colission editor and reload table
    if( hideEditor ) {
      $('#collisions_editor').hide();
    }
    getCollisions();
  }, function(code, msg) {
    alert('An error occured while removing the collision record : (' + code + ') ' + msg);
  });
}

function doSaveCollision(collisionHash, recordUid) {
  console.log('currentRec = ', activeCollision.current);
  sync.update(datasetId, recordUid, activeCollision.current, function(res) {
    alert('Collision successfully resolved');
    doDiscardCollision(collisionHash, true);
  }, function(code, msg) {
    alert('An error occured while udating the collision record : (' + code + ') ' + msg);
  });
}

function sortObject(o) {
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