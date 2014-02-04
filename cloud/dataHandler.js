var collisions = {};

exports.doList = function(dataset_id, params, cb, meta_data) {
  console.log("doList : ", dataset_id, " :: ", params, ' :: meta=', meta_data);

  $fh.db({
    "act": "list",
    "type": dataset_id
  }, function(err, res) {
    if (err) return cb(err);

    var resJson = {};

    for (var di = 0, dl = res.list.length; di < dl; di += 1) {
      resJson[res.list[di].guid] = res.list[di].fields;
    }

    if( meta_data && meta_data.syncDelay && !isNaN(meta_data.syncDelay) ) {
      // Simulate a delay with list operation
      setTimeout(function() {
        return cb(null, resJson);
      }, (meta_data.syncDelay * 1000))
    }
    else {
      return cb(null, resJson);
    }
  });
};

exports.doCreate = function(dataset_id, data, cb, meta_data) {
  console.log("Starting doCreate : ", dataset_id, " :: ", data, ' :: meta=', meta_data);

  var recordDelay;
  if( meta_data && meta_data.recordDelay && !isNaN(meta_data.recordDelay) ) {
    recordDelay = meta_data.recordDelay;
  }

  function createImpl() {
    var dataStr = JSON.stringify(data);
    if( dataStr.indexOf("FAILURE") >= 0) {
      return cb('Create Failure');
    }

    $fh.db({
      "act": "create",
      "type": dataset_id,
      "fields": data
    }, function(err, res) {
      if (err) return cb(err);

      var data = {'uid': res.guid, 'data': res.fields};
      console.log("Finished doCreate : ", data);
      return cb(null, data);
    });
  }

  if( recordDelay) {
    // Simulate a delay with create operation
    setTimeout(function() {
      createImpl();
    }, (recordDelay * 1000));
  }
  else {
    createImpl();
  }

};

exports.doRead = function(dataset_id, uid, cb, meta_data) {
  console.log("doRead : ", dataset_id, " :: ", uid, ' :: meta=', meta_data);

  $fh.db({
    "act": "read",
   "type": dataset_id,
   "guid": uid
  }, function(err, res) {
   if (err) return cb(err);

    return cb (null, res.fields);
  });

};

exports.doUpdate = function(dataset_id, uid, data, cb, meta_data) {
  console.log("doUpdate : ", dataset_id, " :: ", uid, " :: ", data, ' :: meta=', meta_data);

  var recordDelay;
  if( meta_data && meta_data.recordDelay && !isNaN(meta_data.recordDelay) ) {
    recordDelay = meta_data.recordDelay;
  }

  function updateImpl() {
    var dataStr = JSON.stringify(data);
    if( dataStr.indexOf("FAILURE") >= 0) {
      return cb(new Error("You asked for failure - you got failure"));
    }

    $fh.db({
      "act": "update",
      "type": dataset_id,
      "guid": uid,
      "fields": data
    }, function(err, res) {
      if (err) return cb(err);

      console.log("Finished doUpdate : ", res);
      return cb(null, res.fields);
    });
  }

  if( recordDelay) {
    // Simulate a delay with create operation
    setTimeout(function() {
      updateImpl();
    }, (recordDelay * 1000));
  }
  else {
    updateImpl();
  }

};

exports.doDelete = function(dataset_id, uid, cb, meta_data) {
  console.log("doDelete : ", dataset_id, " :: ", uid, ' :: meta=', meta_data);

  var recordDelay;
  if( meta_data && meta_data.recordDelay && !isNaN(meta_data.recordDelay) ) {
    recordDelay = meta_data.recordDelay;
  }

  function deleteImpl() {
    $fh.db({
      "act": "delete",
      "type": dataset_id,
      "guid": uid
    }, function(err, res) {
      if (err) return cb(err);

      return cb(null, res.fields);
    });
  }

  if( recordDelay) {
    // Simulate a delay with create operation
    setTimeout(function() {
      deleteImpl();
    }, (recordDelay * 1000));
  }
  else {
    deleteImpl();
  }
};

exports.doCollision = function(dataset_id, hash, timestamp, uid, pre, post, meta_data) {
  console.log("doCollision : ", dataset_id, " :: hash= ", hash, " :: timestamp= ", timestamp, " :: uid= ", uid, " :: pre= ", pre, " :: post= ", post, ' :: meta=', meta_data);
  var fields = {
    "hash" : hash,
    "timestamp" : timestamp,
    "uid" : uid,
    "pre" : pre,
    "post" : post
  };

  $fh.db({
    "act": "create",
    "type": dataset_id + '_collision',
    "fields": fields
  },function (err){
      if(err) console.log(err);
  });
};

exports.listCollisions = function(dataset_id, cb, meta_data) {
  console.log("listCollisions : ", dataset_id,  ' :: meta=', meta_data);
  $fh.db({
    "act": "list",
    "type": dataset_id + '_collision'
  }, function(err, res) {
    if(err) return cb(err);

    var resJson = {};

    for (var di = 0; di < res.list.length; di++) {
      resJson[res.list[di].fields.hash] = res.list[di].fields;
    }

    cb(null, resJson);
  });
};

exports.removeCollision = function(dataset_id, hash, cb, meta_data) {
  console.log("removeCollision : ", dataset_id,  ' :: meta=', meta_data);
  $fh.db({
    "act": "list",
    "type": dataset_id + '_collision',
    "eq": {
      "hash": hash
    }
  }, function(err, data) {
    if(err) cb(err);
    console.log('removeCollision : ', data)

    if( data.list && data.list.length == 1 ) {
      var guid = data.list[0].guid;
      $fh.db({
        "act": "delete",
        "type": dataset_id + '_collision',
        "guid": guid
      }, cb);
    } else {
      return cb(null, {"status":"ok", "message":"No collision found for hash " + hash});
    }
  });
}