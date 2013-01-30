var collisions = {};

exports.doList = function(dataset_id, params, cb) {
  console.log("doList : ", dataset_id, " :: ", params);

  $fh.db({
    "act": "list",
    "type": dataset_id
  }, function(err, res) {
    if (err) return cb(err);

    var resJson = {};

    for (var di = 0, dl = res.list.length; di < dl; di += 1) {
      resJson[res.list[di].guid] = res.list[di].fields;
    }

    return cb(null, resJson);
  });
};

exports.doCreate = function(dataset_id, data, cb) {
  console.log("doCreate : ", dataset_id, " :: ", data);

  $fh.db({
    "act": "create",
    "type": dataset_id,
    "fields": data
  }, function(err, res) {
    if (err) return cb(err);

    var data = {'uid': res.guid, 'data': res.fields};
    return cb(null, data);
  });
};

exports.doRead = function(dataset_id, uid, cb) {
  console.log("doRead : ", dataset_id, " :: ", uid);

  $fh.db({
    "act": "read",
    "type": dataset_id,
    "guid": uid
  }, function(err, res) {
    if (err) return cb(err);

    return cb (null, res.fields);
  });
};

exports.doUpdate = function(dataset_id, uid, data, cb) {
  console.log("doUpdate : ", dataset_id, " :: ", uid, " :: ", data);

  $fh.db({
    "act": "update",
    "type": dataset_id,
    "guid": uid,
    "fields": data
  }, function(err, res) {
    if (err) return cb(err);

    return cb(null, res.fields);
  });
};

exports.doDelete = function(dataset_id, uid, cb) {
  console.log("doDelete : ", dataset_id, " :: ", uid);

  $fh.db({
    "act": "delete",
    "type": dataset_id,
    "guid": uid
  }, function(err, res) {
    if (err) return cb(err);

    return cb(null, res.fields);
  });
};

exports.doCollision = function(dataset_id, hash, uid, pre, post) {
  console.log("doCollision : ", dataset_id, " :: hash= ", hash, " :: uid= ", uid, " :: pre= ", pre, " :: post= ", post);
  collisions[hash] = {"uid":uid, "pre":pre, "post":post};
};

exports.getCollision = function(dataset_id) {
  return collisions;
};

exports.removeCollision = function(dataset_id, hash, cb) {
  var res = collisions[hash];

  if( res ) {
    delete collisions[hash];
    return cb(null, null);
  } else {
    return cb("removeCollision :: No collision found for hash " + hash);
  }
}