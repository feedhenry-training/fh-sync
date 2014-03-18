$fh = $fh || {};

$fh.mbaas = function(options, success, failure) {
  $.ajax(options, success, failure);
}