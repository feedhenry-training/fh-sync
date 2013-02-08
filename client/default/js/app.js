var datasetId = 'myShoppingList';
var datasetHash;

$fh.ready(function() {

  //Bind tab events
  $('a[data-toggle="tab"]').on('shown', function (e) {
    e.target // activated tab
    e.relatedTarget // previous tab

    if(e.target.id == 'tabCollision' ) {
   //   syncAdmin.reloadCollisions();
    }
  });

 // syncUser.init();
 //  syncAdmin.init();
});