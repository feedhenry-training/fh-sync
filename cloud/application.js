var nodeapp = require("fh-nodeapp-test");
nodeapp.HostApp.init();
nodeapp.HostApp.serveApp(require('main.js'));
