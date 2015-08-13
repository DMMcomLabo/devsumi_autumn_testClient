var hbase = require('hbase');

hbase({host:'172.16.100.180',port:20550})
.table('t1')
.create('cf3',function(err,success){
	this.row('key1').put('cf3:aaaa','hogehogehoge,',function(err,success){
		console.dir(success);
		console.dir(err);
	});
});
