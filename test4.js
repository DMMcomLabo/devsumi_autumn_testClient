var kafka = require('kafka-node'),
Consumer = kafka.HighLevelConsumer,
client = new kafka.Client('172.27.100.11:2181,172.27.100.12:2181,172.27.100.13:2181'),
consumer = new Consumer(client,[{topic:"tracking_summary"}],{groupId:"server"});
var Long = require('long');

consumer.on('message',function(message){
  console.time('parse_time');
  var columns = message.value.split(',');
  console.log(message.offset,':',message.partition)
  columns.map(function(d){
    var key = d.split('::')[0];
    var val = d.split('::')[1];
    if(key === 'all_pv') console.log('all_pv:',val);
    if(key === 'spark-time'){
       var epoctime = Long.MAX_VALUE.subtract(Long.fromString(val)).toNumber();
       console.log('spark-time:',(new Date(epoctime)))
   }
  });
  console.timeEnd('parse_time');
//  console.dir(columns)
});
console.log('start');
