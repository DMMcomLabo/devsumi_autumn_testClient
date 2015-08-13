var hbase = require('hbase');
var table = hbase({host:'172.16.100.180',port:20550})
.table('h_summary');
var base = null;
var view = null;
var csv = require('csv');
var fs = require('fs');
var iconv = require('iconv').Iconv;
var MAX_VALUE=9223372036854775
var rec_count = 0;

var create_base = function(r_key,rec,fn){
          var view_data = rec[20].split(';');
	  var api_data = rec[21].split(';');
	  var detail_data = rec[22].split(';');
	  var user_data = rec[23].split(';');
	  var purchase_data = rec[24].split(';');
if(view_data.length != 5){
	view_data = Array.apply(null,new Array(5)).map(String.prototype.valueOf,"");
}
if(api_data.length != 19){
	api_data = Array.apply(null,new Array(19)).map(String.prototype.valueOf,"");
}
if(detail_data.length != 13){
	detail_data = Array.apply(null,new Array(13)).map(String.prototype.valueOf,"");
}
if(user_data.length != 6){
	user_data = Array.apply(null,new Array(6)).map(String.prototype.valueOf,"");
}
if(purchase_data.length != 17){
	purchase_data = Array.apply(null,new Array(17)).map(String.prototype.valueOf,"");
}
	var row = base.row(r_key);
	
		row.put([
				'base:date',
				'base:action',
				'base:options',
				'base:i3_service_code',
				'base:open_id',
				'base:session',
				'base:cookie',
				'base:view_type',
				'base:user_agent',
				'base:ua_replace_id',
				'base:url',
				'base:url_replace_id',
				'base:time',
				'base:ip',
				'base:segment',
				'view:referer',
				'view:referer_replace_id',
				'view:referer_page_id',
				'view:randing_flg',
				'view:randing_type',
				'api:hit_count',
				'api:page_count',
				'api:view_count',
				'api:view_condition',
				'api:content_ids',
				'api:product_ids',
				'api:uids',
				'api:prices',
				'api:stocks',
				'api:api_type',
				'api:via_module',
				'api:parameter',
				'api:word',
				'api:category',
				'api:sort',
				'api:view_pattern',
				'api:re_search_flg',
				'api:api_response_time',
				'api:client_response_time',
				'detail:content_id',
				'detail:content_ids',
				'detail:product_ids',
				'detail:price',
				'detail:stock_flg',
				'detail:via_info',
				'detail:via_info_options',
				'detail:tran_view_order',
				'detail:via_module',
				'detail:month_service_id',
				'detail:month_service_flg',
				'detail:month_service_end_date',
				'detail:month_service_price',
				'user:browser_lang',
				'user:country_code',
				'user:region',
				'user:city',
				'user:latitude',
				'user:longitude',
				'purchase:purchase_id',
				'purchase:purchase_id_type',
				'purchase:sites',
				'purchase:espdb_categorys',
				'purchase:content_ids',
				'purchase:product_ids',
				'purchase:uids',
				'purchase:prices',
				'purchase:numbers',
				'purchase:via_infos',
				'purchase:via_info_options',
				'purchase:subtotal',
				'purchase:tax',
				'purchase:total',
				'purchase:purchase_type',
				'purchase:month_service_id',
				'purchase:month_service_end_date',
			],[
			rec[0],
			rec[1],
			rec[2],
			rec[3],
			rec[4],
			rec[5],
			rec[6],
			rec[7],
			rec[8],
			rec[9],
			rec[10],
			rec[11],
			rec[12],
			rec[13],
			rec[14],
			view_data[0],
			view_data[1],
			view_data[2],
			view_data[3],
			view_data[4],
			api_data[0],
			api_data[1],
			api_data[2],
			api_data[3],
			api_data[4],
			api_data[5],
			api_data[6],
			api_data[7],
			api_data[8],
			api_data[9],
			api_data[10],
			api_data[11],
			api_data[12],
			api_data[13],
			api_data[14],
			api_data[15],
			api_data[16],
			api_data[17],
			api_data[18],
			detail_data[0],
			detail_data[1],
			detail_data[2],
			detail_data[3],
			detail_data[4],
			detail_data[5],
			detail_data[6],
			detail_data[7],
			detail_data[8],
			detail_data[9],
			detail_data[10],
			detail_data[11],
			detail_data[12],
			user_data[0],
			user_data[1],
			user_data[2],
			user_data[3],
			user_data[4],
			user_data[5],
			purchase_data[0],
			purchase_data[1],
			purchase_data[2],
			purchase_data[3],
			purchase_data[4],
			purchase_data[5],
			purchase_data[6],
			purchase_data[7],
			purchase_data[8],
			purchase_data[9],
			purchase_data[10],
			purchase_data[11],
			purchase_data[12],
			purchase_data[13],
			purchase_data[14],
			purchase_data[15],
			purchase_data[16],
			],function(err,success){
		    if(err){
				console.dir(err);
			 return fn(false);
		    }
//			if((rec_count % 10000) === 0) console.log('------------rec_count:',rec_count);
			fn();
	        })
};
var create_view = function(r_key,view_data,fn){
if(view_data.length != 5){
console.dir(view_data);
return fn(false);
}
	var row = view.row(r_key);
	
		row.put([
				'view:referer',
				'view:referer_replace_id',
				'view:referer_page_id',
				'view:randing_flg',
				'view:randing_type',
			],[
			view_data[0],
			view_data[1],
			view_data[2],
			view_data[3],
			view_data[4],
			],function(err,success){
		    if(err){
				//console.dir(err);
			 return fn(false);
		    }
			fn();
	        })
};
var count = 0;
var t = csv.transform(function(rec,cb){
	count += 1;
//	setTimeout(function(){
          var milliseconds = (new Date).getTime();
	  var r_key = rec[3] + '_' + (MAX_VALUE-milliseconds);
		create_base(r_key,rec,function(){});
//		create_view(r_key,view_data,function(){});
			cb(null,count);
		if((count%1000)===0) console.log(count);

//	},50);
},{paralel:100});
//table.create('base',function(err,success){
//	console.dir(err);
//        base = this;
  table.create({ColumnSchema:[{name:'view'},{name:'base'},{name:'api'},{name:'detail'},{name:'user'},{name:'purchase'}]},function(err,success){
	console.dir(err);
        base = this;
        fs.createReadStream('./test.log').on('end',function(err){console.log('read end');}).pipe(csv.parse({delimiter:"\t"})).on('end',function(err){console.log('parse end');}).pipe(t).on('end',function(){console.log('end?');});
  });
//});
