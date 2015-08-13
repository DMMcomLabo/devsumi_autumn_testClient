var kafka = require('kafka-node'),
Producer = kafka.HighLevelProducer,
client = new kafka.Client('172.16.100.180:2181,172.16.100.181:2181,172.16.100.182:2181'),
producer = new Producer(client);
var sample_json = {
    "i3_service_code": "i3_y8gx5b",
    "view_type": "pc",
    "open_id": "abc",
    "var1": "677",
    "url": "http://www.dmm.com/digital/tod/-/title/=/id=150306/",
    "action": "view",
    "option": "page",
    "view_cond": {
        "referrer": "http://www.dmm.com/"
    },
    "user_cond": {
        "browser_lang": "ja-JP",
        "ip": "0.0.0.0"
    },
    "api_cond": {
       "hit_count": 10,
       "page_number": 1,
       "limit": 30,
       "show_count": 10,
       "word": "けいおん",
       "category": "digital_tod",
       "sort": "rankprofile",
       "show_pattern": "package",
       "items": [
           {
               "content_id": "150306",
               "product_id": "tbs5782577",
               "price": 108,
               "has_stock": true
           },
           {
               "content_id": "150688",
               "product_id": "tbs235822276",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150698",
               "product_id": "tbs238222485",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150579",
               "product_id": "tbs5782589",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150689",
               "product_id": "tbs235922273",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150690",
               "product_id": "tbs236022274",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150691",
               "product_id": "tbs236122275",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150770",
               "product_id": "tbs242922915",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150769",
               "product_id": "tbs242822860",
               "price": 216,
               "has_stock": true
           },
           {
               "content_id": "150448",
               "product_id": "tbs213420770",
               "price": 216,
               "has_stock": true
           }
       ]
    },
    "user_agent": "curl/7.19.7 (x86_64-redhat-linux-gnu) libcurl/7.19.7 NSS/3.16.2.3 Basic ECC zlib/1.2.3 libidn/1.18 libssh2/1.4.2",
    "ip": "0.0.0.0",
    "date": "2015-08-07",
    "datetime": "2015-08-07 08:10:25"
};

producer.on('error',function(err){
  console.dir(err);
});
producer.on('ready',function(){
//producer.createTopics(['sample2'],false,function(err,data){
//	console.dir(err);
//	console.dir(data);
//});
var cnt = 0;
  setInterval(function(){
	var str = parse.bind(sample_json)();
        var i3_service_code = getI3ServiceCode();
        console.log(i3_service_code);
//	console.log('log:',str);
	producer.send([
	{topic:"raw_tracking",messages:[str]}
	],function(err,data){
	console.dir(err);
	console.dir(data);
	});
	cnt++;
	},1000);
});
var getStringVal = function(key,valueKey,prefix){
	var prefix = (prefix === undefined) && true;
	var valueKey = valueKey ? valueKey : key;
	return  this[valueKey] && (prefix ? "," : "") + key + ':' + this[valueKey] || "";
};
var crypto = require('crypto');
var getI3ServiceCode = function(){
	return 'i3_' + crypto.createHash('md5').update((Math.floor(Math.random()*100001)).toString(),'binary').digest('hex').substr(0,6);
};

var parse = function(){
	var obj = this;
	var v = getStringVal.bind(obj);
	var str = v('log_date','date',false);
console.log(str);
	str += v('action');
	str += v('option');
	str += v('i3_service_code');
	str += v('open_id');
	str += v('session');
	str += v('cookie');
	str += v('view_type');
	str += v('user_agent');
	str += v('ua_replace_id');
	str += v('url');
	str += v('url_replace_id');
	str += v('time','datetime');
	str += v('ip');
	str += v('segment');
	var v2 = getStringVal.bind(obj.view_cond);
	
	str += v2('referer');
	str += v2('referer_replace_id');
	str += v2('referer_page_id');
	str += v2('randing_flg');
	str += v2('randing_type');
	var v3 = getStringVal.bind(obj.api_cond);

	str += v3('hit_count');
	str += v3('page_count');
	str += v3('view_count','show_count');
	str += v3('view_condition');
//	str += ',content_ids:' + obj.view_cond.referer;
//	str += ',product_ids:' + obj.view_cond.referer;
//	str += ',uids:' + obj.view_cond.referer;
//	str += ',prices:' + obj.view_cond.referer;
//	str += ',stocks:' + obj.view_cond.referer;
	str += v3('api_type','type');
	str += v3('via_module','show_position');
	str += v3('parameter','parameters');
	str += v3('word');
	str += v3('category');
	str += v3('sort');
	str += v3('view_pattern','show_pattern'); 
	str += v3('re_search_flg','is_retried');
	str += v3('api_response_time');
	str += v3('client_response_time');

	var v4 = getStringVal.bind(obj.detail_cond);

	str += v4('content_id');
//	str += ',content_ids:' + obj.view_cond.referer;
//	str += ',product_ids:' + obj.view_cond.referer;
//	str += ',price:' + obj.view_cond.referer;
//	str += ',stock_flg:' + obj.view_cond.referer;
	str += v4('via_info');
	str += v4('via_info_options','via_option');
	str += v4('tran_view_order','via_view_rank');
	str += v4('via_module','via_show_position');
//	str += ',month_service_id:' + obj.view_cond.referer;
//	str += ',month_service_flg:' + obj.view_cond.referer;
//	str += ',month_service_end_date:' + obj.view_cond.referer;
//	str += ',month_service_price:' + obj.view_cond.referer;
	var v5 = getStringVal.bind(obj.user_cond);
	str += v5('browser_lang');
//	str += ',contry_code:' + obj.view_cond.referer;
//	str += ',region:' + obj.view_cond.referer;
//	str += ',city:' + obj.view_cond.referer;
//	str += ',latitude:' + obj.view_cond.referer;
//	str += ',longitude:' + obj.view_cond.referer;
	var v6 = getStringVal.bind(obj.purchase_cond);
	str += v6('purchase_id');
	str += v6('purchase_id_type');
//	str += ',sites:' + obj.view_cond.referer;
//	str += ',espdb_categorys:' + obj.view_cond.referer;
//	str += ',content_ids:' + obj.view_cond.referer;
//	str += ',product_ids:' + obj.view_cond.referer;
//	str += ',uids:' + obj.view_cond.referer;
//	str += ',prices:' + obj.view_cond.referer;
//	str += ',numbers:' + obj.view_cond.referer;
//	str += ',via_infos:' + obj.view_cond.referer;
//	str += ',via_info_options:' + obj.view_cond.referer;
//	str += ',subtotal:' + obj.view_cond.referer;
//	str += ',tax:' + obj.view_cond.referer;
//	str += ',toral:' + obj.view_cond.referer;
//	str += ',purchase_type:' + obj.view_cond.referer;
//	str += ',month_service_id:' + obj.view_cond.referer;
//	str += ',month_service_end_date:' + obj.view_cond.referer;
	return str;
};
//console.log(parse.bind(sample_json)().split(','));
