var globalurl ="http://cmpe239instance.cloudapp.net:8080/analyticsengine/ae/";
var selectedInterface='';
$(document).ready(function () {
	var intervalId1,intervalId2,intervalId3,intervalId4,intervalId5,intervalId,intervalId7,intervalId8;
	var url = globalurl+"devices";
	//alert(url);totalpktdrop
	var i=0;

	function getDropPackets(interfaceName)
	{
		var url = globalurl+"interface/stats/traffic/totalpktdrop?interfaceName="+interfaceName;
		//alert("In getDropPackets"+url);
		intervalId1 = setInterval(function(){
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					//alert(msg);
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					//alert(obj.interfaceTxTotalDrpPckt[interfaceName]);
					$("#txndroppkt").html(obj.interfaceTxTotalDrpPckt[interfaceName]);
					$("#rxndroppkt").html(obj.interfaceRxTotalDrpPckt[interfaceName]);
				},
				error: function () {
					alert("Error");
				}
			});}, 10000);

	}

	function getRxCRCError(interfaceName)
	{
		var url = globalurl+"interface/stats/traffic/rxcrcerror?interfaceName="+interfaceName;
		//alert("In getDropPackets"+url);
		intervalId2 = setInterval(function(){
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					//alert(msg);
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					//alert(obj.interfaceTxTotalDrpPckt[interfaceName]);
					$("#rxcrc").html(obj.interfaceRxCrcErr[interfaceName]);
				},
				error: function () {
					alert("Error");
				}
			});}, 10000);

	}

	function getTotalRxCRCError(interfaceName)
	{
		var url = globalurl+"interface/stats/traffic/rxtotalcrcerror?interfaceName="+interfaceName;
		//alert(url);
		intervalId3 = setInterval(function(){
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					//alert(msg);
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					//alert(obj.interfaceTxTotalDrpPckt[interfaceName]);
					$("#totalrxcrc").html(obj.interfaceRxTotalCrcErr[interfaceName]);
				},
				error: function () {
					alert("Error");
				}
			});}, 10000);

	}

	// plot Stack graphs
	function generateBoxCharts(interfaceName)
	{
		intervalId4 = setInterval(function(){
			url=globalurl+"interface/stats/traffic/pckts?interfaceName="+interfaceName;
			//alert(url);
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					$("#txpkt").html(obj.interfaceTxPckts[interfaceName]);
					$("#rxpkt").html(obj.interfaceRxPckts[interfaceName]);
				},
				error: function () {
					alert("Error");
				}

			});

			url=globalurl+"interface/stats/traffic/bps?interfaceName="+interfaceName;
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					$("#txbps").html(obj.interfaceTxBps[interfaceName]);
					$("#rxbps").html(obj.interfaceRxBps[interfaceName]);

				},
				error: function () {
					alert("Error");
				}

			});

			url=globalurl+"interface/stats/traffic/pps?interfaceName="+interfaceName;
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					$("#txpps").html(obj.interfaceTxPps[interfaceName]);
					$("#rxpps").html(obj.interfaceRxPps[interfaceName]);
				},
				error: function () {
					alert("Error");
				}

			});

			url=globalurl+"interface/stats/traffic/pktdrop?interfaceName="+interfaceName;
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					$("#txdroppkt").html(obj.interfaceTxDrpPckt[interfaceName]);
					$("#rxdroppkt").html(obj.interfaceRxDrpPckt[interfaceName]);
				},
				error: function () {
					alert("Error");
				}

			});}, 1000);
	}	

	var txDropData = [], ttlPoints = 300;
	var rxDropData = [];
	function generateTxDropChart(interfaceName)
	{

		//alert("In generateTxDropChart");
		function getTxDropData() {
			var url = globalurl+"interface/stats/traffic/graphpktdrop?interfaceName="+interfaceName;
			//alert("In generateTxDropChart"+url);
			$.ajax({
				type: "GET",
				url: url,
				async : false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					txDropData = obj.interfaceTxDrpPckt;
					rxDropData = obj.interfaceRxDrpPckt;

				},
				error: function () {
					alert("Error");
				}
			});

			var result = [];

			for (var i = 0; i < txDropData.length; ++i)
				result.push([i, txDropData[i]])
				return result;

		}
		if($("#txDropPcktChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 20000000, max: 70000000 },
					xaxis: { show: false },
					colors: ["#B87FED"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#txDropPcktChart"), [ getTxDropData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getTxDropData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();
				setTimeout(update, updateIntervalTxDropPckt);
			}

			update();
		}
	}

	var updateIntervalTxDropPckt = 3000;
	$("#updateIntervalTxDropPckt").val(updateIntervalTxDropPckt).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateIntervalTxDropPckt = +v;
			if (updateIntervalTxDropPckt < 1)
				updateIntervalTxDropPckt = 1;
			if (updateIntervalTxDropPckt > 5000)
				updateIntervalTxDropPckt = 5000;
			$(this).val("" + updateIntervalTxDropPckt);
		}

	});


	function generateRxDropChart(interfaceName)
	{

		//alert("generateLatencyChart");

		function getRxDropData() {

			var result = [];


			for (var i = 0; i < rxDropData.length; ++i)
				result.push([i, rxDropData[i]])
				return result;
		}


		if($("#rxDropPcktChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 2000000, max: 7000000 },
					xaxis: { show: false },
					colors: ["#F4A506"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#rxDropPcktChart"), [ getRxDropData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getRxDropData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();
				setTimeout(update, updateIntervalRxDropPckt);
			}

			update();
		}
	}

	var updateIntervalRxDropPckt = 3000;
	$("#updateIntervalRxDropPckt").val(updateIntervalRxDropPckt).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateIntervalRxDropPckt = +v;
			if (updateIntervalRxDropPckt < 1)
				updateIntervalRxDropPckt = 1;
			if (updateIntervalRxDropPckt > 5000)
				updateIntervalRxDropPckt = 5000;
			$(this).val("" + updateIntervalRxDropPckt);
		}

	});



	var txData = [];
	var rxData = [];
	function generateTxChart(interfaceName)
	{


		function getTxData() {
			var url = globalurl+"interface/stats/traffic/graphpckts?interfaceName="+interfaceName;
			$.ajax({
				type: "GET",
				url: url,
				async : false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					txData = obj.interfaceTxPckts;
					rxData = obj.interfaceRxPckts;

				},
				error: function () {
					alert("Error");
				}
			});

			var result = [];

			for (var i = 0; i < txData.length; ++i)
				result.push([i, txData[i]])
				return result;
		}

		if($("#txPacketsChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 100000, max: 6000000 },
					xaxis: { show: false },
					colors: ["#B87FED"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#txPacketsChart"), [ getTxData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getTxData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();
				setTimeout(update, updateIntervalTxPackets);
			}

			update();
		}
	}

	var updateIntervalTxPackets = 3000;
	$("#updateIntervalTxPackets").val(updateIntervalTxPackets).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateIntervalTxPackets = +v;
			if (updateIntervalTxPackets < 1)
				updateIntervalTxPackets = 1;
			if (updateIntervalTxPackets > 5000)
				updateIntervalTxPackets = 5000;
			$(this).val("" + updateIntervalTxPackets);
		}

	});


	function generateRxChart(interfaceName)
	{

		//alert("generateLatencyChart");

		function getRxData() {

			var result = [];


			for (var i = 0; i < rxData.length; ++i)
				result.push([i, rxData[i]])
			//console.log(result);
				return result;
		}


		if($("#rxPacketsChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 10000000, max: 800000000 },
					xaxis: { show: false },
					colors: ["#F4A506"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#rxPacketsChart"), [ getRxData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getRxData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();
				setTimeout(update, updateIntervalRxPackets);
			}

			update();
		}
	}

	var updateIntervalRxPackets = 3000;
	$("#updateIntervalRxDropPckt").val(updateIntervalRxPackets).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateIntervalRxPackets = +v;
			if (updateIntervalRxPackets < 1)
				updateIntervalRxPackets = 1;
			if (updateIntervalRxPackets > 5000)
				updateIntervalRxPackets = 5000;
			$(this).val("" + updateIntervalRxPackets);
		}

	});



	function getInterfaceList(deviceName)
	{
		var url = globalurl+"device/interfaces?deviceName="+deviceName;
		//alert(url);

		$.ajax({
			type: "GET",
			url: url,
			async:false,
			success: function(msg){
				//alert(msg);
				var obj = jQuery.parseJSON( ''+ msg +'' );
				var html= "";
				$('#selectInterface').empty();
				var options = '<option value="">Select</option>';
				selectedInterface = obj.interfaces[0];
				//alert("Selected Interface"+selectedInterface);
				for(var i=0;i < obj.interfaces.length;i++) {
					options+='<option value="'+obj.interfaces[i]+'">'+obj.interfaces[i]+'</option>';
				}
				$(options).appendTo($("#selectInterface"));
				$('#selectInterface').bind('change', function()
						{ 
					clearInterval(intervalId1);
					clearInterval(intervalId2);
					clearInterval(intervalId3);
					clearInterval(intervalId4);
					clearInterval(intervalId5);
					$("#txndroppkt").empty();
					$("#rxndroppkt").empty();
					$("#rxcrc").empty();
					$("#totalrxcrc").empty();

					$("#txpkt").empty();
					$("#rxpkt").empty();
					$("#txbps").empty();
					$("#rxbps").empty();

					$("#txpps").empty();
					$("#rxpps").empty();
					$("#txdroppkt").empty();
					$("#rxdroppkt").empty();
					selectedInterface = $(this).val();
					//alert(selectedInterface);
					getInterfaceInfo(deviceName,$(this).val());
					getDropPackets($(this).val());
					getRxCRCError($(this).val());
					getTotalRxCRCError($(this).val());
					generateBoxCharts($(this).val());
					generateTxDropChart($(this).val());
					generateRxDropChart($(this).val());
					generateTxChart($(this).val());
					generateRxChart($(this).val());
						});

			},

			error: function () {
				alert("Error");
			}
		});
	}

	$.ajax({
		type: "GET",
		url: url,
		success: function(msg){

			var obj = jQuery.parseJSON( ''+ msg +'' );
			var html= "";

			$('#interfaceTable tbody').empty();
			var newContent = '<tr><td><div class="control-group"><div class="controls"><select id="selectDevice" data-rel="chosen">';
			var options = '<option value="">Select</option>';
			//alert("Selected Interface"+selectedInterface);
			for(var i=0;i < obj.devices.length;i++) {
				options+='<option value="'+obj.devices[i]+'">'+obj.devices[i]+'</option>';
			}
			newContent += options+'</select></div></div></td><td><div class="control-group"><div class="controls"><select id="selectInterface"><option>Select Interface</option></select></div></div></td></tr>';
			$(newContent).appendTo($("#interfaceTable"));
			$('#selectDevice').bind('change', function()
					{ 

				//alert(selectedInterface);
				getDeviceInformation($(this).val());
				getInterfaceList($(this).val());
				//generateCurrentQDepthChart(deviceName,$(this).val());

					});


		},
		error: function () {
			alert("Error");
		}
	});



//	get device information
	function getDeviceInformation(deviceNum)
	{
		var url = globalurl+"device?deviceName="+deviceNum;
		//alert(url);
		$.ajax({
			type: "GET",
			url: url,
			async:false,
			success: function(msg){
				var obj = jQuery.parseJSON( ''+ msg +'' );
				//alert("In getDeviceInfo");
				$('#deviceInfo tbody').empty();
				var newContent = '<tr><th>Name</th><td>'+deviceNum+'</td></tr>';
				newContent += '<tr><th>Boot Time</th><td>'+obj.information.boot_time+'</td></tr>';
				newContent += '<tr><th>Model Info</th><td>'+obj.information.model_info+'</td></tr>';
				newContent += '<tr><th>Max Ports</th><td>'+obj.information.max_ports+'</td></tr>';
				newContent += '<tr><th>Queue Status : Status</th><td>'+obj.status.queue_status.status+'</td></tr>';
				newContent += '<tr><th>Queue Status : Poll Interval</th><td>'+obj.status.queue_status.poll_interval+'</td></tr>';
				newContent += '<tr><th>Queue Status : Lt high</th><td>'+obj.status.queue_status.lt_high+'</td></tr>';
				newContent += '<tr><th>Queue Status : Lt Low</th><td>'+obj.status.queue_status.lt_low+'</td></tr>';
				newContent += '<tr><th>Traffic Status : Status</th><td>'+obj.status.traffic_status.status+'</td></tr>';
				newContent += '<tr><th>Traffic Status : Poll Interval</th><td>'+obj.status.traffic_status.poll_interval+'</td></tr>';
				//alert(newContent);
				$(newContent).appendTo($("#deviceInfo"));

			},
			error: function () {
				alert("Error");
			}

		});
	}

//	get interface information
	function getInterfaceInfo(deviceNum,interfaceNum)
	{
		var url = globalurl+"device/interface?deviceName="+deviceNum+"&interfaceName="+interfaceNum;
		//alert(url);
		$.ajax({
			type: "GET",
			url: url,
			async:false,
			success: function(msg){
				var obj = jQuery.parseJSON( ''+ msg +'' );
				//alert("In getInterfaceInfo");
				$('#interfaceInfo tbody').empty();
				var newContent = '<tr><th>Name</th><td>'+interfaceNum+'</td></tr>';
				newContent += '<tr><th>SNMP Index</th><td>'+obj.information.snmp_index+'</td></tr>';
				newContent += '<tr><th>Index</th><td>'+obj.information.index+'</td></tr>';
				newContent += '<tr><th>Slot</th><td>'+obj.information.slot+'</td></tr>';
				newContent += '<tr><th>Port</th><td>'+obj.information.port+'</td></tr>';
				newContent += '<tr><th>Media Type</th><td>'+obj.information.media_type+'</td></tr>';
				newContent += '<tr><th>Capability : Lt high</th><td>'+obj.information.capability+'</td></tr>';
				newContent += '<tr><th>Port Type</th><td>'+obj.information.porttype+'</td></tr>';
				newContent += '<tr><th>Link : Speed</th><td>'+obj.status.link.speed+'</td></tr>';
				newContent += '<tr><th>Link : Duplex</th><td>'+obj.status.link.duplex+'</td></tr>';
				newContent += '<tr><th>Link : MTU</th><td>'+obj.status.link.mtu+'</td></tr>';
				newContent += '<tr><th>Link : State</th><td>'+obj.status.link.state+'</td></tr>';
				newContent += '<tr><th>Link : Auto Negotiation</th><td>'+obj.status.link.auto_negotiation+'</td></tr>';
				//alert(newContent);
				$(newContent).appendTo($("#interfaceInfo"));

			},
			error: function () {
				alert("Error");
			}

		});
	}

});
