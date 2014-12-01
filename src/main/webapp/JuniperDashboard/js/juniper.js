var globalurl ="http://cmpe239instance.cloudapp.net:8080/analyticsengine/ae/";
var selectedInterface='';
$(document).ready(function () {
	var intervalId;
	var url = globalurl+"devices";
	//alert(url);
	var i=0;
	$.ajax({
		type: "GET",
		url: url,
		success: function(msg){
			//alert(msg);
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
				getTopInterfaceStats($(this).val());
				//generateCurrentQDepthChart(deviceName,$(this).val());

					});



			//getInterfaceList(obj.devices[0]);
			//getDeviceInformation(obj.devices[0]);


			//getInterfaceInfo(obj.devices[0],selectedInterface);
			//getQueueStats(obj.devices[0],selectedInterface);
		},
		error: function () {
			alert("Error");
		}
	});

	function getTopInterfaceStats(deviceName) {
		var url = globalurl+"device/stats/queue/topInterfaces?deviceName="+deviceName;
		//alert(url);
		$.ajax({
			type: "GET",
			url: url,
			async:false,
			success: function(msg){
				var obj = jQuery.parseJSON( ''+ msg +'' );
				$('#intNo1').html(obj.topInterfaces[0]);
				$('#intNo2').html(obj.topInterfaces[1]);
				$('#intNo3').html(obj.topInterfaces[2]);
			}
		});
				
			
	}

	//get device information
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

	//get interface information
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

	var data = [], totalPoints = 300;
	function generateCurrentQDepthChart(deviceName,interfaceName)
	{
		// be fetched from a server
		//alert("generateCurrentQDepthChart");

		function getQDepthData() {
			var url = globalurl+"device/interface/stats/queue/chart1?deviceName="+deviceName+"&interfaceName="+interfaceName;
			//alert("In getQueueStats"+url);

			$.ajax({
				type: "GET",
				url: url,
				async : false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					data = obj.queueStats;
					generateBarChart(data,obj.queueAvgStats);

				},
				error: function () {
					alert("Error");
				}
			});
			var res = [];

			for (var i = 0; i < data.length; ++i)
				res.push([i, data[i]])
				return res;

		}


		if($("#qDepthChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 8000, max: 25000 },
					xaxis: { show: false },
					colors: ["#F4A506"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#qDepthChart"), [ getQDepthData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getQDepthData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();

				setTimeout(update, updateInterval);
			}

			update();
		}
	}

	//populate inerface list
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
					clearInterval(intervalId);
					$("#qmax").empty();
					$("#qmin").empty();
					$("#qavg").empty();
					$("#qcurr").empty();
					selectedInterface = $(this).val();
					//alert(selectedInterface);
					getInterfaceInfo(deviceName,$(this).val());
					getQueueStats(deviceName,$(this).val());
					generateCurrentQDepthChart(deviceName,$(this).val());
					generateLatencyChart(deviceName,$(this).val());

						});


			},

			error: function () {
				alert("Error");
			}
		});
	}


	function getQueueStats(deviceName,interfaceName)
	{
		var url = globalurl+"device/interface/stats/queue?deviceName="+deviceName+"&interfaceName="+interfaceName;
		//alert("In getQueueStats"+url);
		intervalId = setInterval(function(){
			$.ajax({
				type: "GET",
				url: url,
				async:false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					$("#qmax").html(obj.Maximum);
					$("#qmin").html(obj.Minimum);
					$("#qavg").html(obj.Average);
					$("#qcurr").html(obj.Current);

				},
				error: function () {
					alert("Error");
				}
			});}, 1000);

	}



	var latencyData= [], ttlPoints = 300;

	function generateLatencyChart(deviceName,interfaceName)
	{

		//alert("generateLatencyChart");

		function getLatencyData() {
			var url = globalurl+"device/interface/stats/queue/chart2?deviceName="+deviceName+"&interfaceName="+interfaceName;
			//alert("In generateLatencyChart"+url);
			$.ajax({
				type: "GET",
				url: url,
				async : false,
				success: function(msg){
					var obj = jQuery.parseJSON( ''+ msg +'' );
					var html= "";
					latencyData = obj.queueStats;
				},
				error: function () {
					alert("Error");
				}
			});
			var result = [];

			for (var i = 0; i < latencyData.length; ++i)
				result.push([i, latencyData[i]])
				return result;
		}


		if($("#latencyChart").length)
		{
			var options = {
					series: { shadowSize: 1 },
					lines: { fill: true, fillColor: { colors: [ { opacity: 1 }, { opacity: 0.1 } ] }},
					yaxis: { min: 0, max: 20 },
					xaxis: { show: false },
					colors: ["#B87FED"],
					grid: {	tickColor: "#dddddd",
						borderWidth: 0 
					},
			};
			var plot = $.plot($("#latencyChart"), [ getLatencyData() ], options);
			function update() {
				//alert("In Update");
				plot.setData([ getLatencyData() ]);
				// since the axes don't change, we don't need to call plot.setupGrid()
				plot.draw();
				setTimeout(update, updateIntervalLatency);
			}

			update();
		}
	}

	// bar chart
	
	function generateBarChart(current1,average1)
	{
		var average=[];
		var current=[];
		if($("#stats-chart2").length)
		{	
			$("#stats-chart2").empty();
			for (var i = 0; i < current1.length; i++)
			{
				//current.push([i, current1[i]])
				average.push([i, average1[i]]);
			}
			
			var plot = $.plot($("#stats-chart2"),
					[ { data: average, 
						label: "Average", 
						lines: { show: true, 
							fill: false,
							lineWidth: 2 
						},
						shadowSize: 0	
					}/*, {
						data: current,
						label: "Current",
						bars: { show: true,
							fill: false, 
							barWidth: 0.1, 
							align: "center",
							lineWidth: 1,
						}
					}*/
					], {

				grid: { hoverable: true, 
					clickable: true, 
					tickColor: "rgba(255,255,255,0.05)",
					borderWidth: 0
				},
				legend: {
					show: false
				},	
				colors: ["rgba(255,255,255,0.8)", "rgba(255,255,255,0.6)", "rgba(255,255,255,0.4)", "rgba(255,255,255,0.2)"],
				xaxis: {ticks:15, tickDecimals: 0, color: "rgba(255,255,255,0.8)" },
				yaxis: {ticks:5, tickDecimals: 0, color: "rgba(255,255,255,0.8)" },
			});

			/*
			   [ { data: visitors, label: "Visits"}], {
				   series: {
					   lines: { show: true,
								lineWidth: 2
							 },
					   points: { show: true, 
								 lineWidth: 2 
							 },
					   shadowSize: 0
				   },	
				   grid: { hoverable: true, 
						   clickable: true, 
						   tickColor: "rgba(255,255,255,0.025)",
						   borderWidth: 0
						 },
				 legend: {
						    show: false
						},	
				   colors: ["rgba(255,255,255,0.8)", "rgba(255,255,255,0.6)", "rgba(255,255,255,0.4)", "rgba(255,255,255,0.2)"],
					xaxis: {ticks:15, tickDecimals: 0},
					yaxis: {ticks:5, tickDecimals: 0},
				 });
			 */		



			/*function showTooltip(x, y, contents) {
				$('<div id="tooltip">' + contents + '</div>').css( {
					position: 'absolute',
					display: 'none',
					top: y + 5,
					left: x + 5,
					border: '1px solid #fdd',
					padding: '2px',
					'background-color': '#dfeffc',
					opacity: 0.80
				}).appendTo("body").fadeIn(200);
			}*/

			/*var previousPoint = null;
			$("#stats-chart2").bind("plothover", function (event, pos, item) {
				$("#x").text(pos.x.toFixed(2));
				$("#y").text(pos.y.toFixed(2));

					if (item) {
						if (previousPoint != item.dataIndex) {
							previousPoint = item.dataIndex;

							$("#tooltip").remove();
							var x = item.datapoint[0].toFixed(2),
								y = item.datapoint[1].toFixed(2);

							showTooltip(item.pageX, item.pageY,
										item.series.label + " of " + x + " = " + y);
						}
					}
					else {
						$("#tooltip").remove();
						previousPoint = null;
					}
			});*/

		}
	}


	var updateIntervalLatency = 3000;
	$("#updateIntervalLatency").val(updateIntervalLatency).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateIntervalLatency = +v;
			if (updateIntervalLatency < 1)
				updateIntervalLatency = 1;
			if (updateIntervalLatency > 5000)
				updateIntervalLatency = 5000;
			$(this).val("" + updateIntervalLatency);
		}

	});

	// setup control widget
	var updateInterval = 3000;
	$("#updateInterval").val(updateInterval).change(function () {
		var v = $(this).val();
		if (v && !isNaN(+v)) {
			updateInterval = +v;
			if (updateInterval < 1)
				updateInterval = 1;
			if (updateInterval > 5000)
				updateInterval = 5000;
			$(this).val("" + updateInterval);
		}

	});
	
	
});


