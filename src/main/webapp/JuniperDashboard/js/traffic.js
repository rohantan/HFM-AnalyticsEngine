var globalurl ="http://localhost:8080/analyticsengine/ae/";
var selectedInterface='';
$(document).ready(function () {
	var intervalId1,intervalId2,intervalId3,intervalId4;
	var url = globalurl+"devices";
	//alert(url);
	var i=0;
	
	function getDropPackets(interfaceName)
	{
		var url = globalurl+"interface/stats/traffic/totalpktdrop?interfaceName = "+interfaceName;
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


	function getInterfaceList(deviceName)
	{
		var url = globalurl+"device/interfaces?deviceName="+deviceName;
		alert(url);

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
});