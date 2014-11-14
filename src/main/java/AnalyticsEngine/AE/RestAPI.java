package AnalyticsEngine.AE;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

@Path("/")
public class RestAPI {

	@GET
	@Path("/startQueueServer")
	public String CheckService(){
		JavaQueueReceiver.startServer();
		return "Hi";
	}
	
	@GET
	@Path("/startTrafficServer")
	public String getTxPcktService(){
		JavaTrafficReceiver.startServer();
		return "Hi";
	}

	@GET
	@Path("/device")
	public String fetchDeviceInfo(@QueryParam ("deviceName") String deviceName) throws JSONException{
		return JavaQueueReceiver.getDeviceInfo(deviceName);
	}
	
	@GET
	@Path("/device/interface")
	public String fetchInterfaceInfo(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName = interfaceName.replaceAll("\\\\", "");
		return JavaQueueReceiver.getInterfaceInfo(interfaceName);
	}
	

	@GET
	@Path("/devices")
	public String fetchDevices() throws JSONException{
		return JavaQueueReceiver.getDevices();
	}
	
	@GET
	@Path("/device/interfaces")
	public String fetchInterfaces(@QueryParam ("deviceName") String deviceName) throws JSONException{
		return JavaQueueReceiver.getInterfaces(deviceName);
	}
	
	
	@GET
	@Path("/device/interface/stats/queue")
	public String fetchInterfaceQueueStats(@QueryParam ("deviceName") String deviceName, @QueryParam ("interfaceName") String interfaceName){
		interfaceName = interfaceName.replaceAll("\\\\", "");
		return JavaQueueReceiver.getInterfaceQueueStatsInfo(deviceName, interfaceName);
	}
	
	@GET
	@Path("/device/interface/stats/queue/chart1")
	public String fetchInterfaceQueueStatsChart1(@QueryParam ("deviceName") String deviceName, @QueryParam ("interfaceName") String interfaceName){
		interfaceName = interfaceName.replaceAll("\\\\", "");
		return JavaQueueReceiver.getInterfaceQueueStatsChart1(deviceName, interfaceName);
	}
	
	@GET
	@Path("/device/interface/stats/queue/chart2")
	public String fetchInterfaceQueueStatsChart2(@QueryParam ("deviceName") String deviceName, @QueryParam ("interfaceName") String interfaceName){
		interfaceName = interfaceName.replaceAll("\\\\", "");
		return JavaQueueReceiver.getInterfaceLatencyStatsChart1(deviceName, interfaceName);
	}
	
	@GET
	@Path("/interface/stats/traffic/pktdrop")
	public String fetchInterfaceTrafficStatPktDrop(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxDrpPckt", JavaTrafficReceiver.getInterfacePerTxDrpPcktHM(interfaceName));
		jo.put("interfaceRxDrpPckt", JavaTrafficReceiver.getInterfacePerRxDrpPcktHM(interfaceName));
		return jo.toString();
	}
	
	//TODO
	@GET
	@Path("/interface/stats/traffic/totalpktdrop")
	public String fetchInterfaceTrafficStatTotalPktDrop(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxTotalDrpPckt", JavaTrafficReceiver.getInterfacePerTxDrpPcktHM(interfaceName));
		jo.put("interfaceRxTotalDrpPckt", JavaTrafficReceiver.getInterfacePerRxDrpPcktHM(interfaceName));
		return jo.toString();
	}
	
	@GET
	@Path("/interface/stats/traffic/pps")
	public String fetchInterfaceTrafficStatsPps(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxPps", JavaTrafficReceiver.getInterfacePerTxPktPerSecHM(interfaceName));
		jo.put("interfaceRxPps", JavaTrafficReceiver.getInterfacePerRxPktPerSecHM(interfaceName));
		return jo.toString();
	}
	
	@GET
	@Path("/interface/stats/traffic/bps")
	public String fetchInterfaceTrafficStatsBps(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxBps", JavaTrafficReceiver.getInterfacePerTxBytPerSecHM(interfaceName));
		jo.put("interfaceRxBps", JavaTrafficReceiver.getInterfacePerRxBytPerSecHM(interfaceName));
		return jo.toString();
	}
	
	@GET
	@Path("/interface/stats/traffic/pckts")
	public String fetchInterfaceTrafficStatsPckts(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxPckts", JavaTrafficReceiver.getInterfacePerTxPktsHM(interfaceName));
		jo.put("interfaceRxPckts", JavaTrafficReceiver.getInterfacePerRxPktsHM(interfaceName));
		return jo.toString();
	}
	
	@GET
	@Path("/interface/stats/traffic/rxcrcerror")
	public String fetchInterfaceTrafficStatsRxCRCErr(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceRxCrcErr", JavaTrafficReceiver.getInterfacePerRxCrcErrHM(interfaceName));
		return jo.toString();
	}
	
	@GET
	@Path("/interface/stats/traffic/rxtotalcrcerror")
	public String fetchInterfaceTrafficStatsRxTotalCRCErr(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\\\", "");
		JSONObject jo = new JSONObject();
		jo.put("interfaceRxTotalCrcErr", JavaTrafficReceiver.getInterfacePerRxTotalCrcErrHM(interfaceName));
		return jo.toString();
	}
	
}

