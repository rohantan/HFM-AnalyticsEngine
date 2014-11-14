package AnalyticsEngine.AE;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
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
	@Path("/interface/stats/traffic")
	public String fetchInterfaceTrafficStats(@QueryParam ("interfaceName") String interfaceName) throws JSONException{
		interfaceName=interfaceName.replaceAll("\\", ""); //TODO look into this, this is incorrect!!
		System.out.println("@@@@@@@@@@@interfaceName@@@@@@@@@@@ "+interfaceName);
		JSONObject jo = new JSONObject();
		jo.put("interfaceTxDrpPckt", JavaTrafficReceiver.getInterfacePerTxDrpPcktHM(interfaceName));
		jo.put("interfaceRxDrpPckt", JavaTrafficReceiver.getInterfacePerTxDrpPcktHM(interfaceName));
		return jo.toString();
	}
}

