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
	@Path("/device/{deviceId}/interface/{interfaceId}/stats/queue")
	public String fetchInterfaceQueueStats(@PathParam ("deviceId") int deviceId, @PathParam ("interfaceId") int interfaceId) throws JSONException{
		JavaQueueReceiver.deviceMapping.get(deviceId);
		JavaQueueReceiver.interfaceMapping.get(interfaceId);
		JSONObject jo = new JSONObject();
		jo.put("deviceName", JavaQueueReceiver.deviceMapping.get(deviceId));
		jo.put("interfaceName", JavaQueueReceiver.interfaceMapping.get(interfaceId));
		return jo.toString();
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

