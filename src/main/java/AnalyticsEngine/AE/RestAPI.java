package AnalyticsEngine.AE;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

@Path("ae")
public class RestAPI {

@GET
	@Path("/test")
	public String CheckService(){
		
		return "Hi";
	}
/*
	@GET
	@Path("/check/{values}")
	public String CheckService(@PathParam ("values") String values) throws JSONException{
		System.out.println("values: "+values);
		JSONObject jobj=new JSONObject();
		jobj.put("email", "rohan.tan@gmail.com");
		return jobj.toString();
	}

	@POST
	@Path("/poll")
	public String addPoll(String createPollValues) throws Exception{

		JSONObject jsonCreatePollValues=new JSONObject(createPollValues);
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.createPoll(jsonCreatePollValues);

		return result;
	}

	@POST
	@Path("/poll/media")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public String addMediaToPoll(
			@FormDataParam("file") File fileobject,
			@FormDataParam("file") FormDataContentDisposition contentDispositionHeader,
			@QueryParam("pollid") String pollid) throws Exception{
		
		System.out.println("inside addMediaToPoll....");
		System.out.println("pollid: "+pollid);
		System.out.println("filename: "+contentDispositionHeader.getFileName());

		AWSS3BucketHandling awss3BucketHandling=new AWSS3BucketHandling();
		String result=awss3BucketHandling.addS3BucketObjects(fileobject, contentDispositionHeader, pollid);

		return result;
	}

	@DELETE
	@Path("/poll")
	public String deletePoll(String pollId) throws Exception{

		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.deletePoll(pollId);

		return result;
	}

	@GET
	@Path("poll/All/{user_name}")
	public String showAllPolls(@PathParam ("user_name") String user_name) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.showAllPolls(user_name);

		return result;
	}

	@GET
	@Path("poll/ById/{pollId}")
	public String showPollById(@PathParam ("pollId") String pollId) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.showPollByPollId(pollId);

		return result;
	}

	@GET
	@Path("poll/ByCategory/{category_name}/{user_name}")
	public String showPollsByCategory(@PathParam ("category_name") String category_name,@PathParam ("user_name") String user_name) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.showPollsByCategory(category_name,user_name);

		return result;
	}

	@GET
	@Path("poll/myPolls/{user_name}")
	public String showMyPolls(@PathParam ("user_name") String user_name) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.showMyPolls(user_name);

		return result;
	}


	@GET
	@Path("poll/pollsAssigned/{user_name}")
	public String showpollsAssigned(@PathParam ("user_name") String user_name) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.showAllPollsAssignedToMe(user_name);

		return result;
	}

	@POST
	@Path("/poll/myVote")
	public String voteOnPoll(String voteOnPollValues) throws Exception{

		JSONObject jsonVoteOnPollValues=new JSONObject(voteOnPollValues);
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.voteOnPoll(jsonVoteOnPollValues);

		return result;
	}

	@GET
	@Path("poll/voteResult/{pollId}")
	public String showVoteResults(@PathParam ("pollId") String pollId) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.getPollOptionCount(pollId);

		return result;
	}

	@GET
	@Path("poll/voteResultGeo/{pollId}")
	public String showVoteResultsGeo(@PathParam ("pollId") String pollId) throws Exception{
		IPollsDAO iPollsDAO=new PollsDAO();
		String result=iPollsDAO.getPollOptionCountGeo(pollId);

		return result;
	}

*/}
