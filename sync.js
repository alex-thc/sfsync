const loader = require('./loader');
var jsforce = require('jsforce');

async function updateRequest(dbCollection,request,setter) {
	if (request.origin === "shell")
		return;

	return dbCollection.updateOne({"_id" : request._id},
            	{"$set":setter});
}

async function getLatestFinishedRequest(dbCollection) {
	const query = { "status": "Finished", type: null };
	const options = { sort: {"ts.created":-1} };

	return dbCollection.findOne(query, options);
}

async function getLatestFailedRequest(dbCollection) {
	const query = { "status": "Error", type: null };
	const options = { sort: {"ts.created":-1} };

	return dbCollection.findOne(query, options);
}

// async function getLatestInProgressRequest(dbCollection) {
// 	const query = { "status": "In Progress" };
// 	const options = { sort: {"ts.created":-1} };

// 	return dbCollection.findOne(query, options);
// }

async function syncSFChanges(oauth2, sfUser, sfPasswordWithKey, realmUser, dbCollection, request) {
	var tokenResponse = await oauth2.authenticate(sfUser,sfPasswordWithKey);
	console.log("Successfully logged in to Salesforce!");
	console.log(tokenResponse);

	var conn = new jsforce.Connection({
		instanceUrl : tokenResponse.instance_url,
		accessToken : tokenResponse.access_token
	});

	const resync = (request.resync === true);

	if (request.type !== "manual_override" || request.proj === true)
	{
		await loader.loadProjects(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.projects":new Date()});
	}

	if (request.type !== "manual_override" || request.ms === true)
	{
		await loader.loadMilestones(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.milestones":new Date()});
	}

	if (request.type !== "manual_override" || request.opp === true)
	{
		await loader.loadOpportunities(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.opportunities":new Date()});
	}

	if (request.type !== "manual_override" || request.attach === true)
	{
		await loader.loadGDocs(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.gdocs":new Date()});

		await loader.loadNotes(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.notes":new Date()});
	}

	if (request.type !== "manual_override" || request.cs === true)
	{
		await loader.loadCases(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.cases":new Date()});
	}

	if (request.type !== "manual_override" || request.sched === true)
	{
		await loader.loadSchedules(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.schedules":new Date()});

		await loader.syncSchedules(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.schedules_sync":new Date()});
	}

	if (request.type !== "manual_override" || request.tc === true)
	{
		await loader.loadTimecards(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.timecards":new Date()});

		await loader.syncTimecards(realmUser,conn, resync);
		updateRequest(dbCollection,request,{"ts.timecards_sync":new Date()});
	}
}

module.exports = { 
	syncSFChanges, updateRequest,
	getLatestFinishedRequest, getLatestFailedRequest
	//, getLatestInProgressRequest
	}