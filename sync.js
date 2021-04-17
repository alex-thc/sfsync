const loader = require('./loader');
var jsforce = require('jsforce');

async function updateRequest(dbCollection,request,setter) {
	return dbCollection.updateOne({"_id" : request._id},
            	{"$set":setter});
}

async function getLatestFinishedRequest(dbCollection) {
	const query = { "status": "Finished" };
	const options = { sort: {"ts.created":-1} };

	return dbCollection.findOne(query, options);
}

async function getLatestFailedRequest(dbCollection) {
	const query = { "status": "Error" };
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

	if (request.type !== "manual_override" || request.proj === true)
	{
		await loader.loadProjects(realmUser,conn);
		if (request.type !== "manual_override")
			updateRequest(dbCollection,request,{"ts.projects":new Date()});
	}

	if (request.type !== "manual_override" || request.ms === true)
	{
		await loader.loadMilestones(realmUser,conn);
		if (request.type !== "manual_override")
			updateRequest(dbCollection,request,{"ts.milestones":new Date()});
	}

	if (request.type !== "manual_override" || request.opp === true)
	{
		await loader.loadOpportunities(realmUser,conn);
		if (request.type !== "manual_override")
			updateRequest(dbCollection,request,{"ts.opportunities":new Date()});
	}

	if (request.type !== "manual_override" || request.sched === true)
	{
		await loader.loadSchedules(realmUser,conn);
		if (request.type !== "manual_override")
			updateRequest(dbCollection,request,{"ts.schedules":new Date()});

		await loader.syncSchedules(realmUser,conn);
		if (request.type !== "manual_override")
			updateRequest(dbCollection,request,{"ts.schedules_sync":new Date()});
	}
}

module.exports = { 
	syncSFChanges, updateRequest,
	getLatestFinishedRequest, getLatestFailedRequest
	//, getLatestInProgressRequest
	}