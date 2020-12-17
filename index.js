var jsforce = require('jsforce');
const tr = require('./transform');
const Realm = require('realm-web');
var assert = require('assert');
const moment = require('moment');
var CONFIG = require('./config-prod.json');

//TODO: need to trigger the full resync daily (for projects) to deal with joined fields we don't track
//or do a selective query somehow

const realmApp = new Realm.App({ id: CONFIG.realmAppId });
const realmApiKey = CONFIG.realmApiKey

async function loginApiKey(apiKey) {
  // Create an API Key credential
  const credentials = Realm.Credentials.apiKey(apiKey);
    // Authenticate the user
    const user = await realmApp.logIn(credentials);
    // `App.currentUser` updates to match the logged in user
    assert(user.id === realmApp.currentUser.id)
    return user
}

var oauth2 = new jsforce.OAuth2({
	loginUrl : CONFIG.sfLoginUrl,
	clientId : CONFIG.sfClientId,
	clientSecret : CONFIG.sfClientSecret,
})


oauth2.authenticate(CONFIG.sfUser,CONFIG.sfPasswordWithKey).then((tokenResponse) => {
  console.log("Successfully logged in to Salesforce!");
  console.log(tokenResponse);

  var conn = new jsforce.Connection({
  		instanceUrl : tokenResponse.instance_url,
  		accessToken : tokenResponse.access_token
	});

  loginApiKey(realmApiKey).then(user => {
  	  console.log("Successfully logged in to Realm!");

  	  (async () => {
   		  await loadProjects(user,conn);
   		  await loadMilestones(user,conn);
   		  await loadSchedules(user,conn);
	  })()

	  // user.mongoClient("mongodb-atlas").db("shf").collection("psproject").deleteMany({});
	  // user.mongoClient("mongodb-atlas").db("shf").collection("schedule").deleteMany({});
  }).catch((error) => {
	console.error("Failed to log into Realm", error);
  });

}).catch((error) => {
  console.error("Failed to log into Salesforce", error);
});

function sfQueryWrapper(conn, query) {
    return new Promise((resolve, reject) => {
        conn.query(query,function(err, result) {
        	if (err) reject(err);
            resolve(result);
        });
    });
}

function sfQueryMoreWrapper(conn, locator) {
    return new Promise((resolve, reject) => {
        conn.queryMore(locator,function(err, result) {
        	if (err) reject(err);
            resolve(result);
        });
    });
}

async function loadProjects(user,conn,resync=false) {
	  console.log("Loading projects...");
	  var ts = await user.functions.getTimestamp("psproject");
	  console.log("Project timestamp:",ts)
	  var cond_where;
	  if (ts && !resync) {
	  	let date = moment(ts).toISOString();
	  	cond_where = `SystemModstamp > ${date}`
	  } else {
	  	//we need to get anything that expires in this quarter or later
	  	//for simplicity, we'll just look 3 months back
	  	let date = moment().subtract(3, 'months').format('YYYY-MM-DD');
	  	//console.log(date)
	  	//return
	  	//cond_where = "pse__Stage__c IN ('In Progress','At Risk') OR (pse__Stage__c = 'Expired' AND )"
	  	cond_where = `pse__End_Date__c >= ${date}`
	  }

	  //only customer projects
	  cond_where = `(${cond_where} AND pse__Project_Type__c = 'Customer Project')`

	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_project()} FROM pse__Proj__c WHERE ${cond_where}`);
	  var done = false;
	  var fetched = 0;
	  while(! done) {
	  		done = result.done;
			fetched = fetched + result.records.length;
			console.log(`Fetched: ${fetched}/${result.totalSize}`);
			//console.log(result.records)
			//console.log(tr.projects_transform(result.records))
			if (result.records.length > 0) {
			  let docs = tr.projects_transform(result.records)
			  await user.functions.loadProjects(docs)
			}

	  		if (! result.done) {
	  		      result = await sfQueryMoreWrapper(conn, result.nextRecordsUrl);
	  		}
	}
	console.log("Done.");
}

async function loadMilestones(user,conn,resync=false) {
	  console.log("Loading milestones...");
	  var ts = await user.functions.getMilestoneTimestamp();
	  console.log("Milestone timestamp:",ts)
	  var cond_where;
	  if (ts && !resync && !resync) {
	  	let date = moment(ts).toISOString();
	  	cond_where = `SystemModstamp > ${date}`
	  } else {
	  	//we need to get anything that expires in this quarter or later
	  	//for simplicity, we'll just look 3 months back
	  	let date = moment().subtract(3, 'months').format('YYYY-MM-DD');
	  	//console.log(date)
	  	//return
	  	cond_where = `pse__Project__r.pse__End_Date__c >= ${date}`
	  }

	  //only customer projects
	  cond_where = `(${cond_where} AND pse__Project__r.pse__Project_Type__c = 'Customer Project')`

	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_milestone()} FROM pse__Milestone__c WHERE ${cond_where}`);
	  var done = false;
	  var fetched = 0;
	  while(! done) {
	  		done = result.done;
			fetched = fetched + result.records.length;
			console.log(`Fetched: ${fetched}/${result.totalSize}`);
			//console.log(result.records)
			//console.log(tr.milestones_transform(result.records))
			if (result.records.length > 0) {
			  let docs = tr.milestones_transform(result.records)
			  await user.functions.loadMilestones(docs)
			}

	  		if (! result.done) {
	  		      result = await sfQueryMoreWrapper(conn, result.nextRecordsUrl);
	  		}
	}
	console.log("Done.");
}

async function loadSchedules(user,conn,resync=false) {
	  console.log("Loading schedules...");
	  var ts = await user.functions.getTimestamp("schedule");
	  console.log("Schedule timestamp:",ts)
	  var cond_where;
	  if (ts && !resync) {
	  	let date = moment(ts).toISOString();
	  	cond_where = `SystemModstamp > ${date}`
	  } else {
	  	//we need to get anything that expires in this quarter or later
	  	//for simplicity, we'll just look 3 months back
	  	let date = moment().subtract(3, 'months').format('YYYY-MM-DD');
	  	//console.log(date)
	  	//return
	  	//cond_where = "pse__Stage__c IN ('In Progress','At Risk') OR (pse__Stage__c = 'Expired' AND )"
	  	cond_where = `pse__Assignment__r.pse__Project__r.pse__End_Date__c >= ${date} AND (pse__Estimated_Hours__c > 0 OR pse__Actual_Hours__c > 0)`
	  }

	  //only customer projects
	  cond_where = `(${cond_where} AND pse__Assignment__r.pse__Project__r.pse__Project_Type__c = 'Customer Project')`

	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_schedule()} FROM pse__Est_Vs_Actuals__c WHERE ${cond_where}`);
	  var done = false;
	  var fetched = 0;
	  while(! done) {
	  		done = result.done;
			fetched = fetched + result.records.length;
			console.log(`Fetched: ${fetched}/${result.totalSize}`);
			//console.log(result.records)
			//console.log(tr.schedules_transform(result.records))
			if (result.records.length > 0) {
			  let docs = tr.schedules_transform(result.records)
			  await user.functions.loadSchedules(docs)
			}

	  		if (! result.done) {
	  		      result = await sfQueryMoreWrapper(conn, result.nextRecordsUrl);
	  		}
	}
	console.log("Done.");
}

//hack function to sync future schedules since they may have been hard-deleted on the SF side
// async function syncSchedules(user,conn) {
// 	  console.log("Syncing schedules...");
// 	  var sunday = getSunday();

// 	  var dbCol = ...
// 	  var ids = dbCol.find({week:{$gte:sunday}},{_id:1}).toArray()
// 	  var ids_map = {}
// 	  for(let i in ids) ids_map[ids[i]._id] = 1;

// 	  for each 2000 in ids_map.getKeys() 
// 	  	generate where clause
// 	  	SF query 
// 	  	for each result in the query remove id from ids_map

// 	  dbCol.deleteMany({_id:{$in: ids_map.getKeys() }})


// 	  var ts = await user.functions.getTimestamp("schedule");
// 	  console.log("Schedule timestamp:",ts)
// 	  var cond_where;
// 	  if (ts && !resync) {
// 	  	let date = moment(ts).toISOString();
// 	  	cond_where = `SystemModstamp > ${date}`
// 	  } else {
// 	  	//we need to get anything that expires in this quarter or later
// 	  	//for simplicity, we'll just look 3 months back
// 	  	let date = moment().subtract(3, 'months').format('YYYY-MM-DD');
// 	  	//console.log(date)
// 	  	//return
// 	  	//cond_where = "pse__Stage__c IN ('In Progress','At Risk') OR (pse__Stage__c = 'Expired' AND )"
// 	  	cond_where = `pse__Assignment__r.pse__Project__r.pse__End_Date__c >= ${date} AND (pse__Estimated_Hours__c > 0 OR pse__Actual_Hours__c > 0)`
// 	  }

// 	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_schedule()} FROM pse__Est_Vs_Actuals__c WHERE ${cond_where}`);
// 	  var done = false;
// 	  var fetched = 0;
// 	  while(! done) {
// 	  		done = result.done;
// 			fetched = fetched + result.records.length;
// 			console.log(`Fetched: ${fetched}/${result.totalSize}`);
// 			//console.log(result.records)
// 			//console.log(tr.schedules_transform(result.records))
// 			if (result.records.length > 0) {
// 			  let docs = tr.schedules_transform(result.records)
// 			  await user.functions.loadSchedules(docs)
// 			}

// 	  		if (! result.done) {
// 	  		      result = await sfQueryMoreWrapper(conn, result.nextRecordsUrl);
// 	  		}
// 	}
// 	console.log("Done.");
// }