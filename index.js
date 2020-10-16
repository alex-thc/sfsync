var jsforce = require('jsforce');
const tr = require('./transform');
const Realm = require('realm-web');
var assert = require('assert');
const moment = require('moment');

//TODO: need to trigger the full resync daily (for projects) to deal with joined fields we don't track
//or do a selective query somehow

const realmApp = new Realm.App({ id: "shadowforce-sivxs" });
const realmApiKey = "PDmuuqHPGyRF3eMVWsqle43rcDAaohEdwJIhFWtHivqJ86b3g4crnP8oienz7J7h"

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
	loginUrl : "https://test.salesforce.com",
	clientId : "3MVG9oZtFCVWuSwNs8.tqlbKN7l9ZxeM2bBHBoFKYJ_NMHtAIe16LjwOpq4sC.9QgwK5ti_SoVzoQ8l_K2VJ7",
	clientSecret : "D034CFCE536AB4B1A062C9BD3ABA95CBBEBD406734A7006C007B9E4DB76940CD",
})


oauth2.authenticate("psintegration@mongodb.com.stage","cFt67sp11mCiHSxdO3oGYUpxkz5f1XGl1").then((tokenResponse) => {
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
   		  // await loadMilestones(user,conn);
   		  // await loadSchedules(user,conn);
	  })()

	  user.mongoClient("mongodb-atlas").db("shf").collection("psproject").deleteMany({});
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

	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_project()} FROM pse__Proj__c WHERE ${cond_where} LIMIT 10`);
	  var done = false;
	  var fetched = 0;
	  while(! done) {
	  		done = result.done;
			fetched = fetched + result.records.length;
			console.log(`Fetched: ${fetched}/${result.totalSize}`);
			//console.log(result.records)
			console.log(tr.projects_transform(result.records))
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

	  var result = await sfQueryWrapper(conn, `SELECT ${tr.getSFFieldsString_schedule()} FROM pse__Est_Vs_Actuals__c WHERE ${cond_where} AND pse__Assignment__r.pse__Is_Billable__c = TRUE`);
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