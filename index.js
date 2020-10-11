var jsforce = require('jsforce');
const tr = require('./transform');
const Realm = require('realm-web');
var assert = require('assert');
const moment = require('moment');

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
  	  console.log("Successfully logged in to Realm!")

  	  //projects
  	  user.functions.getTimestamp("psproject").then(ts => {
  	  	  console.log("Project timestamp:",ts)
  	  	  var cond_where;
  	  	  if (ts) {
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

		  conn.query(`SELECT Id, Name, SystemModStamp, pse__Is_Active__c, pse__Stage__c, pse__End_Date__c FROM pse__Proj__c WHERE ${cond_where}`, function(err, result) {
			  if (err) { return console.error(err); }
			  console.log("total : " + result.totalSize);
			  console.log("fetched : " + result.records.length);
			  if (result.records.length > 0) {
				  let docs = tr.projects_transform(result.records)
				  user.functions.loadProjects(docs).then(res => {
				  	  console.log(res)
				  }).catch((error) => {
					  console.error("Failed to load projects into Realm", error);
				  });
			  }
		  });

	  }).catch((error) => {
		  console.error("Failed to get timestamp", error);
	  });
  }).catch((error) => {
	console.error("Failed to log into Realm", error);
  });

}).catch((error) => {
  console.error("Failed to log into Salesforce", error);
});