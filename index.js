var jsforce = require('jsforce');
const Realm = require('realm-web');
const loader = require('./loader');
var assert = require('assert');
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
   		  await loader.loadProjects(user,conn);
   		  await loader.loadMilestones(user,conn);
   		  await loader.loadSchedules(user,conn);
   		  await loader.loadOpportunities(user,conn);
   		  await loader.syncSchedules(user,conn);
	  })()

	  // user.mongoClient("mongodb-atlas").db("shf").collection("psproject").deleteMany({});
	  // user.mongoClient("mongodb-atlas").db("shf").collection("schedule").deleteMany({});
  }).catch((error) => {
	console.error("Failed to log into Realm", error);
  });

}).catch((error) => {
  console.error("Failed to log into Salesforce", error);
});