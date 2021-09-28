var jsforce = require('jsforce');
const Realm = require('realm-web');
const sync = require('./sync');
var assert = require('assert');
var CONFIG = require('./config-prod.json');
const yargs = require('yargs');

const argv = yargs
    .command('sync', 'Trigger manual sync', {
        proj: {
            alias: 'p',
            description: 'Projects',
            type: 'bool',
        },
        ms: {
            alias: 'm',
            description: 'Milestones',
            type: 'bool',
        },
        opp: {
            alias: 'o',
            description: 'Opportunities',
            type: 'bool',
        },
        sched: {
            alias: 's',
            description: 'Schedules',
            type: 'bool',
        },
        tc: {
            alias: 't',
            description: 'Timecards',
            type: 'bool',
        },
        attach: {
            alias: 'a',
            description: 'Attachments',
            type: 'bool',
        },
        cs: {
            alias: 'c',
            description: 'Cases',
            type: 'bool',
        },
        resync: {
            alias: 'x',
            description: 'Resync',
            type: 'bool',
        }
    })
    .help()
    .alias('help', 'h')
    .argv;

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

async function request_should_ignore(dbCollection,request) {
  //check if we should be ignoring this one
  //last success within an hour
  let last_success = await sync.getLatestFinishedRequest(dbCollection);
  if (last_success && last_success.ts.finished
  	&& ( (request.ts.created <= last_success.ts.finished)
  	     || ((request.ts.created - last_success.ts.finished) < CONFIG.syncThrottleSuccess_seconds*1000 ))) {
  	console.log("Found a recent success", last_success.ts.finished);
  	return true;
  }

  let last_fail = await sync.getLatestFailedRequest(dbCollection);
  if (last_fail && last_fail.ts.finished
  	&& ( (request.ts.created <= last_fail.ts.finished)
  	     || ((request.ts.created - last_fail.ts.finished) < CONFIG.syncThrottleFailure_seconds*1000 ))) {
  	console.log("Found a recent failure", last_fail.ts.finished);
  	return true;
  }

  // let last_active = await sync.getLatestInProgressRequest(dbCollection);
  // if (last_active 
  // 	&& ( (request.ts.created <= last_active.ts.created)
  // 	     || ((request.ts.created - last_active.ts.created) < CONFIG.syncInProgressTimeout_seconds*1000 ))) {
  // 	console.log("Found a recent active sync", last_active.ts.created);
  // 	return true;
  // }

  return false;
}

async function process_request(realmUser,dbCollection,request) {
  console.log(`Processing request ${request._id}`)
  console.log(request)

  let should_ignore = await request_should_ignore(dbCollection,request);
  if (should_ignore) {
  	await sync.updateRequest(dbCollection,request,{"status":"Ignored"});
  	return;
  }

  try {

  	await sync.syncSFChanges(oauth2, CONFIG.sfUser, CONFIG.sfPasswordWithKey, realmUser, dbCollection, request);
  	await sync.updateRequest(dbCollection,request,{"status":"Finished","ts.finished":new Date()});
  	console.log("Done");

  } catch(error) {

  	console.error("Failed sync", error);
  	await sync.updateRequest(dbCollection,request,{"status":"Error","error":error.toString(),"ts.finished":new Date()});

  }
}

async function workLoop(realmUser,dbCollection) {
  console.log("Workloop start")

  const query = { "status": "New" };
  const update = {
    "$set": {
      "status": "In Progress",
      "ts.started" : new Date()
    }
  };
  const options = { returnNewDocument: true, sort: {"ts.created":-1} };

  while(true) {
    let doc = await dbCollection.findOneAndUpdate(query, update, options);
    if (!doc)
      return null;
    await process_request(realmUser,dbCollection,doc);
  }
}

async function watcher(realmUser,dbCollection) {
    console.log("Watcher start")
    for await (let event of dbCollection.watch()) {
        const {clusterTime, operationType, fullDocument} = event;

        if ((operationType === 'insert') && event.fullDocument.status === "New") {
            const request = event.fullDocument;

            let res = await dbCollection.updateOne({"_id" : request._id, "status": "New"},
            	{"$set":{"status": "In Progress", "ts.started" : new Date()}});
            if (res.modifiedCount > 0)
            {
              await process_request(realmUser,dbCollection,request);
            } else {
              console.log(`Watcher: someone else picked up the request ${request._id}`)
            }
        }
    }
  }

async function manualSync(realmUser,dbCollection, args) {
	const request = {
		type: "manual_override",
		proj: args.proj,
		ms: args.ms,
		opp: args.opp,
		sched: args.sched,
		attach: args.attach,
		cs: args.cs,
		resync: args.resync,
        tc: args.tc,
	};

	await sync.syncSFChanges(oauth2, CONFIG.sfUser, CONFIG.sfPasswordWithKey, realmUser, dbCollection, request);
}

loginApiKey(realmApiKey).then(user => {
    console.log("Successfully logged in to Realm!");

    const dbCollection = user
      .mongoClient('mongodb-atlas')
      .db('sync')
      .collection('requests');

    if (argv._.includes('sync')) {
      manualSync(user, dbCollection, argv); 
      return;
    }

    let timerId = setTimeout(async function watchForUpdates() {
        timerId && clearTimeout(timerId);
        await workLoop(user,dbCollection);
        await watcher(user,dbCollection);
        timerId = setTimeout(watchForUpdates, 5000);
    }, 5000);

  }).catch((error) => {
    console.error("Failed to log into Realm", error);
  });
