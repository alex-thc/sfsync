const moment = require('moment');
const util = require('./util');
const tr = require('./transform');

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
async function syncSchedules(user,conn) {
	  console.log("Syncing schedules...");
	  var d = util.get7dbefore(new Date());

	  var dbCol = user
	      .mongoClient('mongodb-atlas')
	      .db('shf')
	      .collection('schedule');

	  var res = await dbCol.find({week:{$gte: d}},{_id:1});
	  var ids = [];
	  var ids_map = {};
	  for(let i in res) {
	  	ids_map[res[i]._id] = 1;
	  }

	  ids = Object.keys(ids_map);

		var i,j,temparray,chunk = 100;
		for (i=0,j=ids.length; i<j; i+=chunk) {

		    temparray = ids.slice(i,i+chunk);
		    //console.log(`Processing ${temparray.length} records`)

		    let cond_where = util.generateIdWhereClause(temparray);

		     let result = await sfQueryWrapper(conn, `SELECT Id FROM pse__Est_Vs_Actuals__c WHERE ${cond_where}`);
			  let done = false;
			  let fetched = 0;
			  while(! done) {
			  		done = result.done;
					fetched = fetched + result.records.length;
					//console.log(`Fetched: ${fetched}/${result.totalSize}`);
					if (result.records.length > 0) {
					  for (let r in result.records)
					  	delete ids_map[result.records[r]['Id']]
					}

			  		if (! result.done) {
			  		      result = await sfQueryMoreWrapper(conn, result.nextRecordsUrl);
			  		}
			}

		}

	  console.log(`Deleting ${Object.keys(ids_map).length} keys`)
	  dbCol.deleteMany({_id:{$in: Object.keys(ids_map) }})

	 
	console.log("Done.");
}

module.exports = { 
	loadProjects, loadSchedules, loadMilestones, syncSchedules
	}