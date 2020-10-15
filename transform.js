const moment = require('moment');

const mongo2sf_project_map = {
	"_id": "Id",
	"name" : "Name",
    "account": "pse__Account__r.Name",
	"region" : "pse__Region__r.Name",
	"active" : "pse__Is_Active__c",
	"stage" : "pse__Stage__c",
	"owner" : "Project_Owner__r.Name",

	"opportunity" : {
		"name" : "pse__Opportunity__r.Name",
		"owner" : "pse__Opportunity__r.Owner.Name",
		"engagement_manager" : "pse__Opportunity__r.Eng_Manager__r.Name",
	},
	
	"summary" : {
		"planned_hours" : "pse__Planned_Hours__c",
		"gap_hours" : "Gap_Hours__c",
		"backlog_hours" : "Backlog_Hours__c",
	},
	
	"details" : {
		"pm_project_status" : "PM_Project_Status__c",
		"project_status_notes" : "pse__Project_Status_Notes__c",
		"pm_stage" : "PM_Stage__c",
		"next_projected_delivery_date" : "Next_Projected_Delivery_Date__c",
		"active_csm" : "Active_CSM__c",
		"remaining_hours_will_expire" : "Remaining_Hours_Will_Expire__c",
		"product_end_date" : "pse__End_Date__c",
	},
	"SystemModstamp" : "SystemModstamp"
}

const mongo2sf_milestone_map = {
	"_id" : "Id",
	"projectId" : "pse__Project__r.Id",
	"name" : "Name",
	"country" : "Country__c",
	"currency" : "CurrencyIsoCode",
	"summary" : {
		"planned_hours" : "pse__Planned_Hours__c",
		"sold_hours" : "Sold_Hours__c",
		"delivered_hours" : "Delivered_Hours__c",
		"gap_hours" : "Gap_Hours__c",
		"unscheduled_hours" : "Scheduled_Gap_Hours__c",
		"hours_for_ns" : "Delivered_Hours_for_NS__c",
	},
	"details" : {
		"first_scheduled_date" : "First_Scheduled_Date__c",
		"last_scheduled_date" : "Last_Scheduled_Date__c",
		"bill_rate" : "Bill_Rate__c",
		"milestone_amount" : "pse__Milestone_Amount__c",
		"delivered_amount" : "Delivered_Amount__c",
	},
	"SystemModstamp" : "SystemModstamp"
}

const mongo2sf_schedule_map = {
	"_id" : "Id",
	"projectId" : "pse__Assignment__r.pse__Project__r.Id",
	"milestoneId" : "pse__Assignment__r.pse__Milestone__r.Id",
	"name" : "Name",
	"isDeleted" : "IsDeleted",
	"week" : "pse__Start_Date__c",
	"resource" : "pse__Resource__r.Name",
	"estimated" : {
		"hours" : "pse__Estimated_Hours__c",
		"days" : "pse__Estimated_Days__c",
		"revenue" : "Scheduled_Billings_USD__c"
	},
	"actual" : {
		"hours" : "pse__Actual_Hours__c",
		"days" : "pse__Actual_Days__c",
		"revenue" : "pse__Actual_Billable_Amount__c"
	},
	"SystemModstamp" : "SystemModstamp"
}

function getSFFieldsString(conv_map) {
   	var fields = []
	const iterate = (obj) => {
	    Object.keys(obj).forEach(key => {

	    if (typeof obj[key] === 'object') {
	            iterate(obj[key])
	        } else fields.push(obj[key])
	    }) 
	}
	iterate(conv_map)

	return fields.join(",");
}

function getSFFieldsString_project() {
	//return Object.getOwnPropertyNames(sf2mongo_project_field_map).join(",");
	return getSFFieldsString(mongo2sf_project_map)
}

function getSFFieldsString_milestone() {
	//return Object.getOwnPropertyNames(sf2mongo_project_field_map).join(",");
	return getSFFieldsString(mongo2sf_milestone_map)
}

function getSFFieldsString_schedule() {
	return getSFFieldsString(mongo2sf_schedule_map)
}

function get_value_flat(doc, key) {
	if (key.indexOf(".") < 0) //regular field
		return doc[key]
	else { //nested
		let hr = key.split(".")
		let val = doc
		try {
			for (var l in hr) val = val[ hr[l] ];
			return val
		} catch(err) {
			//we'll do nothing here because the odds are the related object was empty
			return null
		}
	}
}

function parseAsNeeded(val) {
	if ( val === undefined || val === null) return null;

	var date = moment.utc(val,"YYYY-MM-DD",true);
	if (date.isValid())
		return date.toDate();
	else
		//return val
	{
		date = moment.utc(val,"YYYY-MM-DDTHH:mm:ss.SSSZZ",true);
		if (date.isValid())
			return date.toDate();
		else
			return val;
	}
	// var date = new Date(val)
	// if (isNaN(date))
	// 	return val;
	// else
	// 	return date;
}

function transform(sf_docs, conv_map) {
	var mongo_docs = []
	for (var i in sf_docs) {
		var doc = {}

		const iterate = (obj,target) => {
		    Object.keys(obj).forEach(key => {

			    if (typeof obj[key] === 'object') {
		    		target[key] = {}
		            iterate(obj[key],target[key])
			    } else 
			        target[key] = parseAsNeeded(get_value_flat(sf_docs[i], obj[key]))
		    }) 
		}
		iterate(conv_map, doc)

		mongo_docs.push(doc)
	}
	return mongo_docs
}

function projects_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_project_map)
}

function milestones_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_milestone_map)
}

function schedules_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_schedule_map)
}

module.exports = { 
	projects_transform, getSFFieldsString_project,
	milestones_transform, getSFFieldsString_milestone,
	schedules_transform, getSFFieldsString_schedule
}