const moment = require('moment');

const mongo2sf_project_map = {
	"_id": "Id",
	"name" : "Name",
    "account": "pse__Account__r.Name",
    "account_id": "pse__Account__r.Id",
	"region" : "pse__Region__r.Name",
	"active" : "pse__Is_Active__c",
	"stage" : "pse__Stage__c",
	"owner" : "Project_Owner__r.Name",
	"owner_email" : "Project_Owner__r.Email",
	"project_manager" : "pse__Project_Manager__r.Name",
	"project_manager_email" : "pse__Project_Manager__r.Email",
	"ps_ops_resource" : "PS_Ops_Resource__r.Name",
	"ps_ops_resource_email" : "PS_Ops_Resource__r.Email",

	"type" : "pse__Project_Type__c",

	"opportunity" : {
		"name" : "pse__Opportunity__r.Name",
		"owner" : "pse__Opportunity__r.Owner.Name",
		"owner_email" : "pse__Opportunity__r.Owner.Email",
		"engagement_manager" : "pse__Opportunity__r.Eng_Manager__r.Name",
		"engagement_manager_email" : "pse__Opportunity__r.Eng_Manager__r.Email",
		"_id" : "pse__Opportunity__r.Id"
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
		"product_start_date" : "pse__Start_Date__c",
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
		"billable_hours_submitted" : "pse__Billable_Hours_Submitted__c",
		"non_billable_hours_submitted" : "pse__Non_Billable_Hours_Submitted__c",
		"billable_hours_in_financials" : "pse__Billable_Hours_In_Financials__c",
	},
	"details" : {
		"first_scheduled_date" : "First_Scheduled_Date__c",
		"last_scheduled_date" : "Last_Scheduled_Date__c",
		"bill_rate" : "convertCurrency(Bill_Rate__c)",
		"milestone_amount" : "convertCurrency(pse__Milestone_Amount__c)",
		"delivered_amount" : "convertCurrency(Delivered_Amount__c)",
	},
	"SystemModstamp" : "SystemModstamp"
}

const mongo2sf_schedule_map = {
	"_id" : {
		"id" : "Id",
		"ms_id" : "pse__Assignment__r.pse__Milestone__r.Id"
	},
	"projectId" : "pse__Assignment__r.pse__Project__r.Id",
	"milestoneId" : "pse__Assignment__r.pse__Milestone__r.Id",
	"name" : "Name",
	"billable" : "pse__Assignment__r.pse__Is_Billable__c",
	"role" : "pse__Assignment__r.pse__Role__c",
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
		"revenue" : "convertCurrency(pse__Actual_Billable_Amount__c)"
	},
	"assignment" : {
		"start_date" : "pse__Assignment__r.pse__Start_Date__c",
		"end_date" : "pse__Assignment__r.pse__End_Date__c",
		"createdDate" : "pse__Assignment__r.CreatedDate",
	},
	"SystemModstamp" : "SystemModstamp"
}

const mongo2sf_opportunity_map = {
	"_id": "Id",
	"name" : "Name",
	"owner_region" : "Owner_Region__c",
//	"owner_region2" : "Owner.Region__c",
	"type" : "Type",
	"owner" : "Owner.Name",
    "stage" : "StageName",
    "forecast_category" : "ForecastCategoryName",
    "close_date" : "CloseDate",
    "amount" : "convertCurrency(Amount)",
    "account": {
    	"name": "Account.Name",
    	"_id": "Account.Id",
    	"geo_region": "Account.Geo_Region__c",
    	"owner" : "Account.Owner.Name",
    	"owner_region" : "Account.Owner.Region__c"
    },
    
    "em" : {
    	"engagement_manager" : "Eng_Manager__r.Name",
    	"ps_status" : "PS_Status__c",
    	"esd_created" : "ESD_Created__c",
    	"call" : "Engagement_Manager_Call__c",
    	"call_amount" : "Value__c"
    },

    "services_post_carve" : "convertCurrency(Services_post_carve__c)",
    "has_services" : "Has_Services_Products__c",

    "sales_forecast" : {
    	"AE" : "In_Forecast_AE__c",
    	"RD" : "In_Forecast_RD__c",
    	"amount_services_RD" : "In_Forecast_Amount_Services_RD__c"
    },

    "ps_notes" : "PS_Notes__c",
    "ps_region" : "pse__Region__r.Name",

	"SystemModstamp" : "SystemModstamp"
}

function getSpecialTagValue(ps_notes, tag) {
	if (!ps_notes) return null;
	return (ps_notes.indexOf(`${tag}="Yes"`) >= 0) ? "Yes" : (ps_notes.indexOf(`${tag}="Maybe"`) >= 0) ? "Maybe" : (ps_notes.indexOf(`${tag}="No"`) >= 0) ? "No" : null;
}

function opportunity_posttransform(doc) {
	var ps_needed = getSpecialTagValue(doc.ps_notes, "%PS_NEEDED%");
	var em_needed = getSpecialTagValue(doc.ps_notes, "%EM_NEEDED%");
	if (ps_needed || em_needed)
		doc.ps_triage = {ps_needed, em_needed};
	return doc;
}

function milestone_posttransform(doc) {
	doc.fromTraining = doc.name.includes("Training");
	return doc;
}

function getStageSortId(stage) {
	switch(stage) {
		case "Not Started" : return 0;
		case "Planning" : return 10;
		case "In Progress": return 20;
		case "On Hold": return 30;
		case "Cancelled": return 40;
		case "Closed": return 50;
		default: return 0;
	}
}

function project_posttransform(doc) {
	doc.details.pm_stage_sortid = getStageSortId(doc.details.pm_stage);
	return doc;
}

function getSFFieldsString(conv_map) {
   	var fields_map = {}
	const iterate = (obj) => {
	    Object.keys(obj).forEach(key => {

	    if (typeof obj[key] === 'object') {
	            iterate(obj[key])
	        } else fields_map[obj[key]] = 1;
	    }) 
	}
	iterate(conv_map)

	return Object.keys(fields_map).join(",");
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

function getSFFieldsString_opportunity() {
	return getSFFieldsString(mongo2sf_opportunity_map)
}

function get_value_flat(doc, key) {
	//strip the currency conversion function if present
	if (key.startsWith("convertCurrency("))
		key = key.slice(16,key.length-1);

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
			return val; //note that this will insert numbers as both int and double
	}
}

function transform(sf_docs, conv_map, func_posttransform = null) {
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

		if (func_posttransform != null)
			doc = func_posttransform(doc);

		mongo_docs.push(doc)
	}
	return mongo_docs
}

function projects_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_project_map, project_posttransform)
}

function milestones_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_milestone_map, milestone_posttransform)
}

function schedules_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_schedule_map)
}

function opportunities_transform(sf_docs) {
	return transform(sf_docs, mongo2sf_opportunity_map, opportunity_posttransform)
}

module.exports = { 
	projects_transform, getSFFieldsString_project,
	milestones_transform, getSFFieldsString_milestone,
	schedules_transform, getSFFieldsString_schedule, 
	opportunities_transform, getSFFieldsString_opportunity
}