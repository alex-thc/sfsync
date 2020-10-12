const sf2mongo_project_field_map = {
	"Id" : "_id",
	"Name" : "name",
	"pse__Account__r.Name" : "account",
	"pse__Region__r.Name" : "region",
	"pse__Is_Active__c" : "active",
	"pse__Stage__c" : "stage",
	"Project_Owner__r.Name" : "owner",
	//opportunity
	"pse__Opportunity__r.Name" : "opportunity.name",
	"pse__Opportunity__r.Owner.Name" : "opportunity.owner",
	"pse__Opportunity__r.Eng_Manager__r.Name" : "opportunity.engagement_manager",
	//summary
	"pse__Planned_Hours__c" : "summary.planned_hours",
	"Gap_Hours__c" : "summary.gap_hours",
	"Backlog_Hours__c" : "summary.backlog_hours",
	//details
	"PM_Project_Status__c" : "details.pm_project_status",
	"pse__Project_Status_Notes__c" : "details.project_status_notes",
	"PM_Stage__c" : "details.pm_stage",
	"Next_Projected_Delivery_Date__c" : "details.next_projected_delivery_date",
	"Active_CSM__c" : "details.active_csm",
	"Remaining_Hours_Will_Expire__c" : "details.remaining_hours_will_expire",
	"pse__End_Date__c" : "details.product_end_date",
	"SystemModstamp" : "SystemModstamp"
}

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

function getSFFieldsString_project() {
	//return Object.getOwnPropertyNames(sf2mongo_project_field_map).join(",");
	var fields = []
	const iterate = (obj) => {
	    Object.keys(obj).forEach(key => {

	    if (typeof obj[key] === 'object') {
	            iterate(obj[key])
	        } else fields.push(obj[key])
	    }) 
	}
	iterate(mongo2sf_project_map)

	return fields.join(",");
}

function projects_transform(sf_docs) {
	var mongo_docs = []
	for (var i in sf_docs) {
		var doc = {}
		for (const [sf_key, mongo_key] of Object.entries(sf2mongo_project_field_map)) {
  			if (sf_key.indexOf(".") < 0) //regular field
  				doc[mongo_key] = sf_docs[i][sf_key]
  			else { //nested
  				let hr = sf_key.split(".")
  				let val = sf_docs[i]
  				try {
  					for (var l in hr) val = val[ hr[l] ];
  					doc[mongo_key] = val
  				} catch(err) {
  					//we'll do nothing here because the odds are thee related object was empty
  				}
  			}
		}
		mongo_docs.push(doc)
	}
	return mongo_docs
}

module.exports = { projects_transform, getSFFieldsString_project }