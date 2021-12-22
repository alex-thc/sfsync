function get7dbefore(d) {
    d = new Date(d);
    diff = d.getDate() - 7;
    return new Date(d.setDate(diff));
}

function getSunday(d) {
    d = new Date(d);
    var day = d.getUTCDay(),
    diff = d.getUTCDate() - day;
    d = new Date(d.setUTCDate(diff));
    d.setUTCHours(0);	d.setUTCMinutes(0); d.setUTCSeconds(0); d.setUTCMilliseconds(0);
    return d;
 }

function getSunday31dbefore(d) {
    d = new Date(d);
    diff = d.getDate() - 31;
    return getSunday(new Date(d.setDate(diff)));
}

function generateIdWhereClause(ids, func) {
	var where = "Id IN ('"

	if (ids.length>1)
		for (let i in ids.slice(0,ids.length-1)) {
			where = where + `${func(ids[i])}','`
		}

	where = where + `${func(ids[ids.length-1])}')`

	return where;
}

module.exports = {
	get7dbefore, getSunday31dbefore, generateIdWhereClause
}