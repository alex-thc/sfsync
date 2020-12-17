function get7dbefore(d) {
    d = new Date(d);
    diff = d.getDate() - 7;
    return new Date(d.setDate(diff));
}

function generateIdWhereClause(ids) {
	var where = "Id IN ('"

	if (ids.length>1)
		for (let i in ids.slice(0,ids.length-1)) {
			where = where + `${ids[i]}','`
		}

	where = where + `${ids[ids.length-1]}')`

	return where;
}

module.exports = {
	get7dbefore, generateIdWhereClause
}