function projects_transform(sf_docs) {
	var mongo_docs = sf_docs
	for (var i in mongo_docs) {
		mongo_docs[i]._id = mongo_docs[i].Id
	}
	return mongo_docs
}

module.exports = { projects_transform }