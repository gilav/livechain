import db.dbLayer as dbLayer
import logging

# needed to make the models knoan to sqlAlchemy
from models.management import OperationActions, OperationTypes
from models.products import SourceProduct, EoSipProduct

defaultOperationTypes=["operator", "app"]


class DbInit():
	name=None
	url=None

	def __init__(self, name, url):
		self.name=name
		self.url=url
		dbLayer.init(self.name, self.url)
		if dbLayer.databaseIsEmpty():
			dbLayer.createDbSchema()
			if 1==1:
				# populate OperationTypes
				session = dbLayer.Session()
				#map(lambda x, session: session.add(x), defaultOperationTypes)
				#map(lambda x: print(f" creating taction type: {x}"), defaultOperationTypes)
				#aType = OperationTypes()
				#res=list(map(lambda x: session.add(x), defaultOperationTypes))
				#print(res)
				for item in defaultOperationTypes:
					r = OperationTypes()
					r.type=item
					r.description=f"{item} action."
					session.add(r)
				session.commit()





