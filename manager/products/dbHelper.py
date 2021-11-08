from typing import Callable, List, Optional, Dict, Union, Tuple
import db.dbLayer as dbLayer
from models.products import SourceProduct, EoSipProduct

debug=False

def getInputProducts_(filter_date_start: Optional[float], filter_date_stop: Optional[float],
					  limit: Optional[int]) -> []:
	session = dbLayer.Session()
	recs = session.query(SourceProduct).filter().all()
	return None

#
# places = Place.dynamic_filter([('search_id', 'eq', 1)]).all()
#
def dynamic_filter(session, model_class, filter_condition):
	'''
	Return filtered queryset based on condition.
	:param query: takes query
	:param filter_condition: Its a list, ie: [(key,operator,value)]
	operator list:
		eq for ==
		lt for <
		ge for >=
		in for in_
		like for like
		value could be list or a string
	:return: queryset
	'''
	__query = session.query(model_class)
	for raw in filter_condition:
		try:
			key, op, value = raw
		except ValueError:
			raise Exception('Invalid filter: %s' % raw)
		print(f" #### dynamic_filter; key={key}; op={op}; value={value}")
		column = getattr(model_class, key, None)
		if not column:
			raise Exception('Invalid filter column: %s' % key)
		if op == 'in':
			if isinstance(value, list):
				filt = column.in_(value)
			else:
				filt = column.in_(value.split(','))
		else:
			try:
				attr = list(filter(lambda e: hasattr(column, e % op), ['%s', '%s_', '__%s__']))[0] % op
			except IndexError:
				raise Exception(f"Invalid filter operator: {op}; shall be in: 'eq' for ==, 'lt' for <, 'ge' for >=, 'in' for in_, 'like' for like")
			if value == 'null':
				value = None
			filt = getattr(column, attr)(value)
		__query = __query.filter(filt)
	return __query

#
#
#
def getOutputProducts(**kwargs): # filters:[],
	session = dbLayer.Session()
	filters = kwargs['filters'] if 'filters' in kwargs else []
	res=dynamic_filter(session, EoSipProduct, filters)

	if 'order_by' not in kwargs:
		if 'limit' not in kwargs:
			return res.all()
		else:
			return res.limit(kwargs['limit'])
	else:
		if debug:
			print(" order_by")
		if 'desc' in kwargs:
			if debug:
				print(" desc used")
			res=res.order_by(kwargs['order_by'].desc())
			if 'limit' in kwargs:
				if debug:
					print(f" limit used: {kwargs['limit']}")
				return res.limit(kwargs['limit']).all()
			else:
				return res.all()
		else:
			res=res.order_by(kwargs['order_by'])
			if 'limit' in kwargs:
				if debug:
					print(f" limit used: {kwargs['limit']}")
				return res.limit(kwargs['limit']).all()
			else:
				return res.all()

#
#
#
def getInputProducts(**kwargs):
	session = dbLayer.Session()
	filters = kwargs['filters'] if 'filters' in kwargs else []
	print(f" @@@@@@@@%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%@@@@@@@@ getInputProducts; filter: {filters}")
	res=dynamic_filter(session, SourceProduct, filters)

	if 'order_by' not in kwargs:
		if 'limit' not in kwargs:
			return res.all()
		else:
			return res.limit(kwargs['limit'])
	else:
		if debug:
			print(" order_by")
		if 'desc' in kwargs:
			if debug:
				print(" desc used")
			res=res.order_by(kwargs['order_by'].desc())
			if 'limit' in kwargs:
				if debug:
					print(f" limit used: {kwargs['limit']}")
				return res.limit(kwargs['limit']).all()
			else:
				return res.all()
		else:
			res=res.order_by(kwargs['order_by'])
			if 'limit' in kwargs:
				if debug:
					print(f" limit used: {kwargs['limit']}")
				return res.limit(kwargs['limit']).all()
			else:
				return res.all()
