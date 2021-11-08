from collections.abc import MutableMapping


class ContextDict(MutableMapping):
	""" """

	def __init__(self, *args, **kwargs):
		self.store = dict()
		self.update(dict(*args, **kwargs))  # use the free update to set keys

	def __getitem__(self, key):
		return self.store[key]

	def __setitem__(self, key, value):
		self.store[key] = value

	def __delitem__(self, key):
		del self.store[key]

	def __iter__(self):
		return iter(self.store)

	def __len__(self):
		return len(self.store)
