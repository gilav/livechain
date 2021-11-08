if 1==2:
	defaultOperationTypes=["operator", "app"]

	def aa(x):
		print(f" creating action type: {x}")

	res=list(map(lambda x: aa(x), defaultOperationTypes))
	print(res)