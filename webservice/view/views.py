


def indexView(template, caller, app, **kwargs)->str:
	aDict = {}
	return template.render(aDict)


def statusView(template, caller, app, **kwargs)->str:
	aDict = kwargs
	return template.render(aDict)
