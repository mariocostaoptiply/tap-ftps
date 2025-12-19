.DEFAULT_GOAL := test

test:
	pylint tap_ftps -d missing-docstring,fixme,duplicate-code,line-too-long
