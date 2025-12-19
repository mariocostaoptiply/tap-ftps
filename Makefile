.DEFAULT_GOAL := test

test:
	pylint tap_ftp -d missing-docstring,fixme,duplicate-code,line-too-long
