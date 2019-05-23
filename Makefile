.DEFAULT_GOAL := test

test:
	pylint tap_stripe -d missing-docstring,too-many-branches
