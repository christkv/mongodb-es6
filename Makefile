NODE = node
NPM = npm
JSDOC = jsdoc
name = all

generate_docs:
	$(JSDOC) -c conf.json -t docs/jsdoc-template/ -d ./public/api

.PHONY: total
