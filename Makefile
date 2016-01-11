SHELL := /bin/bash
NODE_VERSIONS := 5.4.0

test: $(NODE_VERSIONS)

$(NODE_VERSIONS):
	docker run -d --name=rabbitmq rabbitmq; \
	docker run --link=rabbitmq -v ${PWD}:/src -w /src --rm -e NODE_ENV=docker makeomatic/alpine-node:$@ npm test; \
	EXIT_CODE=$?; \
	docker rm -f rabbitmq; \
	exit ${EXIT_CODE};

.PHONY: test