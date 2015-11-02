#!/usr/bin/env bash

NODE_VERSIONS=( "5.0.0" "4.2.1" "0.10.40" )

test:
	for NODE_VERSION in "${NODE_VERSIONS[@]}"
	do
		docker run -v ${PWD}:/usr/src/app -w /usr/src/app --rm node:$NODE_VERSION npm test
	done

compile:


.PHONY: test compile