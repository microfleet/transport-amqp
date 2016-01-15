#!/bin/sh

cd ../amqp-coffee || cd ./node_modules/amqp-coffee
npm i
npm prune --production