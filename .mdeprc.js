module.exports = {
  node: "16",
  auto_compose: true,
  services: ['rabbitmq'],
  nycCoverage: false,
  nycReport: false,
  test_framework: 'c8 /src/node_modules/.bin/mocha',
  extras: {
    tester: {
      environment: {
        NODE_ENV: 'test',
        RABBITMQ_PORT_5672_TCP_ADDR: 'rabbitmq'
      }
    }
  },
  tests: "./test/*.js",
  rebuild: ["microtime"],
  pre: "rimraf ./coverage/tmp || true",
  post_exec: "pnpm exec -- c8 report -r text -r lcov"
}
