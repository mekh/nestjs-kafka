import type { Config } from 'jest';

const config: Config = {
  verbose: true,
  maxWorkers: 1,
  forceExit: true,
  moduleFileExtensions: ['js', 'json', 'ts'],
  rootDir: '.',
  testEnvironment: 'node',
  testRegex: '\.spec\.ts$',
  collectCoverageFrom: [
    'src/**/*.ts',
  ],
  detectOpenHandles: true,
  transform: {
    '^.+.tsx?$': ['ts-jest', {}],
  },
};

export default config;
