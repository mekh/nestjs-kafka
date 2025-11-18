import * as index from '../src';

describe('index exports', () => {
  it('should export module and service', () => {
    expect(index.KafkaModule).toBeDefined();
    expect(index.KafkaService).toBeDefined();
  });

  it('should export types and helpers & decorators', () => {
    expect(index.KafkaModuleConfig).toBeDefined();
    expect(index.KafkaLogLevel).toBeDefined();
    expect(index.KafkaConsumer).toBeDefined();
    expect(index.Value).toBeDefined();
    expect(index.Key).toBeDefined();
    expect(index.Headers).toBeDefined();
    expect(index.KafkaBatch).toBeDefined();
  });
});
