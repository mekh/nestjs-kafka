import { KafkaRegistryService } from '../src/kafka-registry.service';
import { KAFKA_CONFIG_TOKEN } from '../src/kafka.constants';
import { ConsumerDecorator } from '../src/kafka.decorators';

// Minimal InstanceWrapper mock
class InstanceWrapper<T extends object> {
  constructor(public instance: T, public metatype: any = {}) {}
}

describe('KafkaRegistryService scanning and config precedence', () => {
  it('should register consumers and handlers with decorator-level overrides and batch flag', () => {
    const defaultConfig: any = {
      brokers: ['b:1'],
      consumer: { groupId: 'g-default', sessionTimeout: 1000 },
    };

    // Create a provider with a method to be "decorated"
    class TestProvider {
      // this would normally be decorated, but we mock DiscoveryService to return meta
      handle(_: any) {}
    }

    const providerWrapper = new InstanceWrapper(new TestProvider());

    // Mock DiscoveryService
    const discoveryService: any = {
      getProviders: jest.fn(() => [providerWrapper]),
      // Return metadata as if it were placed by @KafkaConsumer
      getMetadataByDecorator: jest.fn((decorator: any, _provider: any, method: string) => {
        if (decorator === ConsumerDecorator && method === 'handle') {
          return {
            topics: ['topic-x'],
            // override default consumer config
            groupId: 'g-override',
            autoCommit: false,
            fromBeginning: true,
            batch: true,
          } as any;
        }
        return undefined;
      }),
    };

    // Mock MetadataScanner
    const metadataScanner: any = {
      getAllMethodNames: jest.fn(() => ['handle']),
    };

    const registry = new KafkaRegistryService(
      // Inject via token simulated by constructor param order
      (defaultConfig as any)[KAFKA_CONFIG_TOKEN] ?? defaultConfig,
      discoveryService,
      metadataScanner,
    );

    registry.onModuleInit();

    // One consumer created with override groupId
    const consumers = registry.getConsumers();
    expect(consumers).toHaveLength(1);
    const consumer = consumers[0];

    expect(consumer.groupId).toBe('g-override');
    expect(consumer.batch).toBe(true);
    expect(consumer.autoCommit).toBe(false);
    expect(consumer.fromBeginning).toBe(true);
    expect(consumer.topics).toEqual(['topic-x']);

    // Handler registered for topic
    const handlers = registry.getHandlers('topic-x');
    expect(handlers && handlers.length).toBe(1);
    expect(handlers![0].handlerName).toContain('TestProvider.handle');
  });
});
