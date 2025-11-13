import { KafkaConfigService } from '../src/kafka-config.service';
import { KafkaRegistryService } from '../src/kafka-registry.service';
import { KafkaSerde } from '../src/kafka-serde';
import { ConsumerDecorator } from '../src/kafka.decorators';

class InstanceWrapper<T extends object> {
  constructor(public instance: T, public metatype: any = {}) {}
}

describe('KafkaRegistryService scanning and config precedence', () => {
  it('should register consumers and handlers with decorator-level overrides and batch flag', () => {
    const defaultConfig: any = {
      brokers: ['b:1'],
      consumer: { groupId: 'g-default', sessionTimeout: 1000 },
    };

    class TestProvider {
      handle(_: any) {}
    }

    const providerWrapper = new InstanceWrapper(new TestProvider());

    const discoveryService: any = {
      getProviders: jest.fn(() => [providerWrapper]),
      getMetadataByDecorator: jest.fn(
        (decorator: any, _provider: any, method: string) => {
          if (decorator === ConsumerDecorator && method === 'handle') {
            return {
              topics: ['topic-x'],
              groupId: 'g-override',
              autoCommit: false,
              fromBeginning: true,
              batch: true,
            } as any;
          }
          return undefined;
        },
      ),
    };

    const metadataScanner: any = {
      getAllMethodNames: jest.fn(() => ['handle']),
    };

    const serde = new KafkaSerde();
    const configService = new KafkaConfigService(defaultConfig);

    const registry = new KafkaRegistryService(
      serde,
      discoveryService,
      metadataScanner,
      configService,
    );

    registry.onModuleInit();

    const consumers = registry.getConsumers();
    expect(consumers).toHaveLength(1);
    const consumer = consumers[0];

    expect(consumer.groupId).toBe('g-override');
    expect(consumer.batch).toBe(true);
    expect(consumer.autoCommit).toBe(false);
    expect(consumer.subscriptionConfig.fromBeginning).toBe(true);
    expect(consumer.subscriptionConfig.topics).toEqual(['topic-x']);
  });
});
