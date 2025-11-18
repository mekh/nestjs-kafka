import { KafkaRegistryService } from '../src/kafka-registry.service';
import { ConsumerDecorator } from '../src/kafka.decorators';

class InstanceWrapper<T extends object> {
  constructor(public instance: T, public metatype: any = {}) {}
}

describe('KafkaRegistryService scanning and config precedence', () => {
  it('should register consumers and handlers with decorator-level overrides and batch flag', () => {
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

    const consumerMock = {
      addSubscription: jest.fn(),
    } as any;

    const moduleRef: any = {
      get: jest.fn(() => consumerMock),
    };

    const registry = new KafkaRegistryService(
      discoveryService as any,
      metadataScanner as any,
      moduleRef as any,
    );

    registry.onModuleInit();

    const consumers = registry.getConsumers();
    expect(consumers).toHaveLength(1);
    // moduleRef returned our consumer instance
    expect(moduleRef.get).toHaveBeenCalledWith('g-override', { strict: false });
    // ensure subscription added using decorator metadata
    expect(consumerMock.addSubscription).toHaveBeenCalledWith({
      topics: ['topic-x'],
      groupId: 'g-override',
      autoCommit: false,
      fromBeginning: true,
      batch: true,
    });

    // handlers were registered for the topic
    const handlers = registry.getHandlers('topic-x');
    expect(handlers).toBeDefined();
    expect(handlers?.length).toBe(1);
  });
});
