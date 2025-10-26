import { KafkaRegistryService } from '../src/kafka-registry.service';

describe('KafkaRegistryService', () => {
  const makeSvc = (consumerDefault?: any) => {
    const config = {
      brokers: ['b:1'],
      consumer: consumerDefault,
    };

    const methods = ['handle', 'other'];

    const providerWrapper = {
      instance: {
        constructor: { name: 'Prov' },
        handle: () => undefined,
      },
      metatype: function Prov() {},
    };

    const discovery = {
      getProviders: jest.fn(() => [providerWrapper, { instance: undefined }]),
      getMetadataByDecorator: jest.fn((dec, prov, method) => {
        if (method === 'handle') {
          return {
            topics: ['t1', 't2'],
            groupId: 'g1',
            fromBeginning: true,
            autoCommit: false,
          };
        }

        return undefined;
      }),
    } as any;

    const metadataScanner = {
      getAllMethodNames: jest.fn(() => methods),
    } as any;

    const svc = new KafkaRegistryService(
      config,
      discovery,
      metadataScanner,
    );

    return { svc, discovery, metadataScanner, providerWrapper };
  };

  it('should scan providers and register consumers and handlers', () => {
    const { svc } = makeSvc({ groupId: 'g1' });

    svc['scanAndRegister']();

    expect(svc.getTopics().sort()).toEqual(['t1', 't2']);

    const cons = svc.getConsumers();
    expect(cons.length).toBe(1);

    const handlers = svc.getHandlers('t1');
    expect(handlers?.length).toBe(1);
    expect(handlers?.[0].handlerName).toBe('Prov.handle');
  });

  it('should throw on invalid consumer configuration', () => {
    const { svc } = makeSvc(undefined);

    expect(
      () => svc['composeConfig']({ topics: ['t'] }),
    ).toThrow('Invalid consumer configuration');
  });
});
