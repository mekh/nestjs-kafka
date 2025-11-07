import { KafkaService } from '../src';
import { KafkaAdminService } from '../src/kafka-admin.service';
import { KafkaRegistryService } from '../src/kafka-registry.service';
import { KafkaSerdeService } from '../src/kafka-serde.service';
// @ts-ignore
import { __setKafkaMock } from 'kafkajs';

describe('KafkaService', () => {
  const producer = {
    connect: jest.fn(async () => undefined),
    disconnect: jest.fn(async () => undefined),
    send: jest.fn(async () => [{ topicName: 't', partition: 0, errorCode: 0 }]),
  };

  const admin: jest.Mocked<KafkaAdminService> = {
    kafka: {} as any,
    admin: {} as any,
    isConnected: false,
    isConnecting: false,
    ensureTopics: jest.fn(async () => undefined),
    connect: jest.fn(async () => undefined),
    disconnect: jest.fn(async () => undefined),
  } as any;

  const makeConsumer = () => ({
    groupId: 'g1',
    autoCommit: false,
    batch: false,
    createConsumer: jest.fn(() => undefined),
    connect: jest.fn(async () => undefined),
    subscribe: jest.fn(async () => undefined),
    run: jest.fn(async () => undefined),
    commitOffset: jest.fn(async () => undefined),
    disconnect: jest.fn(async () => undefined),
  });

  const registry: jest.Mocked<KafkaRegistryService> = {
    handlers: new Map(),
    consumers: new Map(),
    getTopics: jest.fn(() => ['t1', 't2']),
    getHandlers: jest.fn(() => [] as any),
    getConsumers: jest.fn(() => [makeConsumer() as any]),
    onModuleInit: jest.fn(),
  } as any;

  const config: any = { brokers: ['b:1'] };
  const serde = new KafkaSerdeService();

  beforeEach(() => {
    jest.clearAllMocks();
    __setKafkaMock({ producer, admin });
  });

  it('should connect: producer, admin, topics and consumers', async () => {
    const cons = makeConsumer();
    registry.getConsumers.mockReturnValue([cons as any]);

    const s = new KafkaService(config, serde, registry, admin);

    await s.connect();

    expect(producer.connect).toHaveBeenCalled();
    expect(admin.connect).toHaveBeenCalled();
    expect(admin.ensureTopics).toHaveBeenCalledWith(['t1', 't2']);

    expect(cons.createConsumer).toHaveBeenCalled();
    expect(cons.connect).toHaveBeenCalled();
    expect(cons.subscribe).toHaveBeenCalled();
    expect(cons.run).toHaveBeenCalled();
  });

  it('should disconnect producer and all consumers', async () => {
    const cons = makeConsumer();
    registry.getConsumers.mockReturnValue([cons as any]);

    const s = new KafkaService(config, serde, registry, admin);
    await s.connect();

    await s.disconnect();

    expect(producer.disconnect).toHaveBeenCalled();
    expect(cons.disconnect).toHaveBeenCalled();
  });

  it('should send messages and ensure topics', async () => {
    const s = new KafkaService(config, serde, registry, admin);

    const res = await s.send({
      topic: 't',
      messages: { key: 'k', value: { a: 1 } },
    });

    expect(admin.ensureTopics).toHaveBeenCalledWith('t');
    expect(producer.send).toHaveBeenCalledWith({
      topic: 't',
      messages: [
        { key: 'k', value: JSON.stringify({ a: 1 }) },
      ],
    });
    expect(res.length).toBe(1);
  });
});
