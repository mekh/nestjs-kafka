import { KafkaService } from '../src';
import { KafkaAdminService } from '../src/kafka-admin.service';
import { KafkaRegistryService } from '../src/kafka-registry.service';
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

  beforeEach(() => {
    jest.clearAllMocks();
    __setKafkaMock({ producer, admin });
  });

  it('should connect: producer, admin, topics and consumers', async () => {
    const cons = makeConsumer();
    registry.getConsumers.mockReturnValue([cons as any]);

    const s = new KafkaService(config, registry, admin);

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

    const s = new KafkaService(config, registry, admin);
    await s.connect();

    await s.disconnect();

    expect(producer.disconnect).toHaveBeenCalled();
    expect(cons.disconnect).toHaveBeenCalled();
  });

  it('should send messages and ensure topics', async () => {
    const s = new KafkaService(config, registry, admin);

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

  it('should format and parse messages and handle ack', async () => {
    const s = new KafkaService(config, registry, admin);

    const payload: any = {
      topic: 't',
      partition: 0,
      message: {
        offset: '0',
        key: Buffer.from('k'),
        value: Buffer.from(JSON.stringify({ a: 1 })),
        timestamp: '0',
      },
    };

    const parsed = (s as any).parseMessage(payload);
    expect(parsed).toEqual({ a: 1 });

    const fmt = (s as any).formatLogMessage(payload);
    expect(fmt).toEqual({
      topic: 't',
      partition: 0,
      offset: '0',
      key: 'k',
      message: JSON.stringify({ a: 1 }),
      timestamp: '0',
    });

    const handler = jest.fn(async () => undefined);
    registry.getHandlers.mockReturnValueOnce([
      { handle: handler } as any,
    ]);

    await (s as any).handleMessage(payload, {
      autoCommit: false,
      commitOffset: jest.fn(async () => undefined),
    });

    expect(handler).toHaveBeenCalled();
  });

  it('should commit offset when autoCommit disabled', async () => {
    const s = new KafkaService(config, registry, admin);

    const payload: any = {
      topic: 't',
      partition: 0,
      message: { offset: '5' },
    };

    const consumer: any = {
      autoCommit: false,
      commitOffset: jest.fn(async () => undefined),
    };

    await (s as any).commitOffset(payload, consumer);

    expect(consumer.commitOffset).toHaveBeenCalledWith({
      topic: 't',
      partition: 0,
      offset: '6',
    });
  });

  it('should handle empty/invalid messages and log warn', async () => {
    const s = new KafkaService(config, registry, admin);

    const payload: any = {
      topic: 't',
      partition: 0,
      message: { offset: '0', value: undefined, timestamp: '0' },
    };

    expect((s as any).parseMessage(payload)).toBeUndefined();

    const bad: any = {
      topic: 't',
      partition: 0,
      message: {
        offset: '0',
        value: Buffer.from('not-json'),
        timestamp: '0',
      },
    };

    expect((s as any).parseMessage(bad)).toBeUndefined();
  });
});
