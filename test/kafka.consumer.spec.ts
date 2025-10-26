import { KafkaConsumer } from '../src/kafka.consumer';

describe('KafkaConsumer', () => {
  const kafka = {
    consumer: jest.fn(() => fakeConsumer),
  };

  const fakeConsumer = {
    connect: jest.fn(async () => undefined),
    disconnect: jest.fn(async () => undefined),
    subscribe: jest.fn(async () => undefined),
    run: jest.fn(async () => undefined),
    commitOffsets: jest.fn(async () => undefined),
  };

  const input = {
    config: { groupId: 'g1' },
    topics: ['t1'],
    fromBeginning: true,
    autoCommit: false,
  };

  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('should expose properties and manage topics', () => {
    const c = KafkaConsumer.create(input);

    expect(c.groupId).toBe('g1');
    expect(c.topics).toEqual(['t1']);
    expect(c.config).toEqual({ groupId: 'g1' });
    expect(c.fromBeginning).toBe(true);
    expect(c.autoCommit).toBe(false);

    c.addTopics(['t2', 't1']);
    expect(c.topics.sort()).toEqual(['t1', 't2']);
  });

  it('should require created consumer before using it', async () => {
    const c = KafkaConsumer.create(input);

    expect(() => c.consumer).toThrow('Kafka consumer is not created');

    c.createConsumer(kafka as any);
    expect(c.consumer).toBe(fakeConsumer);
  });

  it('should connect, subscribe, run and commit', async () => {
    const c = KafkaConsumer.create(input);
    c.createConsumer(kafka as any);

    await c.connect();
    await c.subscribe();
    await c.run({ eachMessage: async () => undefined });

    await c.commitOffset({
      topic: 't1',
      partition: 0,
      offset: '1',
      metadata: 'm',
    });

    expect(fakeConsumer.connect).toHaveBeenCalled();
    expect(fakeConsumer.subscribe).toHaveBeenCalledWith({
      topics: ['t1'],
      fromBeginning: true,
    });
    expect(fakeConsumer.run).toHaveBeenCalled();
    expect(fakeConsumer.commitOffsets).toHaveBeenCalledWith([
      { topic: 't1', partition: 0, offset: '1', metadata: 'm' },
    ]);
  });
});
