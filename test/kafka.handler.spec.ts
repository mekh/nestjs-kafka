import { EachBatchPayload, Offsets } from 'kafkajs';
import { KafkaHandler } from '../src/kafka.handler';

describe('KafkaHandler', () => {
  class TestProvider {
    public calls: any[] = [];

    public foo(arg: any): TestProvider {
      this.calls.push(arg);

      return this;
    }
  }

  const providerWrapper: any = { instance: new TestProvider() };
  const consumer: any = {
    autoCommit: false,
    commitOffset: jest.fn(async () => undefined),
    isPaused: jest.fn(() => false),
  };

  it('should expose provider and handler names', () => {
    const h = KafkaHandler.create(providerWrapper as any, 'foo', consumer);

    expect(h.providerName).toBe('TestProvider');
    expect(h.handlerName).toBe('TestProvider.foo');
  });

  it('should call provider handler with deserialized message and ack', async () => {
    const handler = KafkaHandler.create(providerWrapper as any, 'foo', consumer);

    const payload: EachBatchPayload = {
      batch: {
        topic: 't',
        partition: 0,
        highWatermark: '1',
        firstOffset: () => '1',
        offsetLag: () => '1',
        offsetLagLow: () => '1',
        lastOffset: () => '1',
        isEmpty: () => false,
        messages: [{
          offset: '0',
          attributes: 1,
          key: Buffer.from('k'),
          value: Buffer.from(JSON.stringify({ ok: true })),
          headers: { x: Buffer.from('1') },
          timestamp: '0',
        }],
      },
      isStale: jest.fn(() => false),
      isRunning: jest.fn(() => true),
      resolveOffset: jest.fn,
      uncommittedOffsets(): any {},
      commitOffsetsIfNecessary(offsets?: Offsets): any {},
      pause: () => () => {},
      heartbeat(): any {},
    };
    await handler.handle(payload);

    expect(providerWrapper.instance.calls.length).toBe(1);
    const callArg = providerWrapper.instance.calls[0];
    expect(callArg.message.value).toEqual({ ok: true });
    expect(typeof callArg.ack).toBe('function');

    // Ack should commit next offset when autoCommit is false
    await callArg.ack();
    expect(consumer.commitOffset).toHaveBeenCalledWith({
      topic: 't',
      partition: 0,
      offset: '1',
    });
  });
});
