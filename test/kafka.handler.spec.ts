import { KafkaHandler } from '../src/kafka.handler';
import { KafkaSerdeService } from '../src/kafka-serde.service';

describe('KafkaHandler', () => {
  class TestProvider {
    public calls: any[] = [];

    public foo(arg: any): TestProvider {
      this.calls.push(arg);

      return this;
    }
  }

  const providerWrapper: any = { instance: new TestProvider() };
  const serde = new KafkaSerdeService();

  it('should expose provider and handler names', () => {
    const h = KafkaHandler.create({ groupId: 'g' }, providerWrapper, 'foo', serde);

    expect(h.providerName).toBe('TestProvider');
    expect(h.handlerName).toBe('TestProvider.foo');
  });

  it('should call provider handler with deserialized message and ack', async () => {
    const handler = KafkaHandler
      .create({ groupId: 'g' }, providerWrapper, 'foo', serde);

    const payload: any = {
      topic: 't',
      partition: 0,
      message: {
        offset: '0',
        key: Buffer.from('k'),
        value: Buffer.from(JSON.stringify({ ok: true })),
        headers: { x: Buffer.from('1') },
        timestamp: '0',
      },
    };
    const consumer: any = {
      autoCommit: false,
      commitOffset: jest.fn(async () => undefined),
    };

    await handler.handle(payload, consumer);

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
