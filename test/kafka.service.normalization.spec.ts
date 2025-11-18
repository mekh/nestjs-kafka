import { KafkaMessage as IKafkaMessage } from 'kafkajs';
import { KafkaSerde } from '../src/kafka-serde';

describe('KafkaSerdeService normalization helpers', () => {
  const serde = new KafkaSerde();

  it('deserializeMessage should coerce key/headers to string and parse value', () => {
    const msg: IKafkaMessage = {
      key: Buffer.from('k-key'),
      attributes: 1,
      value: Buffer.from(JSON.stringify({ ok: true })),
      headers: {
        x: Buffer.from('1'),
        y: undefined,
      },
      timestamp: '0',
      offset: '0',
    };

    const out = serde.deserialize(msg);
    expect(out.key).toBe('k-key');
    expect(out.value).toEqual({ ok: true });
    expect(out.headers).toEqual({ x: '1', y: undefined });
  });

  it('parseHeaders should return undefined when no headers', () => {
    const res = (serde as any).parseHeaders(undefined);
    expect(res).toBeUndefined();
  });
});
