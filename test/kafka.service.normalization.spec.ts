import { KafkaAdminService } from '../src/kafka-admin.service';
import { KafkaRegistryService } from '../src/kafka-registry.service';
import { KafkaService } from '../src/kafka.service';

describe('KafkaService normalization helpers', () => {
  const dummyConfig: any = { brokers: ['b:1'] };
  const admin: any = {
    connect: jest.fn(),
    disconnect: jest.fn(),
    ensureTopics: jest.fn(),
  };
  const registry: any = {
    getTopics: jest.fn(() => []),
    getConsumers: jest.fn(() => []),
    getHandlers: jest.fn(() => []),
  };

  const service = new KafkaService(
    dummyConfig,
    registry as KafkaRegistryService,
    admin as KafkaAdminService,
  );

  it('formatMessage should coerce key/headers to string and parse value', () => {
    const msg: any = {
      key: Buffer.from('k-key'),
      value: Buffer.from(JSON.stringify({ ok: true })),
      headers: {
        x: Buffer.from('1'),
        y: undefined,
      },
      timestamp: '0',
      offset: '0',
    };

    const out = (service as any).formatMessage(msg);
    expect(out.key).toBe('k-key');
    expect(out.value).toEqual({ ok: true });
    expect(out.headers).toEqual({ x: '1', y: undefined });
  });

  it('parseHeaders should return undefined when no headers', () => {
    const res = (service as any).parseHeaders(undefined);
    expect(res).toBeUndefined();
  });
});
