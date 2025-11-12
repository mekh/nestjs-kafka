import {
  Headers,
  KafkaBatchConsumer,
  KafkaBatchPayload,
  KafkaConsumer,
  KafkaEachMessagePayload,
  Key,
  Value,
} from '../src';

describe('Decorators: KafkaConsumer parameter injection', () => {
  it('should map @Value, @Key, @Headers and pass full payload to undecorated params (single message mode)', async () => {
    class TestSvc {
      // return received args for assertion
      @KafkaConsumer('topic-1', { groupId: 'g1' })
      handle(
        @Value() value: any,
        @Key() key: string | undefined,
        @Headers() headers: Record<string, string | undefined> | undefined,
        payload: KafkaEachMessagePayload,
      ) {
        return { value, key, headers, payload };
      }
    }

    const svc = new TestSvc();

    // We simulate what KafkaService passes after normalization
    const payload: KafkaEachMessagePayload = {
      topic: 'topic-1',
      partition: 0,
      heartbeat: async () => undefined as any,
      pause: () => undefined as any,
      message: {
        offset: '1',
        timestamp: Date.now().toString(),
        key: 'k-1',
        value: { a: 1 } as any,
        headers: { h1: 'v1', num: '2' },
      } as any,
      ack: async () => undefined,
    } as any;

    // The decorator wraps the method, so calling it should inject mapped params
    const res = (svc as any).handle(payload);

    expect(res.value).toEqual({ a: 1 });
    expect(res.key).toBe('k-1');
    expect(res.headers).toEqual({ h1: 'v1', num: '2' });
    // Undecorated param gets full payload (same reference)
    expect(res.payload).toBe(payload);
  });

  it('should map arrays for @Value, @Key, @Headers in batch mode via KafkaConsumer(..., { batch: true })', async () => {
    class BatchSvc1 {
      @KafkaConsumer('topic-batch', { groupId: 'g1', batch: true })
      handle(
        @Value() values: any[],
        @Key() keys: (string | undefined)[],
        @Headers() headersList: Record<string, string | undefined>[],
        payload: KafkaBatchPayload,
      ) {
        return { values, keys, headersList, payload };
      }
    }

    const svc = new BatchSvc1();

    const batchPayload: KafkaBatchPayload = {
      batch: {
        topic: 'topic-batch',
        partition: 0,
        messages: [
          {
            message: {
              offset: '1',
              timestamp: '0',
              key: 'k1',
              value: { x: 1 } as any,
              headers: { a: '1' },
            },
          } as any,
          {
            message: {
              offset: '2',
              timestamp: '0',
              key: undefined,
              value: { x: 2 } as any,
              headers: { b: '2' },
            },
          } as any,
        ],
        isEmpty: () => false,
        firstOffset: () => '1',
        lastOffset: () => '2',
        highWatermark: '3',
        offsetLag: () => '0',
        offsetLagLow: () => '0',
        commitOffsetsIfNecessary: async () => undefined as any,
        heartbeat: async () => undefined as any,
        uncommittedOffsets: async () => ({}) as any,
        resolveOffset: () => undefined as any,
        // kafkajs has other fields, but they are not used by our decorator
      } as any,
      heartbeat: async () => undefined as any,
      isRunning: () => true,
      isStale: () => false,
      pause: () => undefined as any,
    } as any;

    const res = (svc as any).handle(batchPayload);

    expect(res.values).toEqual([{ x: 1 }, { x: 2 }]);
    expect(res.keys).toEqual(['k1', undefined]);
    expect(res.headersList).toEqual([{ a: '1' }, { b: '2' }]);
    // In batch mode no ack is supplied by KafkaService, ensure handler still gets full payload
    expect(res.payload).toBe(batchPayload);
    expect('ack' in res.payload).toBe(false);
  });

  it('should behave the same with @KafkaBatchConsumer shortcut decorator', async () => {
    class BatchSvc2 {
      @KafkaBatchConsumer('topic-batch-2', { groupId: 'g2' })
      handle(@Value() values: any[], @Key() keys: (string | undefined)[]) {
        return { values, keys };
      }
    }

    const svc = new BatchSvc2();

    const payload: KafkaBatchPayload = {
      batch: {
        topic: 'topic-batch-2',
        partition: 1,
        messages: [
          {
            message: {
              offset: '1',
              timestamp: '0',
              key: 'A',
              value: { a: 1 } as any,
            },
          } as any,
          {
            message: {
              offset: '2',
              timestamp: '0',
              key: 'B',
              value: { a: 2 } as any,
            },
          } as any,
        ],
      } as any,
    } as any;

    const res = (svc as any).handle(payload);
    expect(res.values).toEqual([{ a: 1 }, { a: 2 }]);
    expect(res.keys).toEqual(['A', 'B']);
  });
});
