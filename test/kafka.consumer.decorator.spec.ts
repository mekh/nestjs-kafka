import { KafkaConsumer } from '../src';

describe('KafkaConsumer decorator', () => {
  it('should throw when no topics provided', () => {
    // empty topics array
    expect(() => KafkaConsumer('g', [])).toThrow(
      'No topics provided',
    );
  });

  it('should create a decorator for valid topics', () => {
    const dec = KafkaConsumer('g', 't1');

    expect(typeof dec).toBe('function');
  });
});
