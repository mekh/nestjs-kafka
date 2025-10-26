import { KafkaConsumer } from '../src';

describe('KafkaConsumer decorator', () => {
  it('should throw when no topics provided', () => {
    expect(() => KafkaConsumer([], { groupId: 'g' })).toThrow(
      'No topics provided',
    );
  });

  it('should create a decorator for valid topics', () => {
    const dec = KafkaConsumer('t1', { groupId: 'g' });

    expect(typeof dec).toBe('function');
  });
});
