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

  it('should expose provider and handler names', () => {
    const h = KafkaHandler.create({ groupId: 'g' }, providerWrapper, 'foo');

    expect(h.providerName).toBe('TestProvider');
    expect(h.handlerName).toBe('TestProvider.foo');
  });

  it('should bind handle to provider instance', () => {
    const ret = KafkaHandler
      .create({ groupId: 'g' }, providerWrapper, 'foo')
      .handle(123 as any);

    expect(providerWrapper.instance.calls).toEqual([123]);
    expect(ret).toBe(providerWrapper.instance);
  });
});
