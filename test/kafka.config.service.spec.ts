import { KafkaConfigService } from '../src/kafka-config.service';

describe('KafkaConfigService', () => {
  it('composeConfig merges module defaults with decorator overrides and computes run/subscription configs', () => {
    const moduleConfig = {
      brokers: ['b:1'],
      consumer: {
        groupId: 'group-module',
        sessionTimeout: 1000,
        autoCommit: true,
      },
    } as any;

    const svc = new KafkaConfigService(moduleConfig);

    const flat = svc.composeConsumerConfig({
      topics: ['t1', 't2'],
      // override groupId and autoCommit, and enable batch
      groupId: 'group-override',
      autoCommit: false,
      batch: true,
      fromBeginning: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
    } as any);

    // consumer config merged and overridden
    expect(flat.consumerConfig.groupId).toBe('group-override');
    expect(flat.consumerConfig.sessionTimeout).toBe(1000);

    // subscription config
    expect(flat.subscriptionConfig.topics).toEqual(['t1', 't2']);
    expect(flat.subscriptionConfig.fromBeginning).toBe(true);

    // run config
    expect(flat.runConfig.batch).toBe(true);
    expect(flat.runConfig.autoCommit).toBe(false);
    expect(flat.runConfig.eachBatchAutoResolve).toBe(false); // batch && autoCommit
    expect(flat.runConfig.partitionsConsumedConcurrently).toBe(1);
  });

  it('composeConfig uses module-level groupId if decorator does not provide it', () => {
    const moduleConfig = {
      brokers: ['b:1'],
      consumer: { groupId: 'group-module' },
    } as any;

    const svc = new KafkaConfigService(moduleConfig);
    const flat = svc.composeConsumerConfig({ topics: ['t1'] } as any);

    expect(flat.consumerConfig.groupId).toBe('group-module');
  });

  it('composeConfig throws if groupId is missing in both module and decorator configs', () => {
    const moduleConfig = { brokers: ['b:1'] } as any; // no consumer default
    const svc = new KafkaConfigService(moduleConfig);

    expect(() => svc.composeConsumerConfig({ topics: ['t1'] } as any)).toThrow(
      'Invalid consumer configuration',
    );
  });
});
