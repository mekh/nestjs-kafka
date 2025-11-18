import { KafkaConfigService } from '../src/kafka-config.service';

describe('KafkaConfigService', () => {
  it('composeConfig merges module defaults with decorator overrides and computes run config', () => {
    const moduleConfig = {
      brokers: ['b:1'],
      consumer: {
        sessionTimeout: 1000,
        autoCommit: true,
      },
    } as any;

    const svc = new KafkaConfigService(moduleConfig);

    const flat = svc.composeConsumerConfig({
      // provide groupId and override autoCommit, and enable batch
      groupId: 'group-override',
      autoCommit: false,
      batch: true,
      autoCommitInterval: null,
      autoCommitThreshold: null,
    } as any);

    // consumer config merged and overridden
    expect(flat.consumerConfig.groupId).toBe('group-override');
    expect(flat.consumerConfig.sessionTimeout).toBe(1000);

    // run config
    expect(flat.runConfig.batch).toBe(true);
    expect(flat.runConfig.autoCommit).toBe(false);
    expect(flat.runConfig.eachBatchAutoResolve).toBe(false); // batch && autoCommit
    expect(flat.runConfig.partitionsConsumedConcurrently).toBe(1);
  });

  // groupId must be provided via flat config since module-level consumer omits it

  it('composeConfig throws if groupId is missing in both module and decorator configs', () => {
    const moduleConfig = { brokers: ['b:1'] } as any; // no consumer default
    const svc = new KafkaConfigService(moduleConfig);

    expect(() => svc.composeConsumerConfig({} as any)).toThrow(
      'Invalid consumer configuration',
    );
  });
});
