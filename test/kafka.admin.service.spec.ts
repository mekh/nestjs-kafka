import { KafkaAdminService } from '../src/kafka-admin.service';
// @ts-ignore
import { __setKafkaMock } from 'kafkajs';

jest.mock('kafkajs');

describe('KafkaAdminService', () => {
  const admin = {
    listTopics: jest.fn(async () => ['t1']),
    createTopics: jest.fn(async () => true),
    describeCluster: jest.fn(async () => ({ brokers: [{ nodeId: 1 }] })),
    describeConfigs: jest.fn(async () => ({
      resources: [
        {
          configEntries: [
            { configName: 'num.partitions', configValue: '3' },
            { configName: 'default.replication.factor', configValue: '2' },
          ],
        },
      ],
    })),
    connect: jest.fn(async () => undefined),
    disconnect: jest.fn(async () => undefined),
  } as any;

  const baseConfig: any = {
    brokers: ['b:1'],
    topicAutoCreate: true,
    connectionTimeout: 5,
  };

  beforeEach(() => {
    jest.clearAllMocks();
    __setKafkaMock({ admin });
  });

  it('should ensure topics: skip when known or disabled', async () => {
    const s = new KafkaAdminService({ ...baseConfig, topicAutoCreate: false });

    await s.ensureTopics(['t1', 't2']);

    expect(admin.listTopics).not.toHaveBeenCalled();

    const s2 = new KafkaAdminService(baseConfig);
    await s2.ensureTopics(['t1']);

    expect(admin.listTopics).toHaveBeenCalled();
    expect(admin.createTopics).not.toHaveBeenCalled();
  });

  it('should create missing topics with cluster defaults', async () => {
    const s = new KafkaAdminService(baseConfig);

    await s.ensureTopics(['t2']);

    expect(admin.createTopics).toHaveBeenCalledWith({
      topics: [
        { topic: 't2', numPartitions: 3, replicationFactor: 1 },
      ],
    });
  });

  it('should connect, guard re-entry and disconnect', async () => {
    const s = new KafkaAdminService(baseConfig);

    await s.connect();
    expect(admin.connect).toHaveBeenCalledTimes(1);

    await s.connect();
    expect(admin.connect).toHaveBeenCalledTimes(1);

    await s.disconnect();
    expect(admin.disconnect).toHaveBeenCalledTimes(1);
  });

  it('should timeout when waiting connection', async () => {
    const s = new KafkaAdminService({ ...baseConfig, connectionTimeout: 1 });

    (s as any).isConnecting = true;

    await expect(s.connect()).rejects.toThrow('connection timeout');
  });
});
