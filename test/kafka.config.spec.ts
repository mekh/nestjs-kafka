import { KafkaDefaultConfig } from '../src';
import { KafkaLogLevel } from '../src/kafka.enums';

describe('KafkaDefaultConfig', () => {
  const OLD_ENV = process.env;

  beforeEach(() => {
    process.env = { ...OLD_ENV };
  });

  afterEach(() => {
    process.env = OLD_ENV;
  });

  it('should map envs to config correctly', () => {
    process.env.KAFKA_BROKER = 'a:1,b:2';
    process.env.KAFKA_CLIENT_ID = 'cid';
    process.env.KAFKA_GROUP_ID = 'gid';
    process.env.KAFKA_RETRY_COUNT = '5';
    process.env.KAFKA_RETRY_DELAY = '1000';
    process.env.KAFKA_RETRY_TIMEOUT = '5000';
    process.env.KAFKA_ENFORCE_TIMEOUT = 'true';
    process.env.KAFKA_CONNECTION_TIMEOUT = '2000';
    process.env.KAFKA_REQUEST_TIMEOUT = '3000';
    process.env.KAFKA_TOPIC_AUTO_CREATE = 'true';
    process.env.KAFKA_LOG_LEVEL = 'debug';

    const conf = new KafkaDefaultConfig().getConfig();

    expect(conf.brokers).toEqual(['a:1', 'b:2']);
    expect(conf.clientId).toBe('cid');
    expect(conf.consumer?.groupId).toBe('gid');
    expect(conf.retry?.retries).toBe(5);
    expect(conf.retry?.initialRetryTime).toBe(1000);
    expect(conf.retry?.maxRetryTime).toBe(5000);
    expect(conf.enforceRequestTimeout).toBe(true);
    expect(conf.connectionTimeout).toBe(2000);
    expect(conf.requestTimeout).toBe(3000);
    expect(conf.topicAutoCreate).toBe(true);
    expect(conf.logLevel).toBe(KafkaLogLevel.DEBUG);
  });

  it('should return defaults when envs are absent', () => {
    delete process.env.KAFKA_BROKER;
    delete process.env.KAFKA_LOG_LEVEL;

    const defaults = KafkaDefaultConfig.getConfig();

    expect(defaults.brokers).toEqual(['localhost:9092']);
    expect(defaults.logLevel).toBe(KafkaLogLevel.ERROR);
    expect(defaults.topicAutoCreate).toBe(false);
  });

  it('should map known levels and ignore unknown levels', () => {
    const cfg = new KafkaDefaultConfig();

    expect((cfg as any).getLogLevel('nothing')).toBe(
      KafkaLogLevel.NOTHING,
    );
    expect((cfg as any).getLogLevel('error')).toBe(KafkaLogLevel.ERROR);
    expect((cfg as any).getLogLevel('warn')).toBe(KafkaLogLevel.WARN);
    expect((cfg as any).getLogLevel('info')).toBe(KafkaLogLevel.INFO);
    expect((cfg as any).getLogLevel('debug')).toBe(KafkaLogLevel.DEBUG);
    expect((cfg as any).getLogLevel('other')).toBeUndefined();
  });

  it('should parse booleans and arrays safely', () => {
    process.env.KAFKA_TOPIC_AUTO_CREATE = 'false';
    process.env.KAFKA_BROKER = '';

    const conf = new KafkaDefaultConfig();

    expect((conf as any).asBoolean('KAFKA_TOPIC_AUTO_CREATE')).toBe(false);
    expect((conf as any).asArray('KAFKA_BROKER')).toBeUndefined();
  });
});
