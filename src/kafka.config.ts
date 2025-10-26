import { KafkaLogLevel } from './kafka.enums';
import { KafkaConfig } from './kafka.interfaces';

export class KafkaDefaultConfig {
  public static getConfig(): KafkaConfig {
    return new KafkaDefaultConfig().getConfig();
  }

  public readonly brokers = this.asArray('KAFKA_BROKER') ?? ['localhost:9092'];

  public readonly clientId = this.asString('KAFKA_CLIENT_ID');

  public readonly groupId = this.asString('KAFKA_GROUP_ID');

  public readonly retryCount = this.asNumber('KAFKA_RETRY_COUNT');

  public readonly retryDelay = this.asNumber('KAFKA_RETRY_DELAY');

  public readonly retryTimeout = this.asNumber('KAFKA_RETRY_TIMEOUT');

  public readonly enforceTimeout = this.asBoolean('KAFKA_ENFORCE_TIMEOUT');

  public readonly connectionTimeout = this.asNumber('KAFKA_CONNECTION_TIMEOUT');

  public readonly requestTimeout = this.asNumber('KAFKA_REQUEST_TIMEOUT');

  public readonly topicAutoCreate = this.asBoolean('KAFKA_TOPIC_AUTO_CREATE') ??
    false;

  public readonly logLevel = this.asString('KAFKA_LOG_LEVEL') ?? 'error';

  public getConfig(): KafkaConfig {
    return {
      brokers: this.brokers,
      clientId: this.clientId,
      retry: {
        retries: this.retryCount,
        initialRetryTime: this.retryDelay,
        maxRetryTime: this.retryTimeout,
      },
      consumer: {
        groupId: this.groupId,
      },
      enforceRequestTimeout: this.enforceTimeout,
      connectionTimeout: this.connectionTimeout,
      requestTimeout: this.requestTimeout,
      topicAutoCreate: this.topicAutoCreate,
      logLevel: this.getLogLevel(this.logLevel),
    };
  }

  protected getLogLevel(level: string): KafkaLogLevel | undefined {
    switch (level) {
      case 'nothing':
        return KafkaLogLevel.NOTHING;
      case 'error':
        return KafkaLogLevel.ERROR;
      case 'warn':
        return KafkaLogLevel.WARN;
      case 'info':
        return KafkaLogLevel.INFO;
      case 'debug':
        return KafkaLogLevel.DEBUG;
      default:
        return undefined;
    }
  }

  protected get env(): Record<string, string | undefined> {
    return process.env;
  }

  protected asNumber(envName: string): number | undefined {
    const env = this.env[envName];

    return env ? Number(env) : undefined;
  }

  protected asString(envName: string): string | undefined {
    return this.env[envName];
  }

  protected asBoolean(envName: string): boolean | undefined {
    const value = this.asString(envName);

    return value && ['true', 'false'].includes(value)
      ? value === 'true'
      : undefined;
  }

  protected asArray(envName: string): string[] | undefined {
    return this.env[envName] ? this.env[envName].split(',') : undefined;
  }
}
