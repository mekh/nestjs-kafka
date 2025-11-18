import { KafkaLogLevel } from './kafka.enums';
import { KafkaConfig } from './kafka.interfaces';

export class KafkaModuleConfig {
  public static getConfig(): KafkaConfig {
    return new KafkaModuleConfig().getConfig();
  }

  public readonly brokers = this.asArray('KAFKA_BROKER') ?? ['localhost:9092'];

  public readonly clientId = this.asString('KAFKA_CLIENT_ID');

  public readonly retryCount = this.asNumber('KAFKA_RETRY_COUNT');

  public readonly retryDelay = this.asNumber('KAFKA_RETRY_DELAY');

  public readonly retryFactor = this.asNumber('KAFKA_RETRY_FACTOR');

  public readonly retryMultiplier = this.asNumber('KAFKA_RETRY_MULTIPLIER');

  public readonly retryTimeout = this.asNumber('KAFKA_RETRY_TIMEOUT');

  public readonly enforceTimeout = this.asBoolean('KAFKA_ENFORCE_TIMEOUT');

  public readonly connectionTimeout = this.asNumber('KAFKA_CONNECTION_TIMEOUT');

  public readonly requestTimeout = this.asNumber('KAFKA_REQUEST_TIMEOUT');

  public readonly authenticationTimeout = this.asNumber(
    'KAFKA_AUTHENTICATION_TIMEOUT',
  );

  public readonly reauthenticationThreshold = this.asNumber(
    'KAFKA_REAUTHENTICATION_THRESHOLD',
  );

  public readonly topicAutoCreate = this.asBoolean('KAFKA_TOPIC_AUTO_CREATE') ??
    false;

  public readonly logLevel = this.asString('KAFKA_LOG_LEVEL') ?? 'error';

  public sessionTimeout = this.asNumber('KAFKA_CONSUMER_SESSION_TIMEOUT');

  public rebalanceTimeout = this.asNumber('KAFKA_CONSUMER_REBALANCE_TIMEOUT');

  public heartbeatInterval = this.asNumber('KAFKA_CONSUMER_HEARTBEAT_INTERVAL');

  public metadataMaxAge = this.asNumber('KAFKA_CONSUMER_METADATA_MAX_AGE');

  public maxBytesPerPartition = this.asNumber(
    'KAFKA_CONSUMER_MAX_BYTES_PER_PARTITION',
  );

  public minBytes = this.asNumber('KAFKA_CONSUMER_MIN_BYTES');

  public maxBytes = this.asNumber('KAFKA_CONSUMER_MAX_BYTES');

  public maxWaitTimeInMs = this.asNumber('KAFKA_CONSUMER_MAX_WAIT_TIME_IN_MS');

  public maxInFlightRequests = this.asNumber(
    'KAFKA_CONSUMER_MAX_IN_FLIGHT_REQUESTS',
  );

  public readUncommitted = this.asBoolean('KAFKA_CONSUMER_READ_UNCOMMITTED');

  public rackId = this.asString('KAFKA_CONSUMER_RACK_ID');

  public getConfig(): KafkaConfig {
    return {
      brokers: this.brokers,
      clientId: this.clientId,
      connectionTimeout: this.connectionTimeout,
      authenticationTimeout: this.authenticationTimeout,
      reauthenticationThreshold: this.reauthenticationThreshold,
      requestTimeout: this.requestTimeout,
      enforceRequestTimeout: this.enforceTimeout,
      topicAutoCreate: this.topicAutoCreate,
      logLevel: this.getLogLevel(this.logLevel),
      retry: {
        maxRetryTime: this.retryTimeout,
        initialRetryTime: this.retryDelay,
        factor: this.retryFactor,
        multiplier: this.retryMultiplier,
        retries: this.retryCount,
      },
      consumer: {
        metadataMaxAge: this.metadataMaxAge,
        sessionTimeout: this.sessionTimeout,
        rebalanceTimeout: this.rebalanceTimeout,
        heartbeatInterval: this.heartbeatInterval,
        maxBytesPerPartition: this.maxBytesPerPartition,
        minBytes: this.minBytes,
        maxBytes: this.maxBytes,
        maxWaitTimeInMs: this.maxWaitTimeInMs,
        maxInFlightRequests: this.maxInFlightRequests,
        readUncommitted: this.readUncommitted,
        rackId: this.rackId,
      },
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
