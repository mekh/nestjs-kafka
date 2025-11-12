import { ModuleMetadata } from '@nestjs/common';
import {
  Batch,
  ConsumerConfig,
  ConsumerRunConfig,
  EachBatchPayload,
  EachMessagePayload,
  KafkaConfig as IKafkaConfig,
  KafkaMessage as IKafkaMessage,
  Message,
  ProducerRecord,
} from 'kafkajs';

import { KafkaLogLevel } from './kafka.enums';

export { ConsumerConfig } from 'kafkajs';

export type Value = Record<string, any>;
export type Key = string;
export type Headers = Record<string, string | undefined>;

export interface KafkaSerde {
  serialize(data: KafkaSendInput): Message[];
  deserialize<T extends Value>(payload: IKafkaMessage): KafkaMessage<T>;
  deserialize<T extends Value>(payload: IKafkaMessage[]): KafkaMessage<T>[];
}

export interface KafkaMessage<
  T extends Value = Value,
> extends Omit<IKafkaMessage, 'value' | 'key' | 'headers'> {
  key?: Key;
  value?: T;
  headers?: Headers;
}

export interface KafkaEachMessagePayload<
  T extends Value = Value,
> extends Omit<EachMessagePayload, 'message'> {
  message: KafkaMessage<T>;
  /**
   * use to ack each message manually if the autoCommit was set to false
   */
  ack: () => Promise<void>;
}

export interface KafkaBatch<
  T extends Value = Value,
> extends Omit<Batch, 'messages'> {
  messages: KafkaEachMessagePayload<T>[];
}

export interface KafkaBatchPayload<
  T extends Value = Value,
> extends Omit<EachBatchPayload, 'batch'> {
  batch: KafkaBatch<T>;
  ack: () => Promise<void>;
}

export interface KafkaConfig extends IKafkaConfig {
  logLevel?: KafkaLogLevel;
  topicAutoCreate?: boolean;
  consumer?: KafkaConsumerConfig;
}

export interface KafkaAsyncConfig
  extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  useFactory: (...args: any[]) => KafkaConfig | Promise<KafkaConfig>;
  inject?: any[];
  global?: boolean;
}

export interface KafkaConsumerConfig extends Omit<ConsumerConfig, 'groupId'> {
  /**
   * Default consumer group id. Can be defined per-decorator.
   * The group id must be either defined on module level or per-decorator.
   */
  groupId?: string;
  /**
   * If true, the consumer group will use the latest committed offset
   * when starting to fetch messages.
   * Default: false
   */
  fromBeginning?: boolean;
  /**
   * Disable auto committing altogether (autoCommitInterval,
   * autoCommitThreshold, etc.) If false, use the ack() fn returned along
   * with the message to manually commit offsets.
   * Default: true
   */
  autoCommit?: boolean;
  /**
   * The consumer will commit offsets after a given period.
   * Value in milliseconds.
   * Default: null
   */
  autoCommitInterval?: number | null;
  /**
   * The consumer will commit offsets after resolving a given
   * number of messages.
   */
  autoCommitThreshold?: number | null;
  /**
   * Concurrently process several messages per once in a single-message mode.
   * Messages in the same partition are still guaranteed
   * to be processed in order, but messages from multiple
   * partitions can be processed at the same time.
   * Default: 1
   */
  partitionsConsumedConcurrently?: number;
  /**
   * Consume messages in batches.
   * In this mode, the payload will contain these utility functions:
   * - resolveOffset(offset: string): void
   *    used to mark a message in the batch as processed. In case of errors,
   *    the consumer will automatically commit the resolved offsets
   * - heartbeat(): Promise<void>
   *    can be used to send heartbeat to the broker according to the set
   *    heartbeatInterval value in consumer configuration, which means if you
   *    invoke heartbeat() sooner than heartbeatInterval, it will be ignored.
   * - commitOffsetsIfNecessary(offsets?: Offsets): Promise<void>
   *    commit offsets based on the autoCommit configurations
   *    (autoCommitInterval and autoCommitThreshold). Note that auto commit
   *    won't happen in eachBatch if commitOffsetsIfNecessary is not invoked.
   * - uncommittedOffsets(): Offsets
   *    returns all offsets by topic-partition that have not yet been committed.
   * - isRunning(): boolean
   *    returns true if `consumer` is in a running state, else it returns false.
   * - isStale(): boolean
   *    returns whether the messages in the batch have been rendered stale
   *    through some other operation and should be discarded. For example,
   *    when calling `consumer.seek` the messages in the batch
   *    should be discarded, as they are not at the offset we seeked to.
   * - pause(): void
   *    pause the consumer for the current topic-partition. All offsets resolved
   *    up to that point will be committed (subject to eachBatchAutoResolve
   *    and autoCommit). Throw an error to pause in the middle of the batch
   *    without resolving the current offset. Alternatively,
   *    disable eachBatchAutoResolve. The returned function can be used
   *    to resume processing of the topic-partition.
   * Default: false
   */
  batch?: boolean;
}

export interface KafkaConsumerDecoratorConfig extends KafkaConsumerConfig {
  topics: string[];
}

export type SubscriptionConfig = Pick<
  KafkaConsumerDecoratorConfig,
  | 'topics'
  | 'fromBeginning'
>;

export type RunConfig =
  & Pick<
    KafkaConsumerDecoratorConfig,
    | 'autoCommit'
    | 'autoCommitInterval'
    | 'autoCommitThreshold'
    | 'partitionsConsumedConcurrently'
    | 'batch'
  >
  & Pick<ConsumerRunConfig, 'eachBatchAutoResolve'>;

export interface KafkaSendInputMessage extends Omit<Message, 'value'> {
  value: Value | Buffer | string | null;
}

export interface KafkaSendInput extends Omit<ProducerRecord, 'messages'> {
  messages: KafkaSendInputMessage | KafkaSendInputMessage[];
}
