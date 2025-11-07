import { ModuleMetadata } from '@nestjs/common';
import {
  Batch,
  ConsumerConfig,
  EachBatchPayload,
  EachMessagePayload,
  KafkaConfig as IKafkaConfig,
  KafkaMessage as IKafkaMessage,
  Message,
  ProducerRecord,
} from 'kafkajs';

import { KafkaLogLevel } from './kafka.enums';

export { ConsumerConfig } from 'kafkajs';

export interface KafkaSerde {
  serialize(data: KafkaSendInput): Message[];
  deserialize(payload: EachMessagePayload): KafkaMessage;
  deserialize(payload: EachBatchPayload): KafkaMessage[];
}

export interface KafkaMessage<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<IKafkaMessage, 'value' | 'key'> {
  key?: string;
  value?: T;
}

export interface KafkaConsumerPayload<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<EachMessagePayload, 'message'> {
  message: KafkaMessage<T>;
  /**
   * use to ack each message manually if the autoCommit was set to false
   */
  ack: () => Promise<void>;
}

export interface KafkaBatch<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<Batch, 'messages'> {
  messages: KafkaMessage<T>[];
}

export interface KafkaBatchPayload<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<EachBatchPayload, 'batch'> {
  batch: KafkaBatch<T>;
}

export type KafkaConsumerHandler = (
  payload: KafkaConsumerPayload,
) => Promise<void>;

export type KafkaBatchHandler = (
  payload: KafkaBatchPayload,
) => Promise<void>;

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
  fromBeginning?: boolean;
  autoCommit?: boolean;
  groupId?: string;
  batch?: boolean;
}

export interface KafkaConsumerDecoratorConfig extends KafkaConsumerConfig {
  topics: string[];
}

export interface KafkaSendInputMessage extends Omit<Message, 'value'> {
  value: Record<string, any> | Buffer | string | null;
}

export interface KafkaSendInput extends Omit<ProducerRecord, 'messages'> {
  messages: KafkaSendInputMessage | KafkaSendInputMessage[];
}
