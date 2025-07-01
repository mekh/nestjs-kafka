import { ModuleMetadata } from '@nestjs/common';
import {
  ConsumerConfig,
  EachMessagePayload,
  KafkaConfig as IKafkaConfig,
  KafkaMessage as IKafkaMessage,
  Message,
  ProducerRecord,
} from 'kafkajs';

import { KafkaLogLevel } from './kafka.enums';

export { ConsumerConfig } from 'kafkajs';

export interface KafkaMessage<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<IKafkaMessage, 'value'> {
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

export type KafkaConsumerHandler = (
  payload: KafkaConsumerPayload,
) => Promise<void>;

export interface KafkaConfig extends IKafkaConfig {
  logLevel?: KafkaLogLevel;
  topicAutoCreate?: boolean;
}

export interface KafkaAsyncConfig
  extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  useFactory: (...args: any[]) => KafkaConfig | Promise<KafkaConfig>;
  inject?: any[];
  global?: boolean;
}

export interface KafkaConsumerConfig extends ConsumerConfig {
  fromBeginning?: boolean;
  autoCommit?: boolean;
}

export interface KafkaConsumerDecoratorConfig extends KafkaConsumerConfig {
  topics: string[];
}

export interface KafkaSendInputMessage extends Omit<Message, 'value'> {
  value: Record<string, any>;
}

export interface KafkaSendInput extends Omit<ProducerRecord, 'messages'> {
  messages: KafkaSendInputMessage | KafkaSendInputMessage[];
}
