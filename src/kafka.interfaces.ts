import { ModuleMetadata } from '@nestjs/common';
import {
  ConsumerConfig,
  EachMessagePayload,
  KafkaConfig as IKafkaConfig,
  KafkaMessage as IKafkaMessage,
  Message,
  ProducerRecord,
} from 'kafkajs';

export { logLevel as KafkaLogLevel } from 'kafkajs';

export interface KafkaMessage<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<IKafkaMessage, 'value'> {
  value?: T;
}

export interface KafkaMessagePayload<
  T extends Record<string, any> = Record<string, any>,
> extends Omit<EachMessagePayload, 'message'> {
  message: KafkaMessage<T>;
  /**
   * use to ack each message manually if the autoCommit was set to false
   */
  ack: () => Promise<void>;
}

export type KafkaMessageHandler = (
  payload: KafkaMessagePayload,
) => Promise<void>;

export interface KafkaConfig extends IKafkaConfig {
  topicAutoCreate?: boolean;
}

export interface KafkaAsyncOptions
  extends Pick<ModuleMetadata, 'imports' | 'providers'> {
  useFactory: (...args: any[]) => KafkaConfig | Promise<KafkaConfig>;
  inject?: any[];
}

export interface KafkaLogMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | undefined;
  timestamp: string;
  message?: string;
}

export interface KafkaConsumerConfig extends ConsumerConfig {
  fromBeginning?: boolean;
  autoCommit?: boolean;
}

export interface KafkaConsumerDecoratorOptions extends ConsumerConfig {
  topics: string[];
  fromBeginning?: boolean;
  autoCommit?: boolean;
}

export interface KafkaProducerMessage extends Omit<Message, 'value'> {
  value: Record<string, any>;
}

export interface KafkaProducerRecord extends Omit<ProducerRecord, 'messages'> {
  messages: KafkaProducerMessage | KafkaProducerMessage[];
}
