import { applyDecorators } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';

import {
  KAFKA_HEADERS_META,
  KAFKA_KEY_META,
  KAFKA_VALUE_META,
} from './kafka.constants';
import {
  KafkaConsumerConfig,
  KafkaConsumerDecoratorConfig,
  KafkaConsumerPayload,
} from './kafka.interfaces';

const copyMeta = (source: { name: string }, target: object): void => {
  Reflect.getMetadataKeys(source).forEach((key) => {
    const prevMeta = Reflect.getMetadata(key, source);

    Reflect.defineMetadata(key, prevMeta, target);
  });

  Object.defineProperty(target, 'name', {
    value: source.name,
    writable: false,
  });
};

const createParamDecorator = (metaKey: string | symbol): ParameterDecorator => {
  return (target: object, key: string | symbol | undefined, idx: number) => {
    const args = Reflect.getOwnMetadata(metaKey, target, key!) ?? [];

    (args as number[]).push(idx);

    Reflect.defineMetadata(
      metaKey,
      args,
      target,
      key!,
    );
  };
};

/**
 * For internal usage only (within the Kafka module)
 */
export const ConsumerDecorator = DiscoveryService.createDecorator<
  KafkaConsumerDecoratorConfig
>();

export const Value = (): ParameterDecorator =>
  createParamDecorator(KAFKA_VALUE_META);

export const Headers = (): ParameterDecorator =>
  createParamDecorator(KAFKA_HEADERS_META);

export const Key = (): ParameterDecorator =>
  createParamDecorator(KAFKA_KEY_META);

export const KafkaConsumer = (
  topic: string | string[],
  config?: KafkaConsumerConfig,
): MethodDecorator => {
  const topics = (Array.isArray(topic) ? topic : [topic]).filter(Boolean);

  if (!topics.length) {
    throw new Error('No topics provided');
  }

  return (
    target: object,
    key: string | symbol,
    descriptor: PropertyDescriptor,
  ) => {
    const orig: Function = descriptor.value;

    descriptor.value = function(data: KafkaConsumerPayload): unknown {
      const [keys, values, headers] = [
        Reflect.getOwnMetadata(KAFKA_KEY_META, target, key) ?? [],
        Reflect.getOwnMetadata(KAFKA_VALUE_META, target, key) ?? [],
        Reflect.getOwnMetadata(KAFKA_HEADERS_META, target, key) ?? [],
      ].map((idx: number[]) => new Set<number>(idx));

      const args = Array.from({ length: orig.length }, (_, i) => {
        if (keys.has(i)) { return data.message.key?.toString(); }
        if (values.has(i)) { return data.message.value; }
        if (headers.has(i)) { return data.message.headers; }
        return data;
      });

      return orig.apply(this, args) as unknown;
    };

    applyDecorators(ConsumerDecorator({ topics, ...config }))(
      target,
      key,
      descriptor,
    );

    copyMeta(orig, descriptor.value as Function);
  };
};
