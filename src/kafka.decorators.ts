import { applyDecorators } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';

import { KafkaBatch } from './kafka.batch';
import {
  KAFKA_HEADERS_META,
  KAFKA_KEY_META,
  KAFKA_VALUE_META,
} from './kafka.constants';
import {
  Headers as IHeaders,
  KafkaConsumerDecoratorConfig,
  KafkaEachMessagePayload,
  Key as IKey,
  Value as IValue,
} from './kafka.interfaces';

interface ArgsData {
  keys: (IKey | undefined)[];
  values: (IValue | undefined)[];
  headers: (IHeaders | undefined)[];
}

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
    const args: number[] = Reflect.getOwnMetadata(metaKey, target, key!) ?? [];

    args.push(idx);

    Reflect.defineMetadata(metaKey, args, target, key!);
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
  groupId: string,
  topic: string | string[],
  config?: { fromBeginning?: boolean },
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
    const origFn: Function = descriptor.value;

    descriptor.value = function(
      data: KafkaEachMessagePayload | KafkaBatch,
    ): unknown {
      const [keysIdx, valuesIdx, headersIdx] = [
        Reflect.getOwnMetadata(KAFKA_KEY_META, target, key),
        Reflect.getOwnMetadata(KAFKA_VALUE_META, target, key),
        Reflect.getOwnMetadata(KAFKA_HEADERS_META, target, key),
      ].map((idx?: number[]) => idx ? new Set<number>(idx) : undefined);

      if (!keysIdx && !valuesIdx && !headersIdx) {
        return origFn.call(this, data) as unknown;
      }

      const isBatch = data instanceof KafkaBatch;
      const messages = isBatch ? data.getMessages() : [data];
      const { keys, values, headers } = messages.reduce<ArgsData>(
        (acc, { message }) => {
          if (keysIdx) { acc.keys.push(message.key); }
          if (valuesIdx) { acc.values.push(message.value); }
          if (headersIdx) { acc.headers.push(message.headers); }
          return acc;
        },
        { keys: [], values: [], headers: [] },
      );

      const args = Array.from({ length: origFn.length }, (_, i) => {
        if (keysIdx?.has(i)) { return isBatch ? keys : keys[0]; }
        if (valuesIdx?.has(i)) { return isBatch ? values : values[0]; }
        if (headersIdx?.has(i)) { return isBatch ? headers : headers[0]; }
        return data;
      });

      return origFn.apply(this, args) as unknown;
    };

    applyDecorators(ConsumerDecorator({ groupId, topics, ...config }))(
      target,
      key,
      descriptor,
    );

    copyMeta(origFn, descriptor.value as Function);
  };
};
