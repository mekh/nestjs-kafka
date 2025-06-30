import { applyDecorators } from '@nestjs/common';
import { DiscoveryService } from '@nestjs/core';

import {
  KafkaConsumerConfig,
  KafkaConsumerDecoratorConfig,
} from './kafka.interfaces';

/**
 * For internal usage only (within the Kafka module)
 */
export const ConsumerDecorator = DiscoveryService.createDecorator<
  KafkaConsumerDecoratorConfig
>();

export const KafkaConsumer = (
  topic: string | string[],
  config: KafkaConsumerConfig,
): MethodDecorator => {
  const topics = (Array.isArray(topic) ? topic : [topic]).filter(Boolean);

  if (!topics.length) {
    throw new Error('No topics provided');
  }

  return applyDecorators(ConsumerDecorator({ topics, ...config }));
};
