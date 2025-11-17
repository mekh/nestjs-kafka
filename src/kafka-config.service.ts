import { Inject, Injectable, Logger } from '@nestjs/common';

import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { ConsumerCreateInput } from './kafka.consumer';
import {
  ConsumerConfig,
  KafkaConfig,
  KafkaConsumerConfig,
  RunConfig,
} from './kafka.interfaces';

@Injectable()
export class KafkaConfigService {
  private readonly logger = new Logger(KafkaConfigService.name);

  private readonly defaultConsumerConfig?: Omit<KafkaConsumerConfig, 'groupId'>;

  constructor(@Inject(KAFKA_CONFIG_TOKEN) config: KafkaConfig) {
    this.defaultConsumerConfig = config.consumer;
  }

  public composeConsumerConfig(
    flatConfig: KafkaConsumerConfig,
  ): ConsumerCreateInput {
    const {
      batch = true,
      autoCommit = true,
      autoCommitInterval = null,
      autoCommitThreshold = null,
      partitionsConsumedConcurrently = 1,
      ...consumerConfig
    } = flatConfig;

    const runConfig: Required<RunConfig> = {
      batch,
      autoCommit,
      autoCommitInterval,
      autoCommitThreshold,
      eachBatchAutoResolve: batch && autoCommit,
      partitionsConsumedConcurrently,
    };

    const consumer = {
      ...this.defaultConsumerConfig,
      ...consumerConfig,
    };

    if (!this.isConsumerConfig(consumer)) {
      throw new Error('Invalid consumer configuration');
    }

    return {
      consumerConfig: consumer,
      runConfig,
    };
  }

  private isConsumerConfig(
    config: KafkaConsumerConfig,
  ): config is ConsumerConfig {
    if (!config.groupId) {
      this.logger.error(
        // eslint-disable-next-line max-len
        'The groupId was not provided neither in the module config nor in the decorator.',
      );

      return false;
    }

    return true;
  }
}
