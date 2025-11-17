import { DynamicModule } from '@nestjs/common';

import { KafkaRegistryService } from './kafka-registry.service';
import { KafkaConsumerConfig } from './kafka.interfaces';

type FeatureConfig = Omit<KafkaConsumerConfig, 'fromBeginning'>;
type Feature = string;
type ForFeature = FeatureConfig | FeatureConfig[] | Feature | Feature[];

export class KafkaConsumersModule {
  public static register(input: ForFeature): DynamicModule {
    (Array.isArray(input) ? input : [input])
      .forEach(
        (item) => {
          const conf = typeof item === 'string'
            ? { groupId: item }
            : item;

          KafkaRegistryService.addConsumerGroup(conf.groupId, conf);
        },
      );

    return {
      module: KafkaConsumersModule,
    };
  }
}
