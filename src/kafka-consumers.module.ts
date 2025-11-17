import { DynamicModule, Provider } from '@nestjs/common';

import { KafkaConfigService } from './kafka-config.service';
import { KafkaRegistryService } from './kafka-registry.service';
import { KafkaConsumer } from './kafka.consumer';
import { KafkaConsumerConfig } from './kafka.interfaces';

type FeatureConfig = Omit<KafkaConsumerConfig, 'fromBeginning'>;
type Feature = string;
type ForFeature = FeatureConfig | FeatureConfig[] | Feature | Feature[];

export class KafkaConsumersModule {
  public static register(input: ForFeature): DynamicModule {
    const providers = (Array.isArray(input) ? input : [input])
      .reduce<Provider[]>(
        (acc, item) => {
          const conf = typeof item === 'string'
            ? { groupId: item }
            : item;

          acc.push({
            provide: conf.groupId,
            inject: [KafkaConfigService, KafkaRegistryService],
            useFactory: (
              configSvc: KafkaConfigService,
              registrySvc: KafkaRegistryService,
            ) => {
              const config = configSvc.composeConsumerConfig(conf);

              return KafkaConsumer.create(
                config,
                registrySvc.handle.bind(registrySvc),
              );
            },
          });

          return acc;
        },
        [],
      );

    return {
      module: KafkaConsumersModule,
      providers,
      exports: providers,
    };
  }
}
