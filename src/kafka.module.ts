import { DynamicModule, Logger, Module, Provider } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';

import { KafkaAdminService } from './kafka-admin.service';
import { KafkaConfigService } from './kafka-config.service';
import { KafkaModuleConfig } from './kafka-module.config';
import { KafkaRegistryService } from './kafka-registry.service';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import { ConsumerCreateInput } from './kafka.consumer';
import {
  KafkaAsyncConfig,
  KafkaConfig as IKafkaConfig,
  KafkaConsumerConfig,
} from './kafka.interfaces';
import { KafkaService } from './kafka.service';

type FeatureConfig = Omit<KafkaConsumerConfig, 'fromBeginning'>;
type Feature = string;
type ForFeature = FeatureConfig | FeatureConfig[] | Feature | Feature[];

const providers = [
  KafkaAdminService,
  KafkaRegistryService,
  KafkaService,
  KafkaConfigService,
];

const toExport = [
  KafkaAdminService,
  KafkaRegistryService,
  KafkaService,
];

@Module({
  imports: [DiscoveryModule],
  providers: [
    {
      provide: KAFKA_CONFIG_TOKEN,
      useFactory: (): IKafkaConfig => {
        const conf = KafkaModuleConfig.getConfig();
        const logger = new Logger('Kafka');

        return {
          ...conf,
          logCreator: () => (entity) => {
            logger.log(entity);
          },
        };
      },
    },
    ...providers,
  ],
  exports: toExport,
})
export class KafkaModule {
  public static forRoot(config: IKafkaConfig): DynamicModule {
    return {
      module: KafkaModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: KAFKA_CONFIG_TOKEN,
          useValue: config,
        },
        ...providers,
      ],
      exports: toExport,
    };
  }

  public static forRootAsync(options: KafkaAsyncConfig): DynamicModule {
    const imports = options.imports ?? [];
    if (!imports.includes(DiscoveryModule)) {
      imports.push(DiscoveryModule);
    }

    return {
      module: KafkaModule,
      global: options.global,
      imports,
      providers: [
        ...options.providers ?? [],
        {
          provide: KAFKA_CONFIG_TOKEN,
          inject: options.inject ?? [],
          useFactory: options.useFactory,
        },
        ...providers,
      ],
      exports: toExport,
    };
  }

  public static forFeature(input: ForFeature): DynamicModule {
    const configMap = (Array.isArray(input) ? input : [input])
      .reduce(
        (acc, item) => {
          const conf = typeof item === 'string'
            ? { groupId: item }
            : item;

          return acc.set(conf.groupId, conf);
        },
        new Map<string, FeatureConfig>(),
      );

    const providers: Provider[] = [...configMap.entries()]
      .map(([groupId, config]) => ({
        provide: groupId,
        inject: [KafkaConfigService, KafkaRegistryService],
        useFactory: (
          configService: KafkaConfigService,
        ): ConsumerCreateInput => {
          const conf = configService.composeConsumerConfig(config);
          KafkaRegistryService.addConsumerGroup(groupId, conf);
          return conf;
        },
      }));

    return {
      module: KafkaModule,
      providers,
    };
  }
}
