import { DynamicModule, Logger, Module } from '@nestjs/common';
import { DiscoveryModule } from '@nestjs/core';

import { KafkaAdminService } from './kafka-admin.service';
import { KafkaRegistryService } from './kafka-registry.service';
import { KafkaDefaultConfig } from './kafka.config';
import { KAFKA_CONFIG_TOKEN } from './kafka.constants';
import {
  KafkaAsyncOptions,
  KafkaConfig as IKafkaConfig,
} from './kafka.interfaces';
import { KafkaService } from './kafka.service';

@Module({
  imports: [DiscoveryModule],
  providers: [
    {
      provide: KAFKA_CONFIG_TOKEN,
      useFactory: (): IKafkaConfig => {
        const conf = KafkaDefaultConfig.getConfig();
        const logger = new Logger('Kafka');

        return {
          ...conf,
          logCreator: () => (entity) => {
            logger.log(entity);
          },
        };
      },
    },
    KafkaAdminService,
    KafkaRegistryService,
    KafkaService,
  ],
  exports: [KafkaAdminService, KafkaRegistryService, KafkaService],
})
export class KafkaModule {
  public static forRoot(config: IKafkaConfig): DynamicModule {
    return {
      module: KafkaModule,
      providers: [
        {
          provide: KAFKA_CONFIG_TOKEN,
          useValue: config,
        },
        KafkaAdminService,
        KafkaRegistryService,
        KafkaService,
      ],
      exports: [KafkaAdminService, KafkaRegistryService, KafkaService],
    };
  }

  public static forRootAsync(options: KafkaAsyncOptions): DynamicModule {
    return {
      module: KafkaModule,
      imports: options.imports ?? [],
      providers: [
        {
          provide: KAFKA_CONFIG_TOKEN,
          useFactory: options.useFactory,
          inject: options.inject ?? [],
        },
        KafkaAdminService,
        KafkaRegistryService,
        KafkaService,
      ],
      exports: [KafkaAdminService, KafkaRegistryService, KafkaService],
    };
  }
}
