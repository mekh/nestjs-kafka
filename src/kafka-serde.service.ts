import { Injectable } from '@nestjs/common';
import { IHeaders, KafkaMessage as IKafkaMessage, Message } from 'kafkajs';

import {
  KafkaMessage,
  KafkaSendInput,
  KafkaSendInputMessage,
  KafkaSerde,
} from './kafka.interfaces';

@Injectable()
export class KafkaSerdeService implements KafkaSerde {
  public serialize(data: KafkaSendInput): Message[] {
    return Array.isArray(data.messages)
      ? data.messages.map((msg) => this.serializeMessage(msg))
      : [this.serializeMessage(data.messages)];
  }

  public deserialize(message: IKafkaMessage): KafkaMessage;
  public deserialize(message: IKafkaMessage[]): KafkaMessage[];
  public deserialize(
    message: IKafkaMessage | IKafkaMessage[],
  ): KafkaMessage | KafkaMessage[] {
    const isArray = Array.isArray(message);
    const data = isArray ? message : [message];

    const res = data.map((msg) => {
      const value = this.parseMessage(msg.value);
      const headers = this.parseHeaders(msg.headers);
      const key = msg.key?.toString();

      return { ...msg, key, headers, value };
    });

    return isArray ? res : res[0];
  }

  protected serializeMessage(data: KafkaSendInputMessage): Message {
    const value = this.isObj(data.value)
      ? JSON.stringify(data.value)
      : data.value;

    return { ...data, value };
  }

  protected parseMessage(
    value: Buffer | null,
  ): Record<string, any> | undefined {
    const str = value?.toString();
    if (!str) {
      return;
    }

    let parsed: Record<string, any> | undefined;
    try {
      parsed = JSON.parse(str);
    } catch {
      //
    }

    return parsed;
  }

  protected parseHeaders(
    headers?: IHeaders,
  ): Record<string, string | undefined> | undefined {
    if (!headers) {
      return;
    }

    return Object.fromEntries(
      Object.entries(headers).map((
        [key, value],
      ) => [key, value?.toString()]),
    );
  }

  private isObj(
    value: Record<string, any> | Buffer | string | null,
  ): value is Record<string, any> {
    return !!value &&
      !Buffer.isBuffer(value)
      && typeof value !== 'string';
  }
}
