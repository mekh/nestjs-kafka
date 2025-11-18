import { IHeaders, KafkaMessage as IKafkaMessage, Message } from 'kafkajs';

import {
  Headers,
  KafkaMessage,
  KafkaSendInput,
  KafkaSendInputMessage,
  Value,
} from './kafka.interfaces';

export class KafkaSerde<T extends Value = Value> {
  public serialize(data: KafkaSendInput): Message[] {
    return Array.isArray(data.messages)
      ? data.messages.map((msg) => this.serializeMessage(msg))
      : [this.serializeMessage(data.messages)];
  }

  public deserialize(message: IKafkaMessage): KafkaMessage<T>;
  public deserialize(message: IKafkaMessage[]): KafkaMessage<T>[];
  public deserialize(
    message: IKafkaMessage | IKafkaMessage[],
  ): KafkaMessage<T> | KafkaMessage<T>[] {
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
  ): T | undefined {
    const str = value?.toString();
    if (!str) {
      return;
    }

    let parsed: T | undefined;
    try {
      parsed = JSON.parse(str);
    } catch {
      //
    }

    return parsed;
  }

  protected parseHeaders(headers?: IHeaders): Headers | undefined {
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
