import { ClientConfig } from 'pg'

export interface Options {
  reconnectMaxRetries?: number;
  reconnectDelay?: number;
  maxPayloadSize?: number;
  maxEmitRetries?: number;
  emitThrottleDelay?: number;
  continuousEmitFailureThreshold?: number;
  queueSize?: number;
  emulateMqEmitterApi?: boolean
  db: ClientConfig;
}

export interface Message {
  topic: string
  payload: string | object
}

declare class PGPubSub {
  constructor (opts: Options);

  emit (message: Message): Promise<void>;
  emit (message: Message, callback: () => void): void;

  on (topic: string, listener: (params: { payload: string | object }, callback?: () => void) => void, callback: () => void): void;
  on (topic: string, listener: (params: { payload: string | object }, callback?: () => void) => void): Promise<void>;

  removeListener (topic: string, listener: (params: { payload: string | object }, callback?: () => void) => void, callback: () => void): void;
  removeListener (topic: string, listener: (params: { payload: string | object }, callback?: () => void) => void): Promise<void>;

  connect (): Promise<void>;

  close (): Promise<void>;
}

export default PGPubSub
