import {
  StreamContext,
  MessageMetadata,
  Message,
  Offset,
  StreamContextOption,
  Statistics,
} from './context.js';

import {
  ElementOf,
  sleep,
  firstPromiseResolveOrSkip,
  partition,
  isAsync,
} from './utils.js';

export class Stream<O> {
  contexts: StreamContext[];
  handleMessages: () => Promise<Message<O>[]>;
  constructor(
    contexts: StreamContext[],
    handleMessages?: () => Promise<Message<O>[]>
  ) {
    this.contexts = contexts;
    if (handleMessages !== undefined) this.handleMessages = handleMessages;
    else if (contexts.length === 1)
      this.handleMessages = async () => {
        const msgs = await this.contexts[0].receive<O>();
        if (msgs === undefined) throw Error('channel is closed');
        else return msgs;
      };
    else
      throw Error(
        'Wrong Stream Initializer Parameters. It must be either of Multiple contexts with handler or single context without handler'
      );
  }
  map<N>(
    next: (value: O, metadata: MessageMetadata) => N | Promise<N>
  ): Stream<N> {
    return new Stream<N>(this.contexts, async () => {
      const messages = await this.handleMessages();
      if (!isAsync(next))
        return messages.map(({value, metadata}) => ({
          value: next(value, metadata) as N,
          metadata,
        }));
      else
        return await Promise.all(
          messages.map(async ({value, metadata}) => ({
            value: await next(value, metadata),
            metadata,
          }))
        );
    });
  }
  filter(
    test: (value: O, metadata: MessageMetadata) => boolean | Promise<boolean>
  ): Stream<O> {
    const lastHandleMessages = this.handleMessages;
    this.handleMessages = async () => {
      let messages: Message<O>[];
      do {
        messages = await lastHandleMessages();
        if (!isAsync(test))
          messages = messages.filter(msg => test(msg.value, msg.metadata));
        else {
          const t = await Promise.all(
            messages.map(msg => test(msg.value, msg.metadata))
          );
          messages = messages.filter((_, i) => t[i]);
        }
      } while (messages.length === 0);
      return messages;
    };
    return this;
  }
  explode<T extends ElementOf<O>>(): Stream<T> {
    return new Stream<T>(this.contexts, async () => {
      const messages = await this.handleMessages();
      //    [ [meta, [1,2]], [meta2, [3,4]] ]
      // => [ [meta, [1]], [meta, [2]], [meta2, [3]], [meta3, [4]] ]
      const newMessages = [];
      for (const {value, metadata} of messages)
        newMessages.push(
          ...(value as unknown as T[]).map(value => ({value, metadata}))
        );
      return newMessages;
    });
  }
  _concatMessages<T>(messages: Message<T>[]): Message<T[]> {
    /* keep first metadata while commit last metdata */
    const offsets = messages.reduce(
      (acc, m) => {
        const key = `${m.metadata.topic}:${m.metadata.partition}`;
        acc[key] = {
          partition: m.metadata.partition,
          topic: m.metadata.topic,
          offset: String(
            Math.max(
              Number(acc[key]?.offset || 0),
              Number(m.metadata.message.offset)
            )
          ),
        };
        return acc;
      },
      {} as Record<string, Offset>
    );
    const contextIds = new Set();
    const contexts = messages
      .map(m =>
        m.metadata.contexts.filter(ctx => {
          if (!contextIds.has(ctx.id)) {
            contextIds.add(ctx.id);
            return true;
          }
          return false;
        })
      )
      .flat();
    return {
      metadata: Object.assign(messages[0].metadata, {
        offsets: Object.values(offsets),
        contexts: contexts,
      }),
      value: messages.map(msg => msg.value),
    };
  }
  window<N>(option: {
    from: number;
    interval: number;
    collect: (msgs: O[]) => N | Promise<N>;
    bufferInterval?: number;
  }): Stream<N> {
    //const messageQueue: Message<O>[] = [];
    const msgQ: Message<O>[] = [];
    const interval = option.interval;
    const collect = option.collect;
    const bufferInterval = option.bufferInterval ?? 1000 * 60;
    let from = option.from;
    let to = from + interval;
    let lastSeeked = -1;
    let seeked = false;
    return new Stream<N>(this.contexts, async () => {
      if (!seeked) {
        seeked = true;
        await Promise.all(this.contexts.map(c => c.seek(from)));
      }
      while (
        msgQ.length === 0 ||
        msgQ[msgQ.length - 1].metadata.message.timestamp < to + bufferInterval
      ) {
        const messages = await this.handleMessages();
        if (messages.length === 0) break;

        const [inbounds, outbounds] = partition(
          messages,
          msg => msg.metadata.message.timestamp >= from
        );

        if (outbounds.length > 0) {
          if (inbounds.length === 0) {
            this.contexts[0].logger.warn(
              'last commit timestamp is too past from the window. skip all messages between the last commit ts and the start of window ts'
            );
            if (lastSeeked !== from) {
              lastSeeked = from;
              await Promise.all(this.contexts.map(c => c.seek(from)));
            }
            continue;
          } else {
            this.contexts[0].logger.debug(
              `${fail.length} messages are behind the target period.`
            );
          }
        }
        msgQ.push(...inbounds);
      }
      const [inbounds, outbounds] = partition(
        msgQ,
        msg => msg.metadata.message.timestamp < to
      );
      from = to;
      to = to + interval;
      if (inbounds.length > 0) {
        msgQ.splice(0, msgQ.length, ...outbounds);
        const messagePack = this._concatMessages(inbounds);
        const maybePromise = collect(messagePack.value);
        const value: N =
          maybePromise instanceof Promise ? await maybePromise : maybePromise;
        return [
          {
            metadata: messagePack.metadata,
            value,
          },
        ];
      } else return [];
    });
  }
  union(other: Stream<O>): Stream<O> {
    const myHandleMessages = this.handleMessages;
    let handleMessages1Fut: Promise<Message<O>[]> | null = null;
    let handleMessages2Fut: Promise<Message<O>[]> | null = null;
    const handleMessages1 = async (): Promise<[number, Message<O>[]]> => {
      if (handleMessages1Fut === null) {
        handleMessages1Fut = myHandleMessages();
      }
      const msgs = await handleMessages1Fut;
      return [1, msgs];
    };
    const handleMessages2 = async (): Promise<[number, Message<O>[]]> => {
      if (handleMessages2Fut === null) {
        handleMessages2Fut = other.handleMessages();
      }
      const msgs = await handleMessages2Fut;
      return [2, msgs];
    };
    this.handleMessages = async () => {
      const [index, msgs] = await firstPromiseResolveOrSkip([
        handleMessages1(),
        handleMessages2(),
      ]);
      if (index === 1) handleMessages1Fut = null;
      else handleMessages2Fut = null;
      return msgs;
    };
    this.contexts.push(...other.contexts);
    return this;
  }
  blackhole(): Stream<O> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      while (this.contexts.every(c => !c.isDisconnected())) {
        await sleep(1000);
      }
      return messages;
    });
  }
  writeTo(topic: string): Stream<O> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      if (messages && messages.length > 0)
        await this.contexts[0].send(topic, messages);
      return messages;
    });
  }
  commit(): Stream<O> {
    return new Stream(this.contexts, async () => {
      const messages = await this.handleMessages();
      if (messages && messages.length > 0)
        await this.contexts[0].updateCommitOffsets(messages);
      return messages;
    });
  }
  flushStatistics(): Statistics {
    return this.contexts
      .map(c => c.flushStatistics())
      .reduce((acc, s) => acc.merge(s));
  }
  async start(): Promise<Stream<O>> {
    await Promise.all(this.contexts.map(c => c.start()));
    while (this.contexts.every(c => !c.isDisconnected())) {
      await this.handleMessages();
    }
    return this;
  }
  async stop(): Promise<void> {
    await Promise.all(this.contexts.map(c => c.disconnect()));
  }
}

export function createStream<T>(option: StreamContextOption) {
  const context = new StreamContext(option);
  return new Stream<T>([context]);
}
