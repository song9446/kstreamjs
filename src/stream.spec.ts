import {StreamContext, Message, Statistics} from './context';
import {createStream} from './stream';

function mockMessages<T>(
  values: T[],
  timestampes: number[] = []
): Message<T>[] {
  return values.map((value, i) => ({
    metadata: {
      topic: '',
      partition: 0,
      message: {
        timestamp: timestampes[i] || 0,
        key: Buffer.from('', 'utf8'),
        size: 0,
        attributes: 0,
        offset: '0',
      },
    },
    value,
  }));
}

describe('with mock kafka context', () => {
  let mockData = mockMessages<unknown>([{a: 1}]);
  beforeAll(() => {
    jest.mock('./context');
    StreamContext.prototype.receive = jest.fn().mockImplementation(async () => {
      return mockData.splice(0, 1);
    });
    StreamContext.prototype.flushStatistics = jest
      .fn()
      .mockImplementation(() => {
        const s = new Statistics();
        s.recvTotal = 1;
        s.sendTotal = 1;
        return s;
      });
  });
  afterAll(() => {
    jest.clearAllMocks();
  });
  function createStreamHelper<T>() {
    return createStream<T>({
      brokers: ['a'],
      inputTopic: 'in-topic',
      clientId: 'client-id',
    });
  }
  it('creates identical stream', async () => {
    mockData = mockMessages([{a: 1}]);
    const stream = createStreamHelper();
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 1}}];
    expect(msgs).toMatchObject(expected);
  });
  it('maps stream', async () => {
    const data = {a: 1};
    mockData = mockMessages([data]);
    const stream = createStreamHelper<typeof data>().map(i => ({
      a: i['a'] + 1,
    }));
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 2}}];
    expect(msgs).toMatchObject(expected);
  });
  it('filter stream', async () => {
    const data = [{a: 1}, {a: 2}, {a: 3}, {a: 1}];
    mockData = mockMessages(data);
    const stream = createStreamHelper<typeof data[number]>().filter(
      i => i['a'] > 2
    );
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 3}}];
    expect(msgs).toMatchObject(expected);
  });

  it('explode stream', async () => {
    mockData = mockMessages([[{a: 1}, {b: 2}]]);
    const stream = createStreamHelper<typeof mockData>().explode();
    const msgs = await stream.handleMessages();
    const expected = [{value: {a: 1}}, {value: {b: 2}}];
    expect(msgs).toMatchObject(expected);
  });
  it('aggregates stream by window', async () => {
    const data = [{a: 1}, {a: 2}, {a: 3}, {a: 4}, {a: 5}];
    mockData = mockMessages(data, [0, 1, 2, 3, 4, 5]);
    const stream = createStreamHelper<typeof data[0]>().window({
      from: 0,
      interval: 2,
      collect: msgs => msgs.reduce((acc, i) => i['a'] + acc, 0),
    });
    let msgs = await stream.handleMessages();
    let expected = [{value: 3}];
    expect(msgs).toMatchObject(expected);
    msgs = await stream.handleMessages();
    expected = [{value: 7}];
    expect(msgs).toMatchObject(expected);
  });
  it('union streams', async () => {
    const data = [1, 2, 3, 4];
    mockData = mockMessages(data);
    const stream2 = createStreamHelper<typeof data[0]>().map(i => i * 10);
    const stream = createStreamHelper<typeof data[0]>().union(stream2);
    const msgs = [];
    for (let i = 0; i < 4; ++i) msgs.push(...(await stream.handleMessages()));
    const expected = [{value: 1}, {value: 20}, {value: 3}, {value: 40}];
    expect(msgs).toMatchObject(expected);
  });

  it('apply mix of transform to stream', async () => {
    const data = [{a: 1}, {b: 2}, {c: 3}, {d: 4}, {e: 5}];
    mockData = mockMessages(data, [0, 1, 2, 3, 4, 5]);
    const stream = createStreamHelper<typeof data[number]>()
      .map(msg => Object.assign(msg, {z: 1}))
      .window({
        from: 0,
        interval: 2,
        collect: msgs => {
          return msgs.map(Object.keys).flat();
        },
      })
      .explode()
      .map(k => k + 'a');
    let msgs = await stream.handleMessages();
    let expected = [{value: 'aa'}, {value: 'za'}, {value: 'ba'}, {value: 'za'}];
    expect(msgs).toMatchObject(expected);
    msgs = await stream.handleMessages();
    expected = [{value: 'ca'}, {value: 'za'}, {value: 'da'}, {value: 'za'}];
    expect(msgs).toMatchObject(expected);
  });

  it('flush statistics', async () => {
    const stream1 = createStreamHelper();
    const stream2 = createStreamHelper();
    const stats = stream1.union(stream2).flushStatistics();
    expect(stats).toMatchObject({
      recvTotal: 2,
      sendTotal: 2,
    });
  });
});
