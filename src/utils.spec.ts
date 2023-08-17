import {isAsync} from './utils.js';

describe('utils', () => {
  it('should guard async functions', async () => {
    expect(isAsync(async () => {})).toBe(true);
    expect(isAsync(() => {})).toBe(false);
  });
});
