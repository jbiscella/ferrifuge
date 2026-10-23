// centrifuge-js side of the interop test (tests/interop.rs): connects to
// the server with the JSON protocol and publishes `count` publications to
// `channel`, the n-th being {"n":n,"text":"héllo ✓ n"}.
import { Centrifuge } from 'centrifuge';

const [url, channel, count] = process.argv.slice(2);
const centrifuge = new Centrifuge(url);
centrifuge.connect();
await centrifuge.ready(10000);
for (let n = 0; n < Number(count); n++) {
  await centrifuge.publish(channel, { n, text: `héllo ✓ ${n}` });
}
centrifuge.disconnect();
