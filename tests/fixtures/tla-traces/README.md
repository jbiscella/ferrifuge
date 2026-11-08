TLA+ model traces: the traces centrifugal/centrifuge-rust generated from the
Centrifugal SDK TLA+ model, copied byte for byte from
https://github.com/centrifugal/centrifuge-rust, commit
88d6edf178f6db370d8e6756d389c89bf100436b, path
`crates/centrifuge-client-core/tests/tla/` (model commit dd28e7f9, per
`upstream/traces/README.md`). Their relative paths are kept under
`upstream/`. MIT license, copyright notice in `LICENSE`. Replayed into the
sans-IO core by `tests/tla_replay.rs` (Block 42); nothing else of that
repository is read.

The traces check the scheduling of connections, subscriptions, tokens,
getState and server-side subscriptions. They deliver 1 live publication and
none as a delta, and they send no refresh or sub_refresh, so delivery,
recovery and delta stay with Block 16's runners, and optimistic subscribes
with Block 36's tests (Block 42, D7).

SHA-256 of each copied file, checked by the replay:

d9ffd47499780adba77afa34ee822d5760effe68df614db42ca3858693e4e145  bugs.jsonl
a2797b57004795cefcf7f67aef0e66bb9cc93f3a77ae39dc3781dea5d4f866a5  traces/MC_close.jsonl
3d08f7a3a52e4a25f0cecccf41a23aea7f0bdf9c0ff56c6c16809be3db15548e  traces/MC_delta.jsonl
33babcfa09f987da23602bc95c6fd9864989d96c07e347ea8082557851caab85  traces/MC_emulation_errors.jsonl
882f145703b7eba05ef97d4c6a48bb07e8a91b63f62ad9384f07eec6bdaa69ad  traces/MC_emulation.jsonl
b3c44ab7e74f49589e47090e1e0511bdc9e240e3618cc884ebd4f41b85415b27  traces/MC_epochs.jsonl
a520ce210e2eb8e1f4b527a2a4d5862598c00e72b393e7542ec5b5e1bf5ef11d  traces/MC_errors.jsonl
b26ba845ece996c3309384d37c2740cf5a880e9da463b8804c1f293c49ed576c  traces/MC_get_state.jsonl
fbc345b0aef2e48db729c054197dffd9dddcbbde949837df724ea3fa13aa1f31  traces/MC_invalidate.jsonl
0ac538c00a8ac47aabcd75ac485125eadbd92bd7976342754dce106597cd6d12  traces/MC_large_emulation.jsonl
ddc6f17eeebe21100615663331f5d02ef189c26fa8c58ab68541ee2e75f7692c  traces/MC_large.jsonl
90a162db329eaab972aa736b0a05171eb61edaa94be00f616fd877ac36a36540  traces/MC_not_recoverable.jsonl
c406b2766e07b838b69214211203c6619886df6e25dcc292d84d47e5ab313cae  traces/MC_refresh.jsonl
73dc653006a5bc1ce82053af300887e05ff253d992655744d4499bb74d0e6675  traces/MC_replace.jsonl
2aceee3efa8ba3c1e7e6b2fa6c8e4a0fada1bb6ec4d8eb7c4345c1e0c8e02870  traces/MC_server_subs_disconnect.jsonl
4dd27c9bf99515158bfffe243ede1d14a06ef483ee7c9a23e6872ca6dab156b4  traces/MC_server_subs.jsonl
1ed32481e30c01cb8a48f1c4ed4d23e1aae696fcabf9f516ddf7f37bb1139584  traces/MC_small.jsonl
718b1b0dc252e1421a7f0ffeb55e9ff6e602f6fa27e7bcaee929ae7ed37dadbf  traces/MC_tokens.jsonl
56b80c826dcf8f357625a8122dd04d9a4e07bc387b3f92c205eff362125d5586  traces/MC_two_channels.jsonl
00f6472385844935e2766eb5f7777b01f1909c189d86f623567734fcbf3402b6  traces/README.md
