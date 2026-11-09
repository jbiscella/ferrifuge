// Checks the imports of a wasm module (Block 44, D19):
//   node check.mjs <module.wasm> none      the core alone: no import
//   node check.mjs <module.wasm> bindgen   only wasm-bindgen's modules
import { readFileSync } from "node:fs";

const [path, expected] = process.argv.slice(2);
const module = new WebAssembly.Module(readFileSync(path));
const imports = WebAssembly.Module.imports(module);
const allowed = new Set(["__wbindgen_placeholder__", "__wbindgen_externref_xform__"]);
const wrong =
  expected === "none"
    ? imports
    : imports.filter((entry) => !allowed.has(entry.module));
for (const entry of wrong) {
  console.error(`unexpected import: ${entry.module}.${entry.name} (${entry.kind})`);
}
console.log(`${path}: ${imports.length} imports, ${wrong.length} unexpected`);
process.exit(wrong.length === 0 ? 0 : 1);
