// Loads the shipped Floor's illustrative Demo fixtures (window.EXO_DASHBOARD_DEMO_FIXTURES)
// from the page source, so tests exercise the real scenarios rather than a copy.
import { readFile } from "node:fs/promises";
import { runInNewContext } from "node:vm";

export async function loadFloorDemoFixtures() {
  const html = await readFile(new URL("../../../../../docs/design/exomachina-floor.html", import.meta.url), "utf8");
  const start = html.indexOf("function B(){");
  const end = html.indexOf("const Floor = (() =>");
  if (start < 0 || end < 0 || end < start) throw new Error("Demo fixture source was not found in the Floor page");
  return runInNewContext(`${html.slice(start, end)}\nFACTORIES;`, { Math, Date, Number, String, Object, Array, JSON, Infinity, Map, Set });
}
