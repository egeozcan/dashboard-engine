import { describe, expect, it } from "vitest";
import {
  completionCandidates,
  filterCompletions,
  identifiersFromLuauTypes,
  parseLuauDiagnostics,
  parseUnifiedDiff,
  tokenBeforeCursor,
} from "./luau";
import { groupIssues, capabilityRequirements, pickInputSlot } from "./graphEditor";
import type { InstanceConfig } from "./api";

describe("Package Studio language helpers", () => {
  it("offers completions from generated Luau types, inputs and the dash SDK", () => {
    const types = "-- Auto-generated\nexport type QueryParams = { repository: string, state: string? }\n";
    expect(identifiersFromLuauTypes(types)).toEqual(expect.arrayContaining(["QueryParams", "repository", "state"]));
    const c = completionCandidates({ generatedTypes: types, inputNames: ["linked"] });
    expect(filterCompletions(c, "ctx.inputs.l")).toEqual(["ctx.inputs.linked"]);
    expect(filterCompletions(c, "dash.data_")).toEqual(["dash.data_provider", "dash.data_ref"]);
    expect(filterCompletions(c, "Query")).toContain("QueryParams");
    expect(tokenBeforeCursor("local x = ctx.inp", 17)).toEqual({ token: "ctx.inp", start: 10 });
  });

  it("maps engine errors to files and lines", () => {
    const runtimeErr =
      'luau runtime error: [string "engineering.pr_table_adapter/runtime.luau"]:12: attempt to index nil\nstack traceback:\n\t[string "engineering.pr_table_adapter/runtime.luau"]:12: in function';
    const d = parseLuauDiagnostics(runtimeErr);
    expect(d[0]).toMatchObject({
      file: "runtime",
      line: 12,
      definitionId: "engineering.pr_table_adapter",
      message: "attempt to index nil",
    });
    const declErr =
      "workspace error: declaration '/data/drafts/dft_1/packages/engineering/custom_metric/definition.luau': syntax error: [string \"definition.luau\"]:3: Expected '}'";
    expect(parseLuauDiagnostics(declErr)).toEqual([
      { file: "definition", line: 3, message: "Expected '}'", definitionId: undefined, severity: "error" },
    ]);
    expect(parseLuauDiagnostics("something unrelated")).toEqual([
      { file: null, line: null, message: "something unrelated", severity: "error" },
    ]);
  });

  it("parses unified diffs with line numbers", () => {
    const patch = [
      "diff --git a/x b/x",
      "--- a/x",
      "+++ b/x",
      "@@ -1,2 +1,3 @@",
      " keep",
      "-old",
      "+new",
      "+added",
      "",
    ].join("\n");
    const lines = parseUnifiedDiff(patch).filter((l) => l.kind !== "meta");
    expect(lines.map((l) => [l.kind, l.text, l.oldNo, l.newNo])).toEqual([
      ["hunk", "@@ -1,2 +1,3 @@", null, null],
      ["ctx", "keep", 1, 1],
      ["del", "old", 2, null],
      ["add", "new", null, 2],
      ["add", "added", null, 3],
    ]);
  });
});

describe("Graph Editor helpers", () => {
  const inst = (id: string, category: string, inputs: Record<string, string> = {}, capabilities: string[] = []): InstanceConfig => ({
    id,
    category,
    definition: `pkg.${id}`,
    inputs,
    config: {},
    secrets: {},
    capabilities,
  });
  const instances = {
    gh: inst("gh", "connector", {}, ["pulls:read"]),
    prs: inst("prs", "data_provider", { github: "gh", merge: "merge_pr" }),
    merge_pr: inst("merge_pr", "action_provider"),
  };

  it("attaches validation issues to the node and the offending input", () => {
    const grouped = groupIssues(
      [
        { severity: "error", code: "FORBIDDEN_BINDING", target: "prs", message: "forbidden binding (§2.1): data provider 'prs' cannot bind action 'merge_pr'" },
        { severity: "error", code: "INSUFFICIENT_CAPABILITY", target: "prs", message: "input 'github' requires capability 'pulls:merge' which connector 'gh' does not grant" },
        { severity: "error", code: "CATEGORY_MISMATCH", target: "prs", message: "instance category mismatch" },
      ],
      instances
    );
    expect(grouped.prs.inputs.merge[0].code).toBe("FORBIDDEN_BINDING");
    expect(grouped.prs.inputs.github[0].code).toBe("INSUFFICIENT_CAPABILITY");
    expect(grouped.prs.node.map((i) => i.code)).toEqual(["CATEGORY_MISMATCH"]);
  });

  it("computes required capabilities and whether they are granted", () => {
    const reqs = capabilityRequirements(
      instances.prs,
      {
        package: "pkg",
        category: "data_provider",
        description: "",
        capabilities: [],
        required_capabilities: ["pulls:read"],
        optional_capabilities: [],
        inputs: { github: { ref_kind: "connector", required_capabilities: ["pulls:read", "pulls:merge"] } },
        accepts_requests: [],
      },
      instances
    );
    expect(reqs).toEqual([
      { capability: "pulls:read", slot: "github", grantedBy: "gh", satisfied: true },
      { capability: "pulls:merge", slot: "github", grantedBy: "gh", satisfied: false },
      { capability: "pulls:read", slot: null, grantedBy: "gh", satisfied: true },
    ]);
  });

  it("picks the declared input slot matching the dragged source category", () => {
    const def = {
      package: "pkg",
      category: "adapter",
      description: "",
      capabilities: [],
      required_capabilities: [],
      optional_capabilities: [],
      inputs: {
        linked: { ref_kind: "data" as const },
        merge: { ref_kind: "action" as const },
        extra: { ref_kind: "data" as const },
      },
      accepts_requests: [],
    };
    const adapter = inst("table", "adapter", { linked: "prs" });
    expect(pickInputSlot(instances.merge_pr, adapter, def)).toBe("merge");
    expect(pickInputSlot(instances.prs, adapter, def)).toBe("extra");
    expect(pickInputSlot(instances.gh, adapter, def)).toBeNull();
  });
});
