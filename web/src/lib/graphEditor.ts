/**
 * Pure helpers for the Dependency Graph Editor (§10.2.2): attach validation issues to nodes
 * and bindings, and compute required-capability status shown inline.
 */
import type { DefinitionSummary, InstanceConfig, ValidationIssue } from "./api";

export interface NodeIssues {
  /** Issues about the Instance itself (not tied to one input). */
  node: ValidationIssue[];
  /** Issues tied to one input slot. */
  inputs: Record<string, ValidationIssue[]>;
}

const INPUT_RE = /input '([^']+)'/;
const BOUND_RE = /cannot bind (?:action|connector) '([^']+)'/;

/** Group a validation report by Instance and, where the message names one, by input slot. */
export function groupIssues(
  issues: ValidationIssue[],
  instances: Record<string, InstanceConfig>
): Record<string, NodeIssues> {
  const out: Record<string, NodeIssues> = {};
  for (const iss of issues) {
    const entry = (out[iss.target] ??= { node: [], inputs: {} });
    let slot: string | undefined = INPUT_RE.exec(iss.message)?.[1];
    if (!slot) {
      const bound = BOUND_RE.exec(iss.message)?.[1];
      const inst = instances[iss.target];
      if (bound && inst) {
        slot = Object.entries(inst.inputs ?? {}).find(([, src]) => src === bound)?.[0];
      }
    }
    if (slot) (entry.inputs[slot] ??= []).push(iss);
    else entry.node.push(iss);
  }
  return out;
}

export interface CapabilityRequirement {
  capability: string;
  /** Input slot that must provide it (`null` for Definition-level requirements). */
  slot: string | null;
  /** Instance expected to grant it. */
  grantedBy: string | null;
  satisfied: boolean;
}

/**
 * Capabilities an Instance needs before activation: per-input `required_capabilities` of
 * connector inputs (granted by the bound connector Instance) plus Definition-level
 * `required_capabilities` (granted by the Instance itself or any bound connector).
 */
export function capabilityRequirements(
  inst: InstanceConfig,
  def: DefinitionSummary | undefined,
  instances: Record<string, InstanceConfig>
): CapabilityRequirement[] {
  const out: CapabilityRequirement[] = [];
  if (!def) return out;
  const boundConnectors = Object.values(inst.inputs ?? {})
    .map((id) => instances[id])
    .filter((i): i is InstanceConfig => Boolean(i) && i.category === "connector");
  for (const [slot, spec] of Object.entries(def.inputs ?? {})) {
    if (spec.ref_kind !== "connector") continue;
    const boundId = inst.inputs?.[slot] ?? null;
    const bound = boundId ? instances[boundId] : undefined;
    for (const cap of spec.required_capabilities ?? []) {
      out.push({
        capability: cap,
        slot,
        grantedBy: boundId,
        satisfied: Boolean(bound?.capabilities?.includes(cap)),
      });
    }
  }
  for (const cap of def.required_capabilities ?? []) {
    const granter =
      (inst.capabilities ?? []).includes(cap)
        ? inst.id
        : boundConnectors.find((c) => (c.capabilities ?? []).includes(cap))?.id ?? null;
    out.push({ capability: cap, slot: null, grantedBy: granter, satisfied: granter !== null });
  }
  return out;
}

const SLOT_KIND: Record<string, string> = {
  connector: "connector",
  data_provider: "data",
  action_provider: "action",
};

/**
 * Input slot to bind when dragging `source` onto `target`: the first declared, unbound input
 * whose kind matches the source category, else the first declared input of that kind.
 */
export function pickInputSlot(
  source: InstanceConfig,
  target: InstanceConfig,
  targetDef: DefinitionSummary | undefined
): string | null {
  const kind = SLOT_KIND[source.category];
  if (!kind || !targetDef) return null;
  const candidates = Object.entries(targetDef.inputs ?? {}).filter(([, s]) => s.ref_kind === kind);
  const free = candidates.find(([slot]) => !(slot in (target.inputs ?? {})));
  return (free ?? candidates[0])?.[0] ?? null;
}
