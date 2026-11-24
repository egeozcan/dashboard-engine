# Dashboard Engine

A self-hosted, single-user engine that composes Luau-defined connectors, data, actions and widgets into reactive dashboards, operable by both a human and autonomous agents.

## Distribution and configuration

**Package**:
A Git-versioned distribution unit containing one or more Definitions plus their libraries, tests and assets.
_Avoid_: Plugin

**Definition**:
A named, exported building block of one category (connector, data provider, action provider, adapter, widget provider, library, automation) declared in Luau, identified as `package.definition` and versioned by the content digest of its resolved sources.
_Avoid_: Plugin, plugin definition

**Instance**:
A configured use of a Definition, with a stable ID, input bindings, configuration, secret references and granted capabilities.
_Avoid_: Plugin instance, node

**Workspace**:
The bound Git repository holding the canonical configuration: Instances, dashboards, package dependencies and locally authored Packages.
_Avoid_: Instance repository, project

**Installation**:
One running `dash` daemon together with its data directory and bound Workspace.
_Avoid_: Instance, deployment, server

## Reactivity

**Signal**:
The single reactive delivery primitive: a typed, latest-value stream offering a snapshot and optional keyed deltas, which may coalesce intermediate values.
_Avoid_: State channel, stream

**State**:
A persisted, scoped, typed value (ephemeral, client, dashboard or instance scope) that is exposed to consumers as a Signal.
_Avoid_: State channel, store

**Query result**:
The derived value produced by a data-provider Instance for one parameter set, delivered to consumers as a Signal.
_Avoid_: Data, feed

**Event**:
A discrete, identified, contract-declared occurrence emitted by an action execution, data provider or connector, optionally delivered durably; unlike a Signal it is never coalesced.
_Avoid_: Message, notification

**Request**:
A typed message from a widget or Adapter asking the host to do something (e.g. invoke an action); it carries no authority and is not an Event.
_Avoid_: Widget event, command

## Change lifecycle

**Revision**:
One commit of the Workspace.
_Avoid_: Version, generation

**Generation**:
One activated runtime graph built from a Revision.
_Avoid_: Revision, graph version

**Draft transaction**:
An isolated set of edits against a base Revision, validated and previewed before becoming one Revision and one Generation.
_Avoid_: Draft branch, changeset

**Preview**:
The isolated runtime in which a Draft transaction's graph runs against mocks before activation.
_Avoid_: Staging, sandbox

**Mock**:
A connector-category Definition that stands in for a named real connector during Preview, loading fixtures as plain data.
_Avoid_: Stub, fake connector

**Definition test**:
A Luau test shipped alongside a Definition and run against Mocks.
_Avoid_: Unit test, plugin test

**Scenario test**:
A declarative Workspace-level test run in a Preview that drives Requests, time, Events and failures and asserts on outcomes.
_Avoid_: Flow test, integration test, e2e test

**Dry run**:
An action's advisory estimate of its effects and feasibility, produced without performing side effects.
_Avoid_: Preview, simulation

## Presentation

**Dashboard**:
A Workspace document with a stable ID composed of a root layout, the Adapter Instances placed in it, and Administrator layout overrides; it is configuration, not an Instance.
_Avoid_: Board, page, view

**Widget node**:
A node in an Adapter-generated widget tree, with a stable ID derived from its Adapter Instance and node key, rendered by a widget provider; never an Instance.
_Avoid_: Widget instance, component

**Client**:
One browser profile on one device, identified by a locally stored ID; all its tabs share Client State.
_Avoid_: Tab, session, device

**Adapter**:
An Instance that turns Query results, Dashboard State and actions into a widget tree for one dashboard; it is evaluated per dashboard, not per client, and holds no state of its own.
_Avoid_: View model, controller

## Authority

**Principal**:
The identity on whose behalf an operation runs and against whose ceiling its authority is checked.
_Avoid_: User, caller

**Administrator**:
The single human Principal, acting through a browser session or the CLI.
_Avoid_: User, owner, admin user

**Agent**:
A Principal bound to one scoped, revocable MCP token.
_Avoid_: Bot, client, MCP user

**Service principal**:
An administrator-granted Principal with its own capability ceiling, under which background work (automations, history sampling, polling, durable event consumers) runs.
_Avoid_: System user, bot
