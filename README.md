# n8n-nodes-clay

Community n8n node package that adds the **Clay** node.

## What this node does

The `Clay` node fans out one request per incoming item to a selected Clay URL and waits for all callbacks before resuming workflow execution.

At execution time it:

1. Reads all input items.
2. Sends a `POST` request for each item to the selected request URL.
3. Injects a callback URL into each outbound payload.
4. Waits until all callback parts are received.
5. Returns callback payloads in part order.

## Node behavior

- **Node name in n8n UI**: `Clay`
- **Operation model**: send-and-wait-many (fan-out + webhook resume)
- **Input**: any number of items
- **Output**: one item per callback payload

### Parameters

- **Request URL Name or ID**: Selects a URL from the Clay Tables data table.
- **Item Field Name**: Field name used for each outbound item payload (default: `data`).
- **Options**:
  - **Callback Field Name**: Field name used to pass resume URL (default: `resume_url`).
  - **Max Wait Minutes**: Max wait before timeout (default: `30`).

## Important limitation

Execution state is currently stored in process memory. This is reliable for single-process deployments. For clustered or multi-instance setups, use an external state store (for example Redis).

## Development

### Prerequisites

- Node.js 22+
- npm

### Install

```bash
npm install
```

### Build

```bash
npm run build
```

### Build in watch mode

```bash
npm run build:watch
```

### Lint

```bash
npm run lint
```

## Package metadata

- npm package: `@maxim_chu/n8n-nodes-clay`
- license: MIT
