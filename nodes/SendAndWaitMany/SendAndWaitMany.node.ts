import {
	NodeConnectionTypes,
	NodeOperationError,
	type IDataObject,
	type IExecuteFunctions,
	type ILoadOptionsFunctions,
	type INodeExecutionData,
	type INodePropertyOptions,
	type INodeType,
	type INodeTypeDescription,
	type IWebhookFunctions,
	type IWebhookResponseData,
} from 'n8n-workflow';

import {
	clearExpiredExecutionStates,
	getExecutionState,
	removeExecutionState,
	upsertExecutionState,
} from './cache';

const CLAY_TABLE_NAME = 'Clay Tables';
const CLAY_URL_COLUMN_NAME = 'url';
const CLAY_SLUG_COLUMN_NAME = 'name';

type N8nApiCredentials = {
	apiKey: string;
	baseUrl: string;
};

type DataTableSummary = {
	id: string;
	name: string;
	columns?: Array<{
		id: string;
		name: string;
		type: string;
	}>;
};

export class SendAndWaitMany implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Send and Wait Many',
		name: 'sendAndWaitMany',
		icon: 'file:clay.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["requestUrl"]}}',
		usableAsTool: true,
		description: 'Fan out requests and resume only after all callbacks are received',
		defaults: {
			name: 'Send and Wait Many',
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'sendAndWaitManyN8nApi',
				required: true,
			},
		],
		webhooks: [
			{
				name: 'default',
				httpMethod: 'POST',
				path: '',
				responseMode: 'onReceived',
				restartWebhook: true,
			},
		],
		properties: [
			{
				displayName: 'Request URL Name or ID',
				name: 'requestUrl',
				type: 'options',
				typeOptions: {
					loadOptionsMethod: 'getClayUrls',
				},
				default: '',
				required: true,
				description:
					'Choose from the list, or specify an ID using an <a href="https://docs.n8n.io/code/expressions/">expression</a>',
			},
			{
				displayName: 'Callback Field Name',
				name: 'callbackFieldName',
				type: 'string',
				default: 'resume_url',
				required: true,
				description: 'Field name to send callback URL in each outbound request body',
			},
			{
				displayName: 'Item Field Name',
				name: 'itemFieldName',
				type: 'string',
				default: 'data',
				required: true,
				description: 'Field name that contains each input item JSON in outbound request body',
			},
			{
				displayName: 'Max Wait Minutes',
				name: 'maxWaitMinutes',
				type: 'number',
				default: 1440,
				typeOptions: {
					minValue: 1,
				},
				description: 'Maximum time to wait before the sleeping execution times out',
			},
			{
				displayName: 'Warning',
				name: 'warning',
				type: 'notice',
				default:
					'State is stored in process memory. This works reliably only in single-process deployments. Use an external store (for example Redis) for clustered or multi-instance setups.',
			},
		],
	};

	methods = {
		loadOptions: {
			async getClayUrls(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials<N8nApiCredentials>('sendAndWaitManyN8nApi');
				const baseUrl = credentials.baseUrl.replace(/\/$/, '');
				const headers = {
					'X-N8N-API-KEY': credentials.apiKey,
				};

				const clayTable = await findOrCreateClayTable.call(this, baseUrl, headers);
				const rows = await getClayTableRows.call(this, baseUrl, headers, clayTable.id);

				const options: INodePropertyOptions[] = [];

				for (const [index, row] of rows.entries()) {
					const urlValue =
						typeof row[CLAY_URL_COLUMN_NAME] === 'string' ? row[CLAY_URL_COLUMN_NAME] : '';
					const slugValue =
						typeof row[CLAY_SLUG_COLUMN_NAME] === 'string' ? row[CLAY_SLUG_COLUMN_NAME] : '';

					if (!urlValue) {
						continue;
					}

					options.push({
						name: slugValue || `URL ${index + 1}`,
						value: urlValue,
					});
				}

				return options;
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		clearExpiredExecutionStates();

		const items = this.getInputData();
		const requestUrl = this.getNodeParameter('requestUrl', 0) as string;
		const callbackFieldName = this.getNodeParameter('callbackFieldName', 0) as string;
		const itemFieldName = this.getNodeParameter('itemFieldName', 0) as string;
		const maxWaitMinutes = this.getNodeParameter('maxWaitMinutes', 0) as number;

		if (!requestUrl) {
			throw new NodeOperationError(this.getNode(), 'Request URL is required.');
		}

		const executionId = this.getExecutionId();
		if (!executionId) {
			throw new NodeOperationError(this.getNode(), 'Execution ID is not available.');
		}

		const resumeUrl = this.evaluateExpression('{{$execution.resumeUrl}}', 0) as string;
		if (!resumeUrl) {
			throw new NodeOperationError(this.getNode(), 'Resume URL is not available.');
		}

		const totalItems = items.length;
		upsertExecutionState(executionId, totalItems);

		await Promise.all(
			items.map(async (item, index) => {
				const callbackUrl = `${resumeUrl}?part=${index}&total=${totalItems}`;

				const body: IDataObject = {
					[itemFieldName]: item.json,
					[callbackFieldName]: callbackUrl,
				};

				await this.helpers.httpRequest({
					method: 'POST',
					url: requestUrl,
					body,
					json: true,
				});
			}),
		);

		const waitTill = new Date(Date.now() + maxWaitMinutes * 60 * 1000);
		await this.putExecutionToWait(waitTill);

		return [items];
	}

	async webhook(this: IWebhookFunctions): Promise<IWebhookResponseData> {
		clearExpiredExecutionStates();

		const executionId = this.getExecutionId();
		if (!executionId) {
			return {
				webhookResponse: {
					status: 400,
					body: { message: 'Missing execution ID.' },
				},
			};
		}

		const request = this.getRequestObject();
		const query = request.query as Record<string, string | string[] | undefined>;
		const partParam = Array.isArray(query.part) ? query.part[0] : query.part;
		const totalParam = Array.isArray(query.total) ? query.total[0] : query.total;

		const partIndex = Number.parseInt(partParam ?? '', 10);
		const totalFromQuery = Number.parseInt(totalParam ?? '', 10);

		if (Number.isNaN(partIndex) || partIndex < 0) {
			return {
				webhookResponse: {
					status: 400,
					body: { message: 'Invalid or missing part query parameter.' },
				},
			};
		}

		if (Number.isNaN(totalFromQuery) || totalFromQuery < 1) {
			return {
				webhookResponse: {
					status: 400,
					body: { message: 'Invalid or missing total query parameter.' },
				},
			};
		}

		const incomingData = this.getBodyData();
		const state =
			getExecutionState(executionId) ?? upsertExecutionState(executionId, totalFromQuery);
		state.expectedParts = Math.max(state.expectedParts, totalFromQuery);
		state.parts.set(partIndex, incomingData);

		if (state.parts.size >= state.expectedParts) {
			const sortedEntries = [...state.parts.entries()].sort(([a], [b]) => a - b);
			removeExecutionState(executionId);

			return {
				workflowData: [
					sortedEntries.map(([, data]) => ({
						json: data,
					})),
				],
				webhookResponse: {
					status: 200,
					body: {
						message: 'All parts received. Resuming workflow execution.',
						received: state.parts.size,
						expected: state.expectedParts,
					},
				},
			};
		}

		return {
			webhookResponse: {
				status: 200,
				body: {
					message: 'Part received. Waiting for remaining callbacks.',
					received: state.parts.size,
					expected: state.expectedParts,
					currentPart: partIndex,
				},
			},
		};
	}
}

async function listDataTables(
	this: ILoadOptionsFunctions,
	baseUrl: string,
	headers: Record<string, string>,
): Promise<DataTableSummary[]> {
	const pageSize = 250;
	let cursor: string | undefined;
	const tables: DataTableSummary[] = [];

	while (true) {
		const response = (await this.helpers.httpRequest({
			method: 'GET',
			url: `${baseUrl}/data-tables`,
			headers,
			qs: {
				limit: pageSize,
				...(cursor ? { cursor } : {}),
			},
			json: true,
		})) as {
			data: DataTableSummary[];
			nextCursor?: string;
		};

		tables.push(...response.data);

		if (!response.nextCursor) {
			break;
		}

		cursor = response.nextCursor;
	}

	return tables;
}

async function findOrCreateClayTable(
	this: ILoadOptionsFunctions,
	baseUrl: string,
	headers: Record<string, string>,
): Promise<DataTableSummary> {
	const tables = await listDataTables.call(this, baseUrl, headers);
	let clayTable = tables.find((table) => table.name === CLAY_TABLE_NAME);

	if (!clayTable) {
		clayTable = (await this.helpers.httpRequest({
			method: 'POST',
			url: `${baseUrl}/data-tables`,
			headers,
			body: {
				name: CLAY_TABLE_NAME,
				columns: [
					{ name: CLAY_SLUG_COLUMN_NAME, type: 'string' },
					{ name: CLAY_URL_COLUMN_NAME, type: 'string' },
				],
			},
			json: true,
		})) as DataTableSummary;
	}

	return clayTable;
}

async function getClayTableRows(
	this: ILoadOptionsFunctions,
	baseUrl: string,
	headers: Record<string, string>,
	tableId: string,
): Promise<Array<Record<string, unknown>>> {
	const pageSize = 250;
	let cursor: string | undefined;
	const rows: Array<Record<string, unknown>> = [];

	while (true) {
		const response = (await this.helpers.httpRequest({
			method: 'GET',
			url: `${baseUrl}/data-tables/${tableId}/rows`,
			headers,
			qs: {
				limit: pageSize,
				...(cursor ? { cursor } : {}),
			},
			json: true,
		})) as {
			data: Array<Record<string, unknown>>;
			nextCursor?: string;
		};

		rows.push(...response.data);

		if (!response.nextCursor) {
			break;
		}

		cursor = response.nextCursor;
	}

	return rows;
}
