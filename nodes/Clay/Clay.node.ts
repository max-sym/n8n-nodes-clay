import {
	NodeConnectionTypes,
	NodeOperationError,
	type IDataObject,
	type IExecuteFunctions,
	type ILoadOptionsFunctions,
	type INodeExecutionData,
	type INodePropertyOptions,
	type ResourceMapperFields,
	type ResourceMapperValue,
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
const CLAY_TABLE_NAME_COLUMN_NAME = 'name';
const CLAY_FIELDS_COLUMN_NAME = 'fields';

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

type HttpRequestOptions = Parameters<ILoadOptionsFunctions['helpers']['httpRequest']>[0];
type HttpRequester = (options: HttpRequestOptions) => Promise<unknown>;

export class Clay implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Clay',
		name: 'clay',
		icon: 'file:../../icons/clay.svg',
		group: ['transform'],
		version: 1,
		subtitle: '',
		usableAsTool: true,
		description: 'Fan out requests and resume only after all callbacks are received',
		defaults: {
			name: 'Clay',
		},
		inputs: [NodeConnectionTypes.Main],
		outputs: [NodeConnectionTypes.Main],
		credentials: [
			{
				name: 'clayN8nApi',
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
				displayName: 'Field Values',
				name: 'fieldValues',
				type: 'resourceMapper',
				typeOptions: {
					resourceMapper: {
						resourceMapperMethod: 'getClayFieldValues',
						mode: 'map',
						valuesLabel: 'Fields',
						addAllFields: true,
						supportAutoMap: true,
						fieldWords: {
							singular: 'field',
							plural: 'fields',
						},
						noFieldsError: 'No fields configured for the selected Clay table URL.',
					},
					loadOptionsDependsOn: ['requestUrl'],
				},
				default: {
					mappingMode: 'defineBelow',
					value: null,
				},
				description: 'Mapped values for fields configured in Clay Tables',
			},
			{
				displayName: 'Options',
				name: 'options',
				type: 'collection',
				default: {},
				placeholder: 'Add option',
				options: [
					{
						displayName: 'Callback Field Name',
						name: 'callbackFieldName',
						type: 'string',
						default: 'resume_url',
						description: 'Field name to send callback URL in each outbound request body',
					},
					{
						displayName: 'Max Wait Minutes',
						name: 'maxWaitMinutes',
						type: 'number',
						default: 30,
						typeOptions: {
							minValue: 1,
						},
						description: 'Maximum time to wait before the sleeping execution times out',
					},
				],
			},
			{
				displayName:
					'State is stored in process memory. This works reliably only in single-process deployments. Use an external store (for example Redis) for clustered or multi-instance setups.',
				name: 'notice',
				type: 'notice',
				default: '',
			},
		],
	};

	methods = {
		loadOptions: {
			async getClayUrls(this: ILoadOptionsFunctions): Promise<INodePropertyOptions[]> {
				const credentials = await this.getCredentials<N8nApiCredentials>('clayN8nApi');
				const baseUrl = credentials.baseUrl.replace(/\/$/, '');
				const headers = {
					'X-N8N-API-KEY': credentials.apiKey,
				};
				const httpRequest = this.helpers.httpRequest.bind(this.helpers) as HttpRequester;

				const clayTable = await findOrCreateClayTable(baseUrl, headers, httpRequest);
				const rows = await getClayTableRows(baseUrl, headers, clayTable.id, httpRequest);

				const options: INodePropertyOptions[] = [];

				for (const [index, row] of rows.entries()) {
					const urlValue =
						typeof row[CLAY_URL_COLUMN_NAME] === 'string' ? row[CLAY_URL_COLUMN_NAME] : '';
					const slugValue =
						typeof row[CLAY_TABLE_NAME_COLUMN_NAME] === 'string'
							? row[CLAY_TABLE_NAME_COLUMN_NAME]
							: '';

					if (!urlValue) {
						continue;
					}

					const fields =
						typeof row[CLAY_FIELDS_COLUMN_NAME] === 'string' ? row[CLAY_FIELDS_COLUMN_NAME] : '';

					options.push({
						name: slugValue || `URL ${index + 1}`,
						value: urlValue,
						description: fields ? `Fields: ${fields}` : undefined,
					});
				}

				return options;
			},
		},
		resourceMapping: {
			async getClayFieldValues(this: ILoadOptionsFunctions): Promise<ResourceMapperFields> {
				const requestUrl = this.getCurrentNodeParameter('requestUrl') as string;

				if (!requestUrl) {
					return {
						fields: [],
						emptyFieldsNotice: 'Select a Request URL first to load configured fields.',
					};
				}

				const configuredFields = await getConfiguredFieldsForRequestUrl.call(this, requestUrl);

				return {
					fields: configuredFields.map((fieldName) => ({
						id: fieldName,
						displayName: fieldName,
						defaultMatch: true,
						required: false,
						display: true,
						type: 'string',
					})),
					emptyFieldsNotice: configuredFields.length
						? undefined
						: 'No fields found in the fields column for the selected Request URL.',
				};
			},
		},
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		clearExpiredExecutionStates();

		const items = this.getInputData();
		const requestUrl = this.getNodeParameter('requestUrl', 0) as string;
		const options = this.getNodeParameter('options', 0, {}) as IDataObject;
		const callbackFieldName = (options.callbackFieldName as string) ?? 'resume_url';
		const maxWaitMinutes = typeof options.maxWaitMinutes === 'number' ? options.maxWaitMinutes : 30;

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

		const configuredFields = await getConfiguredFieldsForRequestUrl.call(this, requestUrl);

		const totalItems = items.length;
		upsertExecutionState(executionId, totalItems);

		await Promise.all(
			items.map(async (item, index) => {
				const callbackUrl = `${resumeUrl}?part=${index}&total=${totalItems}`;
				const payloadData = buildMappedPayloadData(this, index, configuredFields, item.json as IDataObject);

				const body: IDataObject = {
					data: payloadData,
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

function buildMappedPayloadData(
	executeFunctions: IExecuteFunctions,
	itemIndex: number,
	configuredFields: string[],
	itemJson: IDataObject,
): IDataObject {
	if (configuredFields.length === 0) {
		return { ...itemJson };
	}

	const fieldValues = executeFunctions.getNodeParameter('fieldValues', itemIndex) as ResourceMapperValue;
	const mappedValues =
		fieldValues && typeof fieldValues === 'object' && fieldValues.value
			? (fieldValues.value as IDataObject)
			: {};
	const payloadData: IDataObject = {};

	for (const fieldName of configuredFields) {
		const mappedValue = mappedValues[fieldName];
		const fieldValue = typeof mappedValue !== 'undefined' ? mappedValue : itemJson[fieldName];
		payloadData[fieldName] = fieldValue ?? null;
	}

	return payloadData;
}

async function getConfiguredFieldsForRequestUrl(
	this: IExecuteFunctions | ILoadOptionsFunctions,
	requestUrl: string,
): Promise<string[]> {
	const credentials = await this.getCredentials<N8nApiCredentials>('clayN8nApi');
	const baseUrl = credentials.baseUrl.replace(/\/$/, '');
	const headers = {
		'X-N8N-API-KEY': credentials.apiKey,
	};
	const httpRequest = this.helpers.httpRequest.bind(this.helpers) as HttpRequester;

	const clayTable = await findOrCreateClayTable(baseUrl, headers, httpRequest);
	const rows = await getClayTableRows(baseUrl, headers, clayTable.id, httpRequest);
	const matchingRow = rows.find((row) => row[CLAY_URL_COLUMN_NAME] === requestUrl);

	if (!matchingRow) {
		return [];
	}

	const rawFields =
		typeof matchingRow[CLAY_FIELDS_COLUMN_NAME] === 'string'
			? matchingRow[CLAY_FIELDS_COLUMN_NAME]
			: '';

	return parseCommaSeparatedFields(rawFields);
}

function parseCommaSeparatedFields(value: string): string[] {
	return value
		.split(',')
		.map((field) => field.trim())
		.filter((field) => field.length > 0);
}

async function listDataTables(
	baseUrl: string,
	headers: Record<string, string>,
	httpRequest: HttpRequester,
): Promise<DataTableSummary[]> {
	const pageSize = 250;
	let cursor: string | undefined;
	const tables: DataTableSummary[] = [];

	while (true) {
		const response = (await httpRequest({
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
	baseUrl: string,
	headers: Record<string, string>,
	httpRequest: HttpRequester,
): Promise<DataTableSummary> {
	const tables = await listDataTables(baseUrl, headers, httpRequest);
	let clayTable = tables.find((table) => table.name === CLAY_TABLE_NAME);

	if (!clayTable) {
		clayTable = (await httpRequest({
			method: 'POST',
			url: `${baseUrl}/data-tables`,
			headers,
			body: {
				name: CLAY_TABLE_NAME,
				columns: [
					{ name: CLAY_TABLE_NAME_COLUMN_NAME, type: 'string' },
					{ name: CLAY_URL_COLUMN_NAME, type: 'string' },
					{ name: CLAY_FIELDS_COLUMN_NAME, type: 'string' },
				],
			},
			json: true,
		})) as DataTableSummary;
	} else {
		const hasFieldsColumn = (clayTable.columns ?? []).some(
			(column) => column.name === CLAY_FIELDS_COLUMN_NAME,
		);

		if (!hasFieldsColumn) {
			await httpRequest({
				method: 'POST',
				url: `${baseUrl}/data-tables/${clayTable.id}/columns`,
				headers,
				body: {
					name: CLAY_FIELDS_COLUMN_NAME,
					type: 'string',
				},
				json: true,
			});
		}
	}

	return clayTable;
}

async function getClayTableRows(
	baseUrl: string,
	headers: Record<string, string>,
	tableId: string,
	httpRequest: HttpRequester,
): Promise<Array<Record<string, unknown>>> {
	const pageSize = 250;
	let cursor: string | undefined;
	const rows: Array<Record<string, unknown>> = [];

	while (true) {
		const response = (await httpRequest({
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
