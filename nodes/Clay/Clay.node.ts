import {
	ApplicationError,
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
type ConcurrencyOptions = {
	maxConcurrency: number;
};

type ExecutionAggregateState = {
	requestUrl: string;
	callbackFieldName: string;
	resumeUrl: string;
	expectedParts: number;
	maxConcurrency: number;
	requestPayloads: IDataObject[];
	nextPartToSend: number;
	activeRequests: number;
	parts: Map<number, IDataObject>;
	createdAt: number;
};

export class Clay implements INodeType {
	private static readonly config = {
		clay_table_name: 'Clay Tables',
		clay_url_column_name: 'url',
		clay_table_name_column_name: 'name',
		clay_fields_column_name: 'fields',
		max_while_iterations: 1_000_000,
		max_cache_age_ms: 1 * 60 * 60 * 1000,
		aggregate_cache: new Map<string, ExecutionAggregateState>(),
	};

	private static upsertExecutionState(
		executionId: string,
		stateInput: Omit<ExecutionAggregateState, 'createdAt'>,
	): ExecutionAggregateState {
		const state: ExecutionAggregateState = {
			...stateInput,
			createdAt: Date.now(),
		};

		Clay.config.aggregate_cache.set(executionId, state);
		return state;
	}

	private static getExecutionState(executionId: string): ExecutionAggregateState | undefined {
		return Clay.config.aggregate_cache.get(executionId);
	}

	private static removeExecutionState(executionId: string): void {
		Clay.config.aggregate_cache.delete(executionId);
	}

	private static clearExpiredExecutionStates(now: number = Date.now()): void {
		for (const [executionId, state] of Clay.config.aggregate_cache.entries()) {
			if (now - state.createdAt > Clay.config.max_cache_age_ms) {
				Clay.config.aggregate_cache.delete(executionId);
			}
		}
	}

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
						displayName: 'Max Concurrency',
						name: 'maxConcurrency',
						type: 'number',
						default: 5,
						typeOptions: {
							minValue: 1,
						},
						description: 'Maximum number of outbound requests running at the same time',
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

				const clayTable = await Clay.findOrCreateClayTable(baseUrl, headers, httpRequest);
				const rows = await Clay.getClayTableRows(baseUrl, headers, clayTable.id, httpRequest);

				const options: INodePropertyOptions[] = [];

				for (const [index, row] of rows.entries()) {
					const rawUrlValue = row[Clay.config.clay_url_column_name];
					const rawSlugValue = row[Clay.config.clay_table_name_column_name];
					const rawFieldsValue = row[Clay.config.clay_fields_column_name];

					const urlValue = typeof rawUrlValue === 'string' ? rawUrlValue : '';
					const slugValue = typeof rawSlugValue === 'string' ? rawSlugValue : '';

					if (!urlValue) {
						continue;
					}

					const fields = typeof rawFieldsValue === 'string' ? rawFieldsValue : '';

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

				const configuredFields = await Clay.getConfiguredFieldsForRequestUrl(this, requestUrl);

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
		Clay.clearExpiredExecutionStates();

		const items = this.getInputData();
		const requestUrl = this.getNodeParameter('requestUrl', 0) as string;
		const options = this.getNodeParameter('options', 0, {}) as IDataObject;
		const callbackFieldName = (options.callbackFieldName as string) ?? 'resume_url';
		const maxWaitMinutes = typeof options.maxWaitMinutes === 'number' ? options.maxWaitMinutes : 30;
		const concurrencyOptions = Clay.getConcurrencyOptions(options);

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

		const configuredFields = await Clay.getConfiguredFieldsForRequestUrl(this, requestUrl);

		const totalItems = items.length;
		if (totalItems === 0) {
			return [items];
		}

		const requestPayloads = items.map((item, index) =>
			Clay.buildMappedPayloadData(this, index, configuredFields, item.json as IDataObject),
		);

		const state = Clay.upsertExecutionState(executionId, {
			requestUrl,
			callbackFieldName,
			resumeUrl,
			expectedParts: totalItems,
			maxConcurrency: concurrencyOptions.maxConcurrency,
			requestPayloads,
			nextPartToSend: 0,
			activeRequests: 0,
			parts: new Map<number, IDataObject>(),
		});

		const httpRequest = this.helpers.httpRequest.bind(this.helpers) as HttpRequester;
		await Clay.dispatchQueuedRequests(state, httpRequest);

		const waitTill = new Date(Date.now() + maxWaitMinutes * 60 * 1000);
		await this.putExecutionToWait(waitTill);

		return [items];
	}

	async webhook(this: IWebhookFunctions): Promise<IWebhookResponseData> {
		Clay.clearExpiredExecutionStates();

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

		const partIndex = Number.parseInt(partParam ?? '', 10);

		if (Number.isNaN(partIndex) || partIndex < 0) {
			return {
				webhookResponse: {
					status: 400,
					body: { message: 'Invalid or missing part query parameter.' },
				},
			};
		}

		const state = Clay.getExecutionState(executionId);
		if (!state) {
			return {
				webhookResponse: {
					status: 404,
					body: { message: 'Execution state not found or expired.' },
				},
			};
		}

		if (partIndex >= state.expectedParts) {
			return {
				webhookResponse: {
					status: 400,
					body: {
						message: 'Part index is out of range.',
						expectedParts: state.expectedParts,
					},
				},
			};
		}

		if (partIndex >= state.nextPartToSend) {
			return {
				webhookResponse: {
					status: 409,
					body: {
						message: 'Callback received for a request that has not been sent yet.',
						part: partIndex,
						nextPartToSend: state.nextPartToSend,
					},
				},
			};
		}

		const incomingData = this.getBodyData();
		const isDuplicateCallback = state.parts.has(partIndex);
		if (!isDuplicateCallback) {
			state.parts.set(
				partIndex,
				Clay.isDataObject(incomingData) ? incomingData : { data: incomingData },
			);
			state.activeRequests = Math.max(0, state.activeRequests - 1);

			const httpRequest = this.helpers.httpRequest.bind(this.helpers) as HttpRequester;
			await Clay.dispatchQueuedRequests(state, httpRequest);
		}

		if (state.parts.size >= state.expectedParts) {
			const sortedEntries = [...state.parts.entries()].sort(([a], [b]) => a - b);
			Clay.removeExecutionState(executionId);

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
					message: isDuplicateCallback
						? 'Duplicate callback received. Waiting for remaining callbacks.'
						: 'Part received. Waiting for remaining callbacks.',
					received: state.parts.size,
					expected: state.expectedParts,
					currentPart: partIndex,
					activeRequests: state.activeRequests,
					nextPartToSend: state.nextPartToSend,
					remainingToSend: state.expectedParts - state.nextPartToSend,
					pendingCallbacks: state.expectedParts - state.parts.size,
				},
			},
		};
	}

	private static isDataObject(value: unknown): value is IDataObject {
		return typeof value === 'object' && value !== null && !Array.isArray(value);
	}

	private static buildMappedPayloadData(
		executeFunctions: IExecuteFunctions,
		itemIndex: number,
		configuredFields: string[],
		itemJson: IDataObject,
	): IDataObject {
		if (configuredFields.length === 0) {
			return { ...itemJson };
		}

		const fieldValues = executeFunctions.getNodeParameter(
			'fieldValues',
			itemIndex,
		) as ResourceMapperValue;
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

	private static async getConfiguredFieldsForRequestUrl(
		context: IExecuteFunctions | ILoadOptionsFunctions,
		requestUrl: string,
	): Promise<string[]> {
		const credentials = await context.getCredentials<N8nApiCredentials>('clayN8nApi');
		const baseUrl = credentials.baseUrl.replace(/\/$/, '');
		const headers = {
			'X-N8N-API-KEY': credentials.apiKey,
		};
		const httpRequest = context.helpers.httpRequest.bind(context.helpers) as HttpRequester;

		const clayTable = await Clay.findOrCreateClayTable(baseUrl, headers, httpRequest);
		const rows = await Clay.getClayTableRows(baseUrl, headers, clayTable.id, httpRequest);
		const matchingRow = rows.find((row) => row[Clay.config.clay_url_column_name] === requestUrl);

		if (!matchingRow) {
			return [];
		}

		const rawMatchingFields = matchingRow[Clay.config.clay_fields_column_name];
		const rawFields = typeof rawMatchingFields === 'string' ? rawMatchingFields : '';

		return Clay.parseCommaSeparatedFields(rawFields);
	}

	private static parseCommaSeparatedFields(value: string): string[] {
		return value
			.split(',')
			.map((field) => field.trim())
			.filter((field) => field.length > 0);
	}

	private static getConcurrencyOptions(options: IDataObject): ConcurrencyOptions {
		const rawMaxConcurrency = options.maxConcurrency;
		const maxConcurrency =
			typeof rawMaxConcurrency === 'number' && Number.isFinite(rawMaxConcurrency)
				? Math.floor(rawMaxConcurrency)
				: 5;

		return {
			maxConcurrency: Math.max(1, maxConcurrency),
		};
	}

	private static async dispatchQueuedRequests(
		state: ExecutionAggregateState,
		httpRequest: HttpRequester,
	): Promise<void> {
		let iterationCount = 0;

		while (
			state.activeRequests < state.maxConcurrency &&
			state.nextPartToSend < state.expectedParts
		) {
			iterationCount += 1;
			if (iterationCount > Clay.config.max_while_iterations) {
				throw new ApplicationError('Loop guard exceeded in dispatchQueuedRequests');
			}

			const partIndex = state.nextPartToSend;
			state.nextPartToSend += 1;
			state.activeRequests += 1;

			try {
				await Clay.sendRequestForPart(state, partIndex, httpRequest);
			} catch (error) {
				state.nextPartToSend = Math.max(partIndex, state.nextPartToSend - 1);
				state.activeRequests = Math.max(0, state.activeRequests - 1);
				throw error;
			}
		}
	}

	private static async sendRequestForPart(
		state: ExecutionAggregateState,
		partIndex: number,
		httpRequest: HttpRequester,
	): Promise<void> {
		const payloadData = state.requestPayloads[partIndex] ?? {};
		const callbackUrl = `${state.resumeUrl}?part=${partIndex}`;
		const body: IDataObject = {
			data: payloadData,
			[state.callbackFieldName]: callbackUrl,
		};

		await httpRequest({
			method: 'POST',
			url: state.requestUrl,
			body,
			json: true,
		});
	}

	private static async listDataTables(
		baseUrl: string,
		headers: Record<string, string>,
		httpRequest: HttpRequester,
	): Promise<DataTableSummary[]> {
		const pageSize = 250;
		let cursor: string | undefined;
		const tables: DataTableSummary[] = [];
		let iterationCount = 0;

		while (true) {
			iterationCount += 1;
			if (iterationCount > Clay.config.max_while_iterations) {
				throw new ApplicationError('Loop guard exceeded in listDataTables');
			}

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

	private static async findOrCreateClayTable(
		baseUrl: string,
		headers: Record<string, string>,
		httpRequest: HttpRequester,
	): Promise<DataTableSummary> {
		const tables = await Clay.listDataTables(baseUrl, headers, httpRequest);
		let clayTable = tables.find((table) => table.name === Clay.config.clay_table_name);

		if (!clayTable) {
			clayTable = (await httpRequest({
				method: 'POST',
				url: `${baseUrl}/data-tables`,
				headers,
				body: {
					name: Clay.config.clay_table_name,
					columns: [
						{ name: Clay.config.clay_table_name_column_name, type: 'string' },
						{ name: Clay.config.clay_url_column_name, type: 'string' },
						{ name: Clay.config.clay_fields_column_name, type: 'string' },
					],
				},
				json: true,
			})) as DataTableSummary;
		} else {
			const hasFieldsColumn = (clayTable.columns ?? []).some(
				(column) => column.name === Clay.config.clay_fields_column_name,
			);

			if (!hasFieldsColumn) {
				await httpRequest({
					method: 'POST',
					url: `${baseUrl}/data-tables/${clayTable.id}/columns`,
					headers,
					body: {
						name: Clay.config.clay_fields_column_name,
						type: 'string',
					},
					json: true,
				});
			}
		}

		return clayTable;
	}

	private static async getClayTableRows(
		baseUrl: string,
		headers: Record<string, string>,
		tableId: string,
		httpRequest: HttpRequester,
	): Promise<Array<Record<string, unknown>>> {
		const pageSize = 250;
		let cursor: string | undefined;
		const rows: Array<Record<string, unknown>> = [];
		let iterationCount = 0;

		while (true) {
			iterationCount += 1;
			if (iterationCount > Clay.config.max_while_iterations) {
				throw new ApplicationError('Loop guard exceeded in getClayTableRows');
			}

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
}
