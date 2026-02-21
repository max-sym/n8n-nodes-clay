import type { IDataObject } from 'n8n-workflow';

export type ExecutionAggregateState = {
	expectedParts: number;
	parts: Map<number, IDataObject>;
	createdAt: number;
};

const CACHE_HOURS = 1;
const MAX_CACHE_AGE_MS = CACHE_HOURS * 60 * 60 * 1000;
const aggregateCache = new Map<string, ExecutionAggregateState>();

export function upsertExecutionState(
	executionId: string,
	expectedParts: number,
): ExecutionAggregateState {
	const existingState = aggregateCache.get(executionId);
	if (existingState) {
		existingState.expectedParts = expectedParts;
		return existingState;
	}

	const state: ExecutionAggregateState = {
		expectedParts,
		parts: new Map<number, IDataObject>(),
		createdAt: Date.now(),
	};

	aggregateCache.set(executionId, state);
	return state;
}

export function getExecutionState(executionId: string): ExecutionAggregateState | undefined {
	return aggregateCache.get(executionId);
}

export function removeExecutionState(executionId: string): void {
	aggregateCache.delete(executionId);
}

export function clearExpiredExecutionStates(now: number = Date.now()): void {
	for (const [executionId, state] of aggregateCache.entries()) {
		if (now - state.createdAt > MAX_CACHE_AGE_MS) {
			aggregateCache.delete(executionId);
		}
	}
}
