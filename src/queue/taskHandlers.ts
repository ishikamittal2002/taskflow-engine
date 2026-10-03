export interface TaskPayload {
  email?: string;
  template?: string;
  datasetId?: string;
  [key: string]: any;
}

export const processTask = async (taskType: string, payload: TaskPayload): Promise<any> => {
  // Simulate processing time
  const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

  switch (taskType) {
    case 'EMAIL':
      console.log(`[Worker] Sending email to: ${payload.email}`);
      await delay(1000); // simulate network latency
      return { delivered: true, recipient: payload.email, timestamp: new Date().toISOString() };

    case 'DATA_PROCESSING':
      console.log(`[Worker] Processing dataset: ${payload.datasetId}`);
      await delay(1500);
      return { rowsProcessed: 450, status: 'COMPLETED' };

    case 'FAIL_TEST':
      // Test case to verify retry and backoff logic
      console.log(`[Worker] Executing test failure task...`);
      await delay(500);
      throw new Error('Simulated upstream service error (testing retry policy)');

    default:
      throw new Error(`Unsupported task type: ${taskType}`);
  }
};