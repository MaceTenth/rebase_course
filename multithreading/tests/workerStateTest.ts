import { Worker, isMainThread } from 'worker_threads';
import { WorkerStatsManager } from '../src/workerStats';
import { TaskManager } from '../src/taskManager';

describe('Worker-Main Thread State Consistency', () => {
    let statsManager: WorkerStatsManager;
    let taskManager: TaskManager;
    const busyWorkers = new Set<number>();

    beforeEach(() => {
        statsManager = new WorkerStatsManager();
        taskManager = new TaskManager();
    });

    test('worker state consistency during errors', async () => {
        // Create a task that will cause an error
        const errorTask = { id: 1, startByte: 0, endByte: 100 };

        // Track states
        const states = {
            workerBusy: false,
            taskAssigned: false,
            errorReceived: false,
            cleanupCompleted: false
        };

        // Create worker with error task
        const worker = new Worker(__filename, {
            workerData: { workerId: 1, task: errorTask }
        });

        busyWorkers.add(1);
        states.workerBusy = true;
        // Ensure workerId is initialized
        statsManager.initWorkerStats(1);
        statsManager.setCurrentTask(1, errorTask);
        states.taskAssigned = true;

        await new Promise<void>((resolve) => {
            worker.on('error', () => {
                states.errorReceived = true;
                busyWorkers.delete(1);
                // Clear the worker task
                statsManager.clearWorkerTask(1);
                expect(statsManager.getCurrentTask(1)).toBeNull();
                states.cleanupCompleted = true;
                worker.terminate().then(() => resolve());
            });
        });

        // Verify final state
        expect(states.errorReceived).toBeTruthy();
        expect(busyWorkers.has(1)).toBeFalsy();
        expect(statsManager.getCurrentTask(1)).toBeNull();
    }, 10000); // Increase timeout to 10 seconds
});