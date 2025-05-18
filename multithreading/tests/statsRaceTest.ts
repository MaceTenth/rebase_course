import { WorkerStatsManager } from '../src/workerStats';
import { Task } from '../src/types';

describe('WorkerStatsManager Race Conditions', () => {
    let statsManager: WorkerStatsManager;
    
    beforeEach(() => {
        statsManager = new WorkerStatsManager();

        // Ensure all workerIds are initialized
        for (let i = 0; i < 4; i++) {
            statsManager.initWorkerStats(i);
        }
    });

    test('concurrent stats updates', async () => {
        // Simulate multiple workers updating stats simultaneously
        const updates = Array(1000).fill(null).map((_, index) => {
            const taskCompleted = { id: index, startByte: 0, endByte: 100 };
            return {
                workerId: index % 4,
                taskCompleted,
                processingTime: 100,
                count: 5,
                memoryUsage: process.memoryUsage()
            };
        });

        // Execute updates in parallel but ensure each worker's updates are sequential
        const workerUpdates = new Map<number, Promise<void>>();
        
        const updatePromises = updates.map(update => {
            const workerId = update.workerId;
            const currentPromise = workerUpdates.get(workerId) || Promise.resolve();
            
            // Chain the new update after any pending updates for this worker
            const newPromise = currentPromise.then(() => 
                new Promise<void>(resolve => {
                    statsManager.updateWorkerStats(update);
                    resolve();
                })
            );
            
            workerUpdates.set(workerId, newPromise);
            return newPromise;
        });

        // Wait for all updates to complete
        await Promise.all(updatePromises);

        // Verify consistency
        const totalProcessed = statsManager.getTotalProcessed();
        const expectedTotalTasks = updates.length;
        expect(totalProcessed).toBe(expectedTotalTasks);

        // Verify total primes found
        let totalPrimesFound = 0;
        statsManager.getWorkerStats().forEach(workerStat => {
            totalPrimesFound += workerStat.primesFound;
        });
        const expectedTotalPrimes = updates.length * 5; // 5 is the count per update
        expect(totalPrimesFound).toBe(expectedTotalPrimes);
    });
});