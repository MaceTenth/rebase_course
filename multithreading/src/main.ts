import * as fs from 'fs';
import * as path from 'path';
import { Worker, isMainThread, workerData } from 'worker_threads';
import { WorkerStatsManager } from './workerStats';
import { TaskManager } from './taskManager';
import { handleTasks } from './worker';
import { Task } from './types';

if (isMainThread) {
    const startTime = process.hrtime();
    let totalBytesProcessed = 0;
    let primeCount = 0;
    
    const statsManager = new WorkerStatsManager();
    const taskManager = new TaskManager();
    const busyWorkers = new Set<number>();
    
    const customFilePath = process.argv[2];
    const inputFileName = customFilePath || 'input.txt';
    const inputFilePath = path.resolve(inputFileName);

    console.log(`Reading from file: ${inputFilePath}`);

    fs.stat(inputFilePath, (err, stats) => {
        if (err) {
            console.error('Failed to get file stats:', err);
            return;
        }

        const fileSize = stats.size;
        const tasks = taskManager.createInitialTasks(fileSize);
        
        function assignTaskToWorker(worker: Worker, workerId: number) {
            const currentWorkerStats = statsManager.getWorkerStats().get(workerId);
            const avgProcessingTime = currentWorkerStats?.avgProcessingTime || 0;
            
            // Calculate global average processing time
            let globalAverageProcessingTime = 0;
            let totalWorkers = 0;
            for (const stats of statsManager.getWorkerStats().values()) {
                if (stats.tasksCompleted > 0) {
                    globalAverageProcessingTime += stats.avgProcessingTime;
                    totalWorkers++;
                }
            }
            globalAverageProcessingTime = totalWorkers > 0 ? 
                globalAverageProcessingTime / totalWorkers : 0;
            
            // Sort tasks by size and assign smaller chunks to slower workers
            if (avgProcessingTime > globalAverageProcessingTime) {
                const smallerTask = tasks.find(t => 
                    (t.endByte - t.startByte) < taskManager.calculateChunkSize(
                        fileSize,
                        t.endByte - t.startByte,
                        workerId,
                        statsManager.getWorkerStats()
                    )
                );
                if (smallerTask) {
                    tasks.splice(tasks.indexOf(smallerTask), 1);
                    busyWorkers.add(workerId);
                    worker.postMessage({ type: 'task', task: smallerTask });
                    console.log(`Main: Assigned smaller task ${smallerTask.id} to worker ${workerId}`);
                    return;
                }
            }
            
            const task = tasks.pop();
            if (task) {
                busyWorkers.add(workerId);
                worker.postMessage({ type: 'task', task });
                console.log(`Main: Assigned task ${task.id} to worker ${workerId}`);
            } else {
                worker.postMessage({ type: 'exit' });
            }
        }

        console.log(`Utilizing ${taskManager.getNumCores()} CPU cores for ${tasks.length} chunks`);

        // Create workers
        for (let i = 0; i < taskManager.getNumCores(); i++) {
            statsManager.initWorkerStats(i);
            const initialTask = tasks.pop() as Task;
            const worker = new Worker(__filename, {
                workerData: {
                    workerId: i,
                    filePath: inputFilePath,
                    initialTask
                }
            });

            busyWorkers.add(i);
            console.log(`Main: Created worker ${i} with initial task ${initialTask.id}`);

            worker.on('message', (result) => {
                primeCount += result.count;
                totalBytesProcessed += result.taskCompleted.endByte - result.taskCompleted.startByte;
                statsManager.updateWorkerStats(result);
                taskManager.addPerformanceData(result.processingTime);
                
                statsManager.updateProgress(fileSize, totalBytesProcessed, startTime);
                busyWorkers.delete(result.workerId);
                assignTaskToWorker(worker, result.workerId);

                if (busyWorkers.size === 0 && tasks.length === 0) {
                    const endTime = process.hrtime(startTime);
                    const duration = (endTime[0] * 1e9 + endTime[1]) / 1e6;
                    
                    console.log('\n=== Final Results ===');
                    console.log(`Found ${primeCount} prime numbers.`);
                    console.log(`Time taken: ${duration.toFixed(3)}ms`);
                    
                    statsManager.printFinalStats();
                    process.exit(0);
                }
            });

            worker.on('error', (error) => {
                console.error(`Worker ${i} error:`, error);
                busyWorkers.delete(i);
            });
        }
    });
} else {
    // Worker thread code
    handleTasks(workerData).catch(error => {
        console.error(`Worker ${workerData.workerId} error:`, error);
        process.exit(1);
    });
}
