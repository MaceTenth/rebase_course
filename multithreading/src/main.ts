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
    
    // Track failed tasks for reprocessing
    const failedTasks: Task[] = [];
    // Main tasks queue
    let tasks: Task[] = [];
    // Track remaining file bytes to process for dynamic task creation
    let remainingFileRange: { start: number, end: number } | null = null;
    
    const customFilePath = process.argv[2];
    const inputFileName = customFilePath || 'input.txt';
    const inputFilePath = path.resolve(inputFileName);
    // Track file size for progress calculation
    let fileSize = 0;

    console.log(`Reading from file: ${inputFilePath}`);

    // Create a separate function to handle worker creation
    function createWorker(workerId: number, initialTask: Task): Worker {
        statsManager.initWorkerStats(workerId);
        const worker = new Worker(__filename, {
            workerData: {
                workerId,
                filePath: inputFilePath,
                initialTask
            },
            // Remove execArgv and use resourceLimits:
            resourceLimits: { maxOldGenerationSizeMb: 500 }
        });

        busyWorkers.add(workerId);
        statsManager.setCurrentTask(workerId, initialTask);
        console.log(`Main: Created worker ${workerId} with initial task ${initialTask.id}`);

        worker.on('message', handleWorkerMessage.bind(null, worker, workerId));

        worker.on('error', (error) => {
            console.error(`Worker ${workerId} error:`, error);
            const currentTask = statsManager.getCurrentTask(workerId);
            if (currentTask) {
                console.log(`Main: Re-queueing failed task ${currentTask.id} from worker ${workerId}`);
                failedTasks.push(currentTask);
            }
            busyWorkers.delete(workerId);
            if (tasks.length > 0 || failedTasks.length > 0 || remainingFileRange !== null) {
                const replacementTask = failedTasks.pop() || tasks.pop() ||
                    (remainingFileRange && taskManager.createAdaptiveTask(
                        remainingFileRange.start,
                        remainingFileRange.end,
                        'average',
                        statsManager.getGlobalAverageProcessingTime()
                    ));
                if (replacementTask) {
                    if (remainingFileRange && replacementTask.startByte >= remainingFileRange.start) {
                        remainingFileRange.start = replacementTask.endByte;
                        if (remainingFileRange.start >= remainingFileRange.end) {
                            remainingFileRange = null;
                        }
                    }
                    createWorker(workerId, replacementTask);
                }
            }
        });

        return worker;
    }

    // Function to handle messages from workers
    function handleWorkerMessage(worker: Worker, workerId: number, result: any) {
        primeCount += result.count;
        totalBytesProcessed += result.taskCompleted.endByte - result.taskCompleted.startByte;
        statsManager.updateWorkerStats(result);
        taskManager.addPerformanceData(result.processingTime);
        
        statsManager.updateProgress(fileSize, totalBytesProcessed, startTime);
        busyWorkers.delete(workerId);
        assignTaskToWorker(worker, workerId);

        // Check if all tasks are completed
        if (busyWorkers.size === 0 && tasks.length === 0 && failedTasks.length === 0 && remainingFileRange === null) {
            const endTime = process.hrtime(startTime);
            const duration = (endTime[0] * 1e9 + endTime[1]) / 1e6;
            
            console.log('\n=== Final Results ===');
            console.log(`Found ${primeCount} prime numbers.`);
            console.log(`Time taken: ${duration.toFixed(3)}ms`);
            
            statsManager.printFinalStats();
            process.exit(0);
        }
    }

    // Improved function for assigning tasks
    function assignTaskToWorker(worker: Worker, workerId: number) {
        // First check for failed tasks
        if (failedTasks.length > 0) {
            const task = failedTasks.pop();
            if (task) {
                busyWorkers.add(workerId);
                statsManager.setCurrentTask(workerId, task);
                worker.postMessage({ type: 'task', task });
                console.log(`Main: Assigned recovered task ${task.id} to worker ${workerId}`);
                return;
            }
        }
        
        // Check for regular tasks
        if (tasks.length > 0) {
            // Use a simplified task assignment strategy based on worker performance
            const workerPerformance = statsManager.getWorkerPerformance(workerId);
            
            // For slower workers, assign smaller tasks if available
            if (workerPerformance === 'slow' && tasks.length > 1) {
                // Sort tasks by size (ascending) and assign the smallest
                tasks.sort((a, b) => (a.endByte - a.startByte) - (b.endByte - b.startByte));
                const task = tasks.shift()!;
                busyWorkers.add(workerId);
                statsManager.setCurrentTask(workerId, task);
                worker.postMessage({ type: 'task', task });
                console.log(`Main: Assigned smaller task ${task.id} to slow worker ${workerId}`);
                return;
            }
            
            // For fast workers or when we don't have choices, assign the next task
            const task = tasks.pop()!;
            busyWorkers.add(workerId);
            statsManager.setCurrentTask(workerId, task);
            worker.postMessage({ type: 'task', task });
            console.log(`Main: Assigned task ${task.id} to worker ${workerId}`);
            return;
        }
        
        // Check if there's still a portion of the file to process dynamically
        if (remainingFileRange !== null) {
            const workerPerformance = statsManager.getWorkerPerformance(workerId);
            const globalAvg = statsManager.getGlobalAverageProcessingTime();
            
            // Create a task sized appropriately for this worker's performance
            const task = taskManager.createAdaptiveTask(
                remainingFileRange.start, 
                remainingFileRange.end,
                workerPerformance,
                globalAvg
            );
            
            // Update remaining range
            remainingFileRange.start = task.endByte;
            if (remainingFileRange.start >= remainingFileRange.end) {
                remainingFileRange = null;
            }
            
            busyWorkers.add(workerId);
            statsManager.setCurrentTask(workerId, task);
            worker.postMessage({ type: 'task', task });
            console.log(`Main: Assigned adaptive task ${task.id} to ${workerPerformance} worker ${workerId}`);
            return;
        }
        
        // No tasks left
        worker.postMessage({ type: 'exit' });
    }

    fs.stat(inputFilePath, (err, stats) => {
        if (err) {
            console.error('Failed to get file stats:', err);
            return;
        }

        fileSize = stats.size;
        // Create initial tasks
        tasks = taskManager.createInitialTasks(fileSize);
        
        // Set initial remaining range for dynamic task creation later
        if (tasks.length > 0) {
            const lastInitialTask = tasks[tasks.length - 1];
            remainingFileRange = {
                start: lastInitialTask.endByte,
                end: fileSize
            };
            
            // If the last initial task already reaches the end of file,
            // there's no remaining range to process
            if (remainingFileRange.start >= remainingFileRange.end) {
                remainingFileRange = null;
            }
        }
        
        console.log(`Utilizing ${taskManager.getNumCores()} CPU cores for ${tasks.length} initial chunks`);

        // Create workers with initial tasks
        for (let i = 0; i < taskManager.getNumCores() && (tasks.length > 0 || remainingFileRange !== null); i++) {
            let initialTask: Task;
            
            if (tasks.length > 0) {
                initialTask = tasks.pop()!;
            } else {
                // Create a task from remaining range if no pre-created tasks are available
                initialTask = taskManager.createAdaptiveTask(
                    remainingFileRange!.start,
                    remainingFileRange!.end,
                    'average',
                    0 // No average time data yet
                );
                
                // Update remaining range
                remainingFileRange!.start = initialTask.endByte;
                if (remainingFileRange!.start >= remainingFileRange!.end) {
                    remainingFileRange = null;
                }
            }
            
            createWorker(i, initialTask);
        }
    });
} else {
    // Worker thread code
    handleTasks(workerData).catch(error => {
        console.error(`Worker ${workerData.workerId} error:`, error);
        process.exit(1);
    });
}
