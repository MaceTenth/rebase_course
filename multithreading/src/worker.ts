import * as fs from 'fs';
import * as readline from 'readline';
import { parentPort } from 'worker_threads';
import { Task, WorkerInput } from './types';
import { isPrime } from './primeCalculator';

export async function processTask(task: Task, filePath: string): Promise<{count: number, time: number}> {
    const startTime = process.hrtime();
    
    return new Promise((resolve, reject) => {
        let count = 0;
        
        // Add error handling for file access issues
        let readStream;
        try {
            readStream = fs.createReadStream(filePath, {
                start: task.startByte,
                end: task.endByte - 1,
                encoding: 'utf8',
                highWaterMark: 64 * 1024 // 64KB buffer for better performance
            });
        } catch (err) {
            const error = err as Error;
            reject(new Error(`Failed to create read stream for task ${task.id}: ${error.message}`));
            return;
        }

        // Add error handler to the stream
        readStream.on('error', (err) => {
            const error = err as Error;
            reject(new Error(`Read stream error for task ${task.id}: ${error.message}`));
        });

        const rl = readline.createInterface({
            input: readStream,
            crlfDelay: Infinity
        });

        let isFirstLine = true;

        rl.on('line', (line) => {
            // Skip first line if we're starting in the middle of the file
            // to avoid partial line issues
            if (isFirstLine && task.startByte > 0) {
                isFirstLine = false;
                return;
            }

            try {
                const num = parseInt(line.trim(), 10);
                if (!isNaN(num) && isPrime(num)) {
                    count++;
                }
            } catch (err) {
                // Log invalid line but continue processing
                const error = err as Error;
                console.error(`Error processing line in task ${task.id}: ${error.message}`);
            }
        });

        rl.on('error', (err) => {
            const error = err as Error;
            reject(new Error(`Readline error for task ${task.id}: ${error.message}`));
        });

        rl.on('close', () => {
            const endTime = process.hrtime(startTime);
            const duration = (endTime[0] * 1e9 + endTime[1]) / 1e6;
            resolve({ count, time: duration });
        });
    });
}

export async function handleTasks(workerData: WorkerInput): Promise<void> {
    const { workerId, filePath, initialTask } = workerData;
    let currentTask: Task | null = initialTask;
    
    while (currentTask !== null) {
        try {
            const { count, time } = await processTask(currentTask, filePath);
            parentPort?.postMessage({
                count,
                workerId,
                taskCompleted: currentTask,
                processingTime: time,
                memoryUsage: process.memoryUsage()
            });
        } catch (err) {
            const error = err as Error;
            // Send error notification to main thread before exiting
            parentPort?.postMessage({
                type: 'error',
                workerId,
                taskFailed: currentTask,
                error: error.message || 'Unknown error'
            });
            
            // Re-throw to trigger the worker's 'error' event
            throw error;
        }

        // Wait for next task from main thread
        currentTask = await new Promise<Task | null>(resolve => {
            parentPort?.once('message', (message) => {
                if (message.type === 'task') {
                    resolve(message.task);
                } else if (message.type === 'exit') {
                    resolve(null);
                } else {
                    // Unknown message type, default to null
                    console.warn(`Worker ${workerId} received unknown message type: ${message.type}`);
                    resolve(null);
                }
            });
        });
    }
}