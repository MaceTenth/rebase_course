import * as fs from 'fs';
import * as readline from 'readline';
import { parentPort } from 'worker_threads';
import { Task, WorkerInput } from './types';
import { isPrime } from './primeCalculator';

export async function processTask(task: Task, filePath: string): Promise<{count: number, time: number}> {
    const startTime = process.hrtime();
    
    return new Promise((resolve) => {
        let count = 0;
        const readStream = fs.createReadStream(filePath, {
            start: task.startByte,
            end: task.endByte - 1,
            encoding: 'utf8'
        });

        const rl = readline.createInterface({
            input: readStream,
            crlfDelay: Infinity
        });

        let isFirstLine = true;

        rl.on('line', (line) => {
            if (isFirstLine && task.startByte > 0) {
                isFirstLine = false;
                return;
            }

            const num = parseInt(line.trim(), 10);
            if (!isNaN(num) && isPrime(num)) {
                count++;
            }
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
        const { count, time } = await processTask(currentTask, filePath);
        parentPort?.postMessage({
            count,
            workerId,
            taskCompleted: currentTask,
            processingTime: time,
            memoryUsage: process.memoryUsage()
        });

        currentTask = await new Promise<Task | null>(resolve => {
            parentPort?.once('message', (message) => {
                if (message.type === 'task') {
                    resolve(message.task);
                } else {
                    resolve(null);
                }
            });
        });
    }
}