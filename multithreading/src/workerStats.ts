import { WorkerStats, WorkerResult, Task } from './types';

export class WorkerStatsManager {
    private workerStats: Map<number, WorkerStats> = new Map();
    private lastProgressUpdate = Date.now();
    private readonly PROGRESS_UPDATE_INTERVAL = 1000; // 1 second
    private currentTasks: Map<number, Task> = new Map(); // Track current tasks by worker ID
    private globalAverageProcessingTime: number = 0; // Maintain a running global average
    private totalTasksCompleted: number = 0;
    private updateLock = false;

    initWorkerStats(workerId: number): void {
        this.workerStats.set(workerId, {
            tasksCompleted: 0,
            totalProcessingTime: 0,
            primesFound: 0,
            avgProcessingTime: 0
        });
    }

    updateWorkerStats(result: WorkerResult): void {
        // Wait for lock to be released
        while (this.updateLock) {
            // Busy wait
        }
        
        try {
            this.updateLock = true;
            const stats = this.workerStats.get(result.workerId);
            if (stats) {
                // Update individual worker stats
                stats.tasksCompleted++;
                stats.totalProcessingTime += result.processingTime;
                stats.primesFound += result.count;
                stats.avgProcessingTime = stats.totalProcessingTime / stats.tasksCompleted;
                
                // Update global average efficiently
                this.totalTasksCompleted++;
                this.globalAverageProcessingTime = (
                    (this.globalAverageProcessingTime * (this.totalTasksCompleted - 1)) + 
                    result.processingTime
                ) / this.totalTasksCompleted;
                
                // Clear current task tracking
                this.currentTasks.delete(result.workerId);
            }
        } finally {
            this.updateLock = false;
        }
    }

    // Track which task a worker is currently processing
    setCurrentTask(workerId: number, task: Task): void {
        this.currentTasks.set(workerId, task);
    }

    // Get the current task a worker is processing (for error recovery)
    getCurrentTask(workerId: number): Task | null {
        return this.currentTasks.get(workerId) || null;
    }

    // Clear the current task for a worker
    clearWorkerTask(workerId: number): void {
        this.currentTasks.delete(workerId);
    }

    // Determine if a worker is slow, average, or fast based on its performance
    getWorkerPerformance(workerId: number): 'slow' | 'average' | 'fast' {
        if (this.totalTasksCompleted < 3) {
            return 'average'; // Not enough data to make a determination
        }
        
        const stats = this.workerStats.get(workerId);
        if (!stats || stats.tasksCompleted === 0) {
            return 'average';
        }
        
        const ratio = stats.avgProcessingTime / this.globalAverageProcessingTime;
        
        if (ratio > 1.2) {
            return 'slow';
        } else if (ratio < 0.8) {
            return 'fast';
        } else {
            return 'average';
        }
    }

    // Get the global average processing time
    getGlobalAverageProcessingTime(): number {
        return this.globalAverageProcessingTime;
    }

    // Get the total number of processed tasks across all workers
    getTotalProcessed(): number {
        return Array.from(this.workerStats.values()).reduce((total, stats) => total + stats.tasksCompleted, 0);
    }

    updateProgress(fileSize: number, totalBytesProcessed: number, startTime: [number, number]): void {
        const now = Date.now();
        if (now - this.lastProgressUpdate >= this.PROGRESS_UPDATE_INTERVAL) {
            const progress = (totalBytesProcessed / fileSize) * 100;
            const totalMemory = process.memoryUsage();
            
            console.clear();
            console.log(`Progress: ${progress.toFixed(2)}%`);
            console.log(`Memory Usage (Main Thread):`);
            console.log(`- Heap Used: ${(totalMemory.heapUsed / 1024 / 1024).toFixed(2)} MB`);
            console.log(`- RSS: ${(totalMemory.rss / 1024 / 1024).toFixed(2)} MB`);
            console.log('\nWorker Statistics:');
            
            for (const [workerId, stats] of this.workerStats) {
                console.log(`Worker ${workerId}:`);
                console.log(`- Tasks Completed: ${stats.tasksCompleted}`);
                console.log(`- Primes Found: ${stats.primesFound}`);
                console.log(`- Avg Processing Time: ${stats.avgProcessingTime.toFixed(2)}ms`);
                console.log(`- Performance: ${this.getWorkerPerformance(workerId)}`);
                
                // Show current task if one exists
                const currentTask = this.currentTasks.get(workerId);
                if (currentTask) {
                    console.log(`- Current Task: ID ${currentTask.id} (${(currentTask.endByte - currentTask.startByte).toLocaleString()} bytes)`);
                }
            }
            
            console.log(`\nGlobal Average Processing Time: ${this.globalAverageProcessingTime.toFixed(2)}ms`);
            this.printTimeEstimate(fileSize, totalBytesProcessed, startTime);
            this.lastProgressUpdate = now;
        }
    }

    printFinalStats(): void {
        console.log('\nWorker Performance Summary:');
        for (const [workerId, stats] of this.workerStats) {
            console.log(`\nWorker ${workerId}:`);
            console.log(`- Total Tasks: ${stats.tasksCompleted}`);
            console.log(`- Total Primes Found: ${stats.primesFound}`);
            console.log(`- Average Processing Time: ${stats.avgProcessingTime.toFixed(2)}ms`);
            console.log(`- Performance Category: ${this.getWorkerPerformance(workerId)}`);
        }
        console.log(`\nGlobal Average Processing Time: ${this.globalAverageProcessingTime.toFixed(2)}ms`);
    }

    private printTimeEstimate(fileSize: number, totalBytesProcessed: number, startTime: [number, number]): void {
        const processedPercentage = totalBytesProcessed / fileSize;
        const elapsedTime = process.hrtime(startTime)[0];
        const estimatedTotalTime = elapsedTime / processedPercentage;
        const remainingTime = estimatedTotalTime - elapsedTime;
        console.log(`Estimated time remaining: ${remainingTime.toFixed(1)}s`);
    }

    getWorkerStats(): Map<number, WorkerStats> {
        return this.workerStats;
    }
}