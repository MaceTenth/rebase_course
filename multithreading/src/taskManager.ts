import { Task, WorkerStats } from './types';
import * as os from 'os';

export class TaskManager {
    private readonly MIN_CHUNK_SIZE = 1024 * 1024; // 1MB
    private readonly MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB
    private readonly NUM_CORES = os.cpus().length;
    private taskIdCounter = 0;
    private performanceHistory: number[] = [];

    createInitialTasks(fileSize: number): Task[] {
        const initialChunkSize = Math.ceil(fileSize / (this.NUM_CORES * 4));
        const tasks: Task[] = [];
        
        for (let startByte = 0; startByte < fileSize; startByte += initialChunkSize) {
            tasks.push({
                startByte,
                endByte: Math.min(startByte + initialChunkSize, fileSize),
                id: this.taskIdCounter++
            });
        }
        
        return tasks;
    }

    calculateChunkSize(
        fileSize: number,
        currentTaskSize: number,
        currentWorkerId: number,
        workerStats: Map<number, WorkerStats>
    ): number {
        if (this.performanceHistory.length < 2) {
            return Math.ceil(fileSize / (this.NUM_CORES * 4));
        }
        
        const avgProcessingTime = this.performanceHistory.reduce((a, b) => a + b, 0) / 
            this.performanceHistory.length;
        let adjustedSize = Math.ceil(fileSize / (this.NUM_CORES * 4));
        
        // Adjust chunk size based on processing time
        if (avgProcessingTime > 1000) {
            adjustedSize = Math.max(adjustedSize / 2, this.MIN_CHUNK_SIZE);
        } else if (avgProcessingTime < 100) {
            adjustedSize = Math.min(adjustedSize * 1.5, this.MAX_CHUNK_SIZE);
        }
        
        // Add prime density consideration
        const workerStat = workerStats.get(currentWorkerId);
        const primeDensity = workerStat ? 
            workerStat.primesFound / currentTaskSize : 0;
        
        if (primeDensity > 0.5) {
            adjustedSize = Math.max(adjustedSize / 1.5, this.MIN_CHUNK_SIZE);
        }
        
        return Math.ceil(adjustedSize);
    }

    addPerformanceData(processingTime: number): void {
        this.performanceHistory.push(processingTime);
    }

    getNumCores(): number {
        return this.NUM_CORES;
    }
}