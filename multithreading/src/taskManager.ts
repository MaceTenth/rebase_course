import { Task, WorkerStats } from './types';
import * as os from 'os';

export class TaskManager {
    private readonly MIN_CHUNK_SIZE = 1024 * 1024; // 1MB
    private readonly MAX_CHUNK_SIZE = 10 * 1024 * 1024; // 10MB
    private readonly NUM_CORES = os.cpus().length;
    private taskIdCounter = 0;
    private performanceHistory: number[] = [];
    private recentGlobalAverage: number = 0;
    
    createInitialTasks(fileSize: number): Task[] {
        // Start with a more balanced chunk size based on file size and available cores
        const initialChunkSize = this.calculateOptimalChunkSize(fileSize);
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
    
    // Calculate optimal initial chunk size based on file size and available cores
    private calculateOptimalChunkSize(fileSize: number): number {
        // For very small files, use smaller chunks
        if (fileSize < this.MIN_CHUNK_SIZE * this.NUM_CORES * 2) {
            return Math.max(Math.ceil(fileSize / (this.NUM_CORES * 2)), 1024); // At least 1KB
        }
        
        // For medium files, aim for 4 tasks per core initially
        if (fileSize < this.MAX_CHUNK_SIZE * this.NUM_CORES * 4) {
            return Math.ceil(fileSize / (this.NUM_CORES * 4));
        }
        
        // For large files, balance between MIN and MAX chunk size
        const avgChunkSize = Math.ceil(fileSize / (this.NUM_CORES * 4));
        return Math.min(Math.max(avgChunkSize, this.MIN_CHUNK_SIZE), this.MAX_CHUNK_SIZE);
    }
    
    // This method is now simplified and used for adaptive chunk creation
    // rather than searching for existing smaller tasks
    calculateChunkSize(globalAverageTime: number): number {
        // Use recent global average if available, otherwise use the provided one
        const avgTime = this.recentGlobalAverage || globalAverageTime;
        
        // Adjust chunk size based on processing time
        let adaptiveSize: number;
        
        if (avgTime > 1000) {
            // If tasks are taking too long, reduce chunk size
            adaptiveSize = this.MIN_CHUNK_SIZE;
        } else if (avgTime > 500) {
            // Moderately sized chunks for medium processing times
            adaptiveSize = (this.MIN_CHUNK_SIZE + this.MAX_CHUNK_SIZE) / 4;
        } else if (avgTime > 200) {
            // Balanced chunks for good processing times
            adaptiveSize = (this.MIN_CHUNK_SIZE + this.MAX_CHUNK_SIZE) / 2;
        } else {
            // Larger chunks for fast processing times
            adaptiveSize = this.MAX_CHUNK_SIZE;
        }
        
        return Math.ceil(adaptiveSize);
    }
    
    // Create a new adaptive task based on remaining file size and worker performance
    createAdaptiveTask(
        startByte: number, 
        endByte: number, 
        workerPerformance: 'slow' | 'average' | 'fast',
        globalAverageTime: number
    ): Task {
        const baseChunkSize = this.calculateChunkSize(globalAverageTime);
        let adjustedChunkSize: number;
        
        // Adjust chunk size based on worker performance
        switch (workerPerformance) {
            case 'slow':
                adjustedChunkSize = Math.max(baseChunkSize / 2, this.MIN_CHUNK_SIZE);
                break;
            case 'fast':
                adjustedChunkSize = Math.min(baseChunkSize * 1.5, this.MAX_CHUNK_SIZE);
                break;
            default: // 'average'
                adjustedChunkSize = baseChunkSize;
        }
        
        // Make sure chunk isn't bigger than the remaining data
        const actualChunkSize = Math.min(adjustedChunkSize, endByte - startByte);
        
        return {
            startByte,
            endByte: startByte + actualChunkSize,
            id: this.taskIdCounter++
        };
    }

    addPerformanceData(processingTime: number): void {
        // Keep at most 20 recent processing times
        if (this.performanceHistory.length >= 20) {
            this.performanceHistory.shift();
        }
        this.performanceHistory.push(processingTime);
        
        // Update the recent global average
        this.recentGlobalAverage = this.performanceHistory.reduce((sum, time) => sum + time, 0) / 
            this.performanceHistory.length;
    }

    getNumCores(): number {
        return this.NUM_CORES;
    }
}