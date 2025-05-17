export interface Task {
    startByte: number;
    endByte: number;
    id: number;
}

export interface WorkerInput {
    workerId: number;
    filePath: string;
    initialTask: Task;
}

export interface WorkerResult {
    count: number;
    workerId: number;
    taskCompleted: Task;
    processingTime: number;
    memoryUsage: NodeJS.MemoryUsage;
}

export interface WorkerStats {
    tasksCompleted: number;
    totalProcessingTime: number;
    primesFound: number;
    avgProcessingTime: number;
}