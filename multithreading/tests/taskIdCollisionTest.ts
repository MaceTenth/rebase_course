import { TaskManager } from '../src/taskManager';

describe('TaskManager ID Generation', () => {
    let taskManager: TaskManager;
    
    beforeEach(() => {
        taskManager = new TaskManager();
    });

    test('unique task ids under concurrent creation', () => {
        const taskIds = new Set<number>();
        const fileSize = 1000000;
        
        // Create many tasks rapidly
        for (let i = 0; i < 1000; i++) {
            const task = taskManager.createAdaptiveTask(0, fileSize, 'average', 100);
            
            // Verify no ID collisions
            expect(taskIds.has(task.id)).toBeFalsy();
            taskIds.add(task.id);
        }
    });
});