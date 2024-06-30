# Global variable to signal the task to stop
should_stop = false

# Function that the task will run
function my_task_function()
    while !should_stop
        # Task's main work goes here
        sleep(1) # Simulate some work
        println("Working...")
    end
    println("Task gracefully stopping.")
end

# Start the task
task = Task(my_task_function)
schedule(task)

# At some point later, signal the task to stop
should_stop = true