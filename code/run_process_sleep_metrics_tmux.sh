#!/bin/bash

# Process Sleep Metrics with tmux - Easy Management Script
# Usage: ./run_process_sleep_metrics_tmux.sh [start|status|attach|logs|stop]

SESSION_NAME="process_sleep"
GET_ACC="code/00_get_acc_data_from_movebank.py"
PREP_ACC="code/01_prep_tag_acc_long.py"
CALC_VEDBA="code/02_acc_to_vedba_par.py"
FIND_INACT="code/03_find_inactivity.R"
GET_SLEEP="code/04_get_sleep_metrics.R"
WORK_DIR="/home/TOP/rharel/EAS_ind/rharel/analysis/JK_ch1"

case "$1" in
    start)
        echo "Starting Process Sleep Metrics in tmux..."
        
        # Create new session
        tmux new-session -d -s $SESSION_NAME
        
        # Navigate to work directory
        tmux send-keys -t $SESSION_NAME "cd $WORK_DIR" Enter
        
        # Create and activate virtual environment if it doesn't exist
        if [ ! -d "vedba_env" ]; then
            echo "Creating virtual environment..."
            tmux send-keys -t $SESSION_NAME "python3 -m venv vedba_env" Enter
            sleep 2
        fi
        
        tmux send-keys -t $SESSION_NAME "source vedba_env/bin/activate" Enter
        sleep 1
        
        # Install packages
        echo "Installing required packages..."
        tmux send-keys -t $SESSION_NAME "pip install pyreadr numpy pandas tqdm pyarrow" Enter
        sleep 10
        
        # Run all scripts sequentially (Python + R)
        echo "Starting processing pipeline..."
        echo "Step 0: $GET_ACC"
        echo "Step 1: $PREP_ACC"
        echo "Step 2: $CALC_VEDBA"
        echo "Step 3: $FIND_INACT"
        echo "Step 4: $GET_SLEEP"
        tmux send-keys -t $SESSION_NAME "python3 $GET_ACC && echo '=== Step 0 completed, starting step 1 ===' && python3 $PREP_ACC && echo '=== Step 1 completed, starting step 2 ===' && python3 $CALC_VEDBA && echo '=== Step 2 completed, starting step 3 ===' && Rscript $FIND_INACT && echo '=== Step 3 completed, starting step 4 ===' && Rscript $GET_SLEEP && echo '=== Pipeline completed ==='" Enter
        
        echo "Processing started in tmux session: $SESSION_NAME"
        echo "Use './run_process_sleep_metrics_tmux.sh status' to check progress"
        echo "Use './run_process_sleep_metrics_tmux.sh attach' to view the session"
        ;;
        
    status)
        echo "Checking Process Sleep Metrics status..."
        if tmux has-session -t $SESSION_NAME 2>/dev/null; then
            echo "Session '$SESSION_NAME' is running"
            echo "Last few lines of output:"
            echo "----------------------------------------"
            tmux capture-pane -t $SESSION_NAME -p | tail -10
            echo "----------------------------------------"
        else
            echo "Session '$SESSION_NAME' is not running"
        fi
        ;;
        
    attach)
        echo "Attaching to Process Sleep Metrics session..."
        echo "Press Ctrl+b, then 'd' to detach and leave it running"
        tmux attach-session -t $SESSION_NAME
        ;;
        
    logs)
        echo "Showing recent logs..."
        if tmux has-session -t $SESSION_NAME 2>/dev/null; then
            tmux capture-pane -t $SESSION_NAME -p
        else
            echo "Session '$SESSION_NAME' is not running"
        fi
        ;;
        
    stop)
        echo "Stopping Process Sleep Metrics..."
        tmux kill-session -t $SESSION_NAME 2>/dev/null
        echo "Session stopped"
        ;;
        
    *)
        echo "Process Sleep Metrics Manager"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  start   - Start Process Sleep Metrics processing in tmux"
        echo "  status  - Check if processing is running and show progress"
        echo "  attach  - Attach to the tmux session (view live output)"
        echo "  logs    - Show all output from the session"
        echo "  stop    - Stop the processing session"
        echo ""
        echo "Examples:"
        echo "  $0 start     # Start the processing"
        echo "  $0 status    # Check progress"
        echo "  $0 attach    # View live output"
        echo "  $0 stop      # Stop processing"
        ;;
esac