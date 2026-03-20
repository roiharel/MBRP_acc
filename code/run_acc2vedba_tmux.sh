#!/bin/bash

# VEDBA Processing with tmux - Easy Management Script
# Usage: ./run_vedba_tmux.sh [start|status|attach|logs|stop]

SESSION_NAME="vedba_processing"
SCRIPT_PATH_0="code/00_get_acc_data_from_movebank.py"
SCRIPT_PATH_1="code/01_prep_tag_acc_long.py"
SCRIPT_PATH_2="code/02_acc_to_vedba_par.py"
WORK_DIR="/home/TOP/rharel/EAS_ind/rharel/analysis/JK_ch1"

case "$1" in
    start)
        echo "Starting VEDBA processing in tmux..."
        
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
        
        # Run all three scripts sequentially
        echo "Starting processing pipeline..."
        echo "Step 0: $SCRIPT_PATH_0"
        echo "Step 1: $SCRIPT_PATH_1"
        echo "Step 2: $SCRIPT_PATH_2"
        tmux send-keys -t $SESSION_NAME "python3 $SCRIPT_PATH_0 && echo '=== Step 0 completed, starting step 1 ===' && python3 $SCRIPT_PATH_1 && echo '=== Step 1 completed, starting step 2 ===' && python3 $SCRIPT_PATH_2" Enter
        
        echo "Processing started in tmux session: $SESSION_NAME"
        echo "Use './run_vedba_tmux.sh status' to check progress"
        echo "Use './run_vedba_tmux.sh attach' to view the session"
        ;;
        
    status)
        echo "Checking VEDBA processing status..."
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
        echo "Attaching to VEDBA processing session..."
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
        echo "Stopping VEDBA processing..."
        tmux kill-session -t $SESSION_NAME 2>/dev/null
        echo "Session stopped"
        ;;
        
    *)
        echo "VEDBA Processing Manager"
        echo ""
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  start   - Start VEDBA processing in tmux"
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