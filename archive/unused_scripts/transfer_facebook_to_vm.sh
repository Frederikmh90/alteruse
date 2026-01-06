#!/bin/bash
# Transfer Facebook Pipeline to VM
# ===============================

set -e

# Configuration
VM_HOST="ssh.cloud.sdu.dk"
VM_PORT="2285"
VM_USER="ucloud"
REMOTE_DIR="/work/Datadonationer/facebook_pipeline"
DATA_DIR="/work/Datadonationer/data"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if SSH key is available
check_ssh() {
    if ! ssh -p "$VM_PORT" -o ConnectTimeout=10 -o BatchMode=yes "$VM_USER@$VM_HOST" exit 2>/dev/null; then
        error "SSH connection failed"
        echo "Please ensure:"
        echo "1. Your SSH key is added to the VM"
        echo "2. The VM is accessible at $VM_HOST:$VM_PORT"
        echo "3. You have the correct permissions"
        exit 1
    fi
}

# Create remote directories
create_remote_dirs() {
    log "Creating remote directories..."
    ssh -p "$VM_PORT" "$VM_USER@$VM_HOST" << EOF
        mkdir -p "$REMOTE_DIR"
        mkdir -p "$DATA_DIR/url_extract_facebook"
        mkdir -p "$DATA_DIR/logs"
        echo "Directories created successfully"
EOF
}

# Transfer pipeline files
transfer_pipeline() {
    log "Transferring Facebook pipeline files..."
    
    # Create a temporary directory for files to transfer
    local temp_dir=$(mktemp -d)
    
    # Copy pipeline files
    cp notebooks/url_extraction_facebook/step*.py "$temp_dir/"
    cp notebooks/url_extraction_facebook/run_facebook_pipeline.py "$temp_dir/"
    cp notebooks/url_extraction_facebook/README.md "$temp_dir/"
    cp requirements.txt "$temp_dir/"
    
    # Transfer files
    scp -P "$VM_PORT" -r "$temp_dir"/* "$VM_USER@$VM_HOST:$REMOTE_DIR/"
    
    # Clean up
    rm -rf "$temp_dir"
    
    log "Pipeline files transferred successfully"
}

# Transfer monitoring scripts
transfer_monitoring() {
    log "Transferring monitoring scripts..."
    
    scp -P "$VM_PORT" run_facebook_pipeline_vm.sh "$VM_USER@$VM_HOST:/work/Datadonationer/"
    scp -P "$VM_PORT" monitor_facebook_pipeline.sh "$VM_USER@$VM_HOST:/work/Datadonationer/"
    
    # Make scripts executable
    ssh -p "$VM_PORT" "$VM_USER@$VM_HOST" "chmod +x /work/Datadonationer/run_facebook_pipeline_vm.sh"
    ssh -p "$VM_PORT" "$VM_USER@$VM_HOST" "chmod +x /work/Datadonationer/monitor_facebook_pipeline.sh"
    
    log "Monitoring scripts transferred successfully"
}

# Check remote setup
check_remote_setup() {
    log "Checking remote setup..."
    
    ssh -p "$VM_PORT" "$VM_USER@$VM_HOST" << EOF
        echo "=== Directory Structure ==="
        ls -la "$REMOTE_DIR"
        echo ""
        echo "=== Data Directory ==="
        ls -la "$DATA_DIR"
        echo ""
        echo "=== Python Environment ==="
        which python
        python --version
        echo ""
        echo "=== Available Disk Space ==="
        df -h "$DATA_DIR"
EOF
}

# Main execution
main() {
    echo "Facebook Pipeline Transfer to VM"
    echo "================================"
    echo "VM: $VM_USER@$VM_HOST:$VM_PORT"
    echo "Remote directory: $REMOTE_DIR"
    echo ""
    
    # Check SSH connection
    log "Testing SSH connection..."
    check_ssh
    log "SSH connection successful"
    
    # Create directories
    create_remote_dirs
    
    # Transfer files
    transfer_pipeline
    transfer_monitoring
    
    # Check setup
    check_remote_setup
    
    echo ""
    log "Transfer completed successfully!"
    echo ""
    echo "Next steps:"
    echo "1. SSH to the VM: ssh -p $VM_PORT $VM_USER@$VM_HOST"
    echo "2. Navigate to: cd /work/Datadonationer"
    echo "3. Start the pipeline: ./run_facebook_pipeline_vm.sh"
    echo "4. Monitor progress: ./monitor_facebook_pipeline.sh status"
    echo ""
    echo "Or use tmux:"
    echo "1. tmux new-session -d -s facebook_pipeline"
    echo "2. tmux attach-session -t facebook_pipeline"
    echo "3. ./run_facebook_pipeline_vm.sh"
}

# Run main function
main "$@" 